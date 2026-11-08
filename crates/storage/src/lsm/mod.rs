//! A durable LSM-tree key/value engine used as the KV state machine's
//! storage (the Raft log/WAL in this crate is separate and untouched).
//!
//! ```text
//!  apply(ops, raft_index) ──► WAL record (ops + index, one CRC) ──► memtable
//!                                      │ full
//!                              rotate: fsync old WAL, new WAL, memtable → imm
//!                                      │ background / flush()
//!                       imm → L0 SSTable (tmp → fsync → rename → dir fsync)
//!                                      │ MANIFEST atomically replaced  ◄── commit point
//!                       L0 files ≥ trigger ──► merge with overlapping L1 files
//!                                      │ MANIFEST atomically replaced  ◄── commit point
//! ```
//!
//! **Crash safety.** The MANIFEST names every live SSTable and the oldest
//! live WAL. It is replaced atomically, so a crash leaves either the old or
//! the new version, never a mixture. Files it does not name (half-written
//! outputs, compacted-away inputs) are garbage and are removed on open. WAL
//! records carry the Raft `applied` index in the same checksummed record as
//! the data, so data and position can never disagree. A damaged final WAL
//! record is a torn tail and is dropped; damage anywhere else is reported as
//! [`LsmError::Corruption`] and nothing is modified.
//!
//! **Fail-stop.** After any I/O error on a write path the handle is
//! poisoned and refuses all further work (no retry-and-hope after a failed
//! fsync). Reopening re-runs recovery.
//!
//! Two levels: L0 (overlapping flush outputs, newest first) and L1
//! (non-overlapping, sorted). L1 is the bottom level, so compaction drops
//! tombstones.

mod bloom;
mod cache;
mod error;
mod manifest;
mod sstable;
mod wal;

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

pub use cache::BlockCache;
pub use error::{LsmError, Result};
pub use wal::Op;

use crate::durable;
use crate::fault::FaultPoint;
use manifest::{FileMeta, Manifest};
use sstable::{SstBuilder, SstIter, SstReader, Value};
use wal::WalWriter;

const MANIFEST: &str = "MANIFEST";
/// Id of the WAL created for a brand-new database.
const FIRST_WAL_ID: u64 = 1;

#[derive(Debug, Clone)]
pub struct LsmOptions {
    /// Rotate + flush the memtable once it holds this many bytes.
    pub memtable_bytes: usize,
    /// Target data block size in an SSTable.
    pub block_bytes: usize,
    /// Bloom filter bits per key (0 disables filters).
    pub bloom_bits_per_key: usize,
    /// Shared block cache capacity.
    pub cache_bytes: usize,
    /// Compact when this many L0 files exist.
    pub l0_compaction_trigger: usize,
    /// Target size of an L1 output file.
    pub l1_file_bytes: usize,
    /// fsync the WAL on every `apply`. When false the WAL is flushed to the
    /// OS only; call [`Lsm::sync`] for a durability barrier. See
    /// `kv::LsmStore` for why that is safe under Raft.
    pub sync_wal: bool,
    /// Run flush/compaction on a background thread. Tests that inject
    /// faults turn this off so every I/O happens on the calling thread.
    pub background: bool,
    /// Read and checksum every block of every table at open.
    pub verify_on_open: bool,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            memtable_bytes: 4 << 20,
            block_bytes: 4096,
            bloom_bits_per_key: 10,
            cache_bytes: 8 << 20,
            l0_compaction_trigger: 4,
            l1_file_bytes: 2 << 20,
            sync_wal: true,
            background: true,
            verify_on_open: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub l0_files: usize,
    pub l1_files: usize,
    pub sst_bytes: u64,
    pub disk_bytes: u64,
    pub memtable_bytes: usize,
    pub applied: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

struct MemTable {
    map: BTreeMap<Vec<u8>, Value>,
    bytes: usize,
    max_applied: u64,
    /// Smallest WAL id holding this memtable's data.
    wal_start: u64,
}

impl MemTable {
    fn new(wal_start: u64) -> Self {
        Self {
            map: BTreeMap::new(),
            bytes: 0,
            max_applied: 0,
            wal_start,
        }
    }
    fn insert(&mut self, op: &Op) {
        let (k, v) = match op {
            Op::Put(k, v) => (k.clone(), Some(v.clone())),
            Op::Delete(k) => (k.clone(), None),
        };
        self.bytes += op.approx_bytes();
        self.map.insert(k, v);
    }
    fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Vec<u8>, Value)> {
        let lo = start.map_or(Bound::Unbounded, Bound::Included);
        let hi = end.map_or(Bound::Unbounded, Bound::Excluded);
        if let (Some(s), Some(e)) = (start, end) {
            if s >= e {
                return Vec::new();
            }
        }
        self.map
            .range::<[u8], _>((lo, hi))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

struct VFile {
    meta: FileMeta,
    reader: Arc<SstReader>,
}

#[derive(Default)]
struct Version {
    /// Level 0, newest (highest id) first.
    l0: Vec<Arc<VFile>>,
    /// Level 1, sorted by min key, pairwise disjoint.
    l1: Vec<Arc<VFile>>,
}

impl Version {
    fn build(files: Vec<Arc<VFile>>) -> Version {
        let mut l0: Vec<_> = files
            .iter()
            .filter(|f| f.meta.level == 0)
            .cloned()
            .collect();
        let mut l1: Vec<_> = files
            .iter()
            .filter(|f| f.meta.level == 1)
            .cloned()
            .collect();
        l0.sort_by_key(|a| std::cmp::Reverse(a.meta.id));
        l1.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));
        Version { l0, l1 }
    }
    fn all(&self) -> Vec<Arc<VFile>> {
        self.l0.iter().chain(self.l1.iter()).cloned().collect()
    }
}

struct Inner {
    active: MemTable,
    imm: Option<Arc<MemTable>>,
    wal: WalWriter,
    version: Arc<Version>,
    manifest: Manifest,
    applied: u64,
    next_id: u64,
    /// Bumped by `replace_all`; in-flight flush/compaction jobs started under
    /// an older epoch must not publish.
    epoch: u64,
    poisoned: Option<String>,
}

impl Inner {
    fn check(&self) -> Result<()> {
        match &self.poisoned {
            Some(m) => Err(LsmError::Poisoned(m.clone())),
            None => Ok(()),
        }
    }
    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

struct Shared {
    dir: PathBuf,
    opts: LsmOptions,
    cache: Arc<BlockCache>,
    inner: Mutex<Inner>,
    /// Wakes the background thread.
    bg_cv: Condvar,
    /// Signalled when the immutable memtable has been flushed.
    imm_cleared: Condvar,
    /// Serializes flush / compaction / ingest publication work.
    maintenance: Mutex<()>,
    shutdown: AtomicBool,
    flushes: AtomicU64,
    compactions: AtomicU64,
}

pub struct Lsm {
    sh: Arc<Shared>,
    bg: Option<JoinHandle<()>>,
}

fn sst_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("sst-{id:010}.sst"))
}

fn parse_id(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?
        .strip_suffix(suffix)?
        .parse()
        .ok()
}

impl Shared {
    fn poison<T>(&self, r: Result<T>) -> Result<T> {
        if let Err(LsmError::Io(e)) = &r {
            let mut g = self.inner.lock().unwrap();
            g.poisoned.get_or_insert_with(|| e.to_string());
            drop(g);
            self.imm_cleared.notify_all();
        }
        r
    }

    /// Write `bytes` as `sst-<id>.sst` using the tmp → fsync → rename
    /// protocol. The directory fsync is the caller's job (batched).
    fn write_sst(&self, id: u64, bytes: &[u8]) -> Result<PathBuf> {
        let path = sst_path(&self.dir, id);
        let tmp = durable::tmp_path(&path);
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        durable::write_all(FaultPoint::LsmSstTmpWrite, &mut f, bytes)?;
        durable::sync_file(FaultPoint::LsmSstTmpSync, &f)?;
        drop(f);
        durable::rename_impl(FaultPoint::LsmSstRename, &tmp, &path)?;
        Ok(path)
    }

    fn publish_manifest(&self, m: &Manifest) -> Result<()> {
        // Never make an inconsistent MANIFEST durable: a bug here would
        // otherwise be persisted, then trusted by every later open.
        m.validate()?;
        durable::atomic_write(
            &self.dir.join(MANIFEST),
            &m.encode(),
            durable::LSM_MANIFEST_POINTS,
        )?;
        Ok(())
    }

    fn remove_old_wals(&self, min_wal: u64) {
        if let Ok(rd) = fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                if let Some(id) = parse_id(&e.file_name().to_string_lossy(), "wal-", ".log") {
                    if id < min_wal {
                        let _ = fs::remove_file(e.path());
                    }
                }
            }
        }
    }

    fn wake_bg(&self) {
        self.bg_cv.notify_all();
    }

    /// Flush the immutable memtable (if any) to an L0 table.
    fn flush_imm(&self) -> Result<()> {
        let _m = self.maintenance.lock().unwrap();
        let (imm, epoch, id) = {
            let mut g = self.inner.lock().unwrap();
            g.check()?;
            let Some(imm) = g.imm.clone() else {
                return Ok(());
            };
            let id = (!imm.map.is_empty()).then(|| g.alloc_id());
            (imm, g.epoch, id)
        };

        let built = match id {
            Some(id) => {
                let r = (|| {
                    let mut b =
                        SstBuilder::new(self.opts.block_bytes, self.opts.bloom_bits_per_key);
                    for (k, v) in &imm.map {
                        b.add(k, v);
                    }
                    let out = b.finish();
                    let path = self.write_sst(id, &out.bytes)?;
                    durable::sync_parent_dir(FaultPoint::LsmSstDirSync, &path)?;
                    Ok::<_, LsmError>(out)
                })();
                Some((id, self.poison(r)?))
            }
            None => None,
        };

        let mut g = self.inner.lock().unwrap();
        g.check()?;
        if g.epoch != epoch {
            // A snapshot ingest replaced everything while we were building.
            if let Some((id, _)) = &built {
                let _ = fs::remove_file(sst_path(&self.dir, *id));
            }
            return Ok(());
        }
        let mut files = g.version.all();
        let mut m = g.manifest.clone();
        if let Some((id, out)) = &built {
            let path = sst_path(&self.dir, *id);
            let reader = SstReader::open(&path, *id, self.cache.clone())?;
            let meta = FileMeta {
                id: *id,
                level: 0,
                size: reader.size(),
                entries: out.entries,
                min_key: out.min_key.clone(),
                max_key: out.max_key.clone(),
            };
            m.files.push(meta.clone());
            files.push(Arc::new(VFile { meta, reader }));
        }
        m.version += 1;
        m.next_file_id = g.next_id;
        m.min_wal_id = g.active.wal_start;
        m.live_wals = vec![g.active.wal_start];
        m.applied = m.applied.max(imm.max_applied);
        let r = self.publish_manifest(&m);
        if let Err(e) = r {
            if let LsmError::Io(io) = &e {
                g.poisoned.get_or_insert_with(|| io.to_string());
            }
            self.imm_cleared.notify_all();
            return Err(e);
        }
        let min_wal = m.min_wal_id;
        g.manifest = m;
        g.version = Arc::new(Version::build(files));
        g.imm = None;
        drop(g);
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.imm_cleared.notify_all();
        self.remove_old_wals(min_wal);
        Ok(())
    }

    /// Fsync + rotate the WAL and freeze the memtable. Caller holds the lock
    /// and has checked `imm.is_none()`.
    fn rotate_locked(&self, g: &mut Inner) -> Result<()> {
        g.check()?;
        let r = (|| {
            g.wal.sync(FaultPoint::LsmWalRotateSync)?;
            let id = g.alloc_id();
            let new_wal = WalWriter::create(&self.dir, id)?;
            // Publish the new WAL in the recovery set before it can receive writes.
            let mut manifest = g.manifest.clone();
            manifest.version += 1;
            manifest.next_file_id = g.next_id;
            manifest.live_wals.push(id);
            manifest.live_wals.sort_unstable();
            self.publish_manifest(&manifest)?;
            g.manifest = manifest;
            let frozen = std::mem::replace(&mut g.active, MemTable::new(id));
            g.imm = Some(Arc::new(frozen));
            g.wal = new_wal;
            Ok(())
        })();
        if let Err(LsmError::Io(e)) = &r {
            g.poisoned.get_or_insert_with(|| e.to_string());
        }
        r
    }

    fn rotate_if_needed(&self) -> Result<()> {
        loop {
            let mut g = self.inner.lock().unwrap();
            g.check()?;
            if g.active.bytes < self.opts.memtable_bytes {
                return Ok(());
            }
            if g.imm.is_none() {
                self.rotate_locked(&mut g)?;
                drop(g);
                if self.opts.background {
                    self.wake_bg();
                } else {
                    self.flush_imm()?;
                }
                return Ok(());
            }
            if self.opts.background {
                // Backpressure: wait for the background flush.
                let _ = self
                    .imm_cleared
                    .wait_timeout(g, Duration::from_millis(20))
                    .unwrap();
            } else {
                drop(g);
                self.flush_imm()?;
            }
        }
    }

    /// One compaction round. Returns `Ok(true)` if work was done.
    fn compact_step(&self) -> Result<bool> {
        let _m = self.maintenance.lock().unwrap();
        let (inputs, epoch) = {
            let g = self.inner.lock().unwrap();
            g.check()?;
            if g.version.l0.len() < self.opts.l0_compaction_trigger.max(1) {
                return Ok(false);
            }
            let l0 = g.version.l0.clone();
            let lo = l0.iter().map(|f| &f.meta.min_key).min().unwrap().clone();
            let hi = l0.iter().map(|f| &f.meta.max_key).max().unwrap().clone();
            let mut inputs = l0;
            for f in &g.version.l1 {
                if f.meta.max_key >= lo && f.meta.min_key <= hi {
                    inputs.push(f.clone());
                }
            }
            (inputs, g.epoch)
        };

        // Sources newest-first: L0 (already newest-first) then L1.
        let mut iters: Vec<SstIter> = inputs.iter().map(|f| f.reader.iter(None, None)).collect();
        let mut heads: Vec<Option<(Vec<u8>, Value)>> = Vec::new();
        for it in iters.iter_mut() {
            heads.push(it.next_entry()?);
        }

        let mut outputs: Vec<(u64, sstable::BuiltSst)> = Vec::new();
        let mut builder = SstBuilder::new(self.opts.block_bytes, self.opts.bloom_bits_per_key);
        let mut written: Vec<PathBuf> = Vec::new();
        let cleanup = |paths: &[PathBuf]| {
            for p in paths {
                let _ = fs::remove_file(p);
            }
        };
        let result: Result<()> = (|| {
            loop {
                // Smallest key among heads; ties resolved to the newest source.
                let mut best: Option<usize> = None;
                for (i, h) in heads.iter().enumerate() {
                    if let Some((k, _)) = h {
                        match best {
                            None => best = Some(i),
                            Some(b) if k < &heads[b].as_ref().unwrap().0 => best = Some(i),
                            _ => {}
                        }
                    }
                }
                let Some(b) = best else { break };
                let (key, val) = heads[b].clone().unwrap();
                for (i, h) in heads.iter_mut().enumerate() {
                    if h.as_ref().is_some_and(|(k, _)| *k == key) {
                        *h = iters[i].next_entry()?;
                    }
                }
                if val.is_none() {
                    continue; // bottom level: tombstone has nothing left to shadow
                }
                builder.add(&key, &val);
                if builder.approx_size() >= self.opts.l1_file_bytes {
                    let id = self.inner.lock().unwrap().alloc_id();
                    let full = std::mem::replace(
                        &mut builder,
                        SstBuilder::new(self.opts.block_bytes, self.opts.bloom_bits_per_key),
                    )
                    .finish();
                    written.push(self.write_sst(id, &full.bytes)?);
                    outputs.push((id, full));
                }
            }
            if !builder.is_empty() {
                let id = self.inner.lock().unwrap().alloc_id();
                let last = std::mem::replace(
                    &mut builder,
                    SstBuilder::new(self.opts.block_bytes, self.opts.bloom_bits_per_key),
                )
                .finish();
                written.push(self.write_sst(id, &last.bytes)?);
                outputs.push((id, last));
            }
            if let Some(p) = written.first() {
                durable::sync_parent_dir(FaultPoint::LsmSstDirSync, p)?;
            }
            Ok(())
        })();
        if let Err(e) = self.poison(result.map(|_| ())) {
            cleanup(&written);
            return Err(e);
        }

        let mut g = self.inner.lock().unwrap();
        let still_valid = g.check().is_ok()
            && g.epoch == epoch
            && inputs
                .iter()
                .all(|i| g.version.all().iter().any(|f| f.meta.id == i.meta.id));
        if !still_valid {
            drop(g);
            cleanup(&written);
            return Ok(false);
        }
        let input_ids: Vec<u64> = inputs.iter().map(|f| f.meta.id).collect();
        let mut files: Vec<Arc<VFile>> = g
            .version
            .all()
            .into_iter()
            .filter(|f| !input_ids.contains(&f.meta.id))
            .collect();
        let mut m = g.manifest.clone();
        m.files.retain(|f| !input_ids.contains(&f.id));
        for (id, out) in &outputs {
            let reader = match SstReader::open(&sst_path(&self.dir, *id), *id, self.cache.clone()) {
                Ok(r) => r,
                Err(e) => {
                    drop(g);
                    cleanup(&written);
                    return Err(e);
                }
            };
            let meta = FileMeta {
                id: *id,
                level: 1,
                size: reader.size(),
                entries: out.entries,
                min_key: out.min_key.clone(),
                max_key: out.max_key.clone(),
            };
            m.files.push(meta.clone());
            files.push(Arc::new(VFile { meta, reader }));
        }
        m.version += 1;
        m.next_file_id = g.next_id;
        if let Err(e) = self.publish_manifest(&m) {
            if let LsmError::Io(io) = &e {
                g.poisoned.get_or_insert_with(|| io.to_string());
            }
            return Err(e); // outputs stay on disk as orphans; open() removes them
        }
        g.manifest = m;
        g.version = Arc::new(Version::build(files));
        drop(g);
        for f in &inputs {
            f.reader.mark_obsolete();
        }
        self.compactions.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}

fn background_loop(sh: Arc<Shared>) {
    loop {
        {
            let g = sh.inner.lock().unwrap();
            if sh.shutdown.load(Ordering::Acquire) {
                return;
            }
            let _ = sh
                .bg_cv
                .wait_timeout(g, Duration::from_millis(100))
                .unwrap();
        }
        if sh.shutdown.load(Ordering::Acquire) {
            return;
        }
        if sh.flush_imm().is_err() {
            return; // poisoned; surfaced to callers by their next operation
        }
        loop {
            match sh.compact_step() {
                Ok(true) => continue,
                Ok(false) => break,
                Err(_) => return,
            }
        }
    }
}

impl Lsm {
    /// Open (creating if needed) the engine in `dir`, running recovery.
    pub fn open(dir: impl AsRef<Path>, opts: LsmOptions) -> Result<Lsm> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let cache = BlockCache::new(opts.cache_bytes);

        // 1. Scan. Stale temp files are never live, but they are only
        // deleted once validation below has passed, so that a refused open
        // leaves the directory exactly as it was found.
        let mut sst_ids_on_disk = Vec::new();
        let mut wal_ids_on_disk: Vec<u64> = Vec::new();
        let mut stale_tmp_files: Vec<PathBuf> = Vec::new();
        for e in fs::read_dir(&dir)?.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.ends_with(".tmp") {
                stale_tmp_files.push(e.path());
            } else if let Some(id) = parse_id(&name, "sst-", ".sst") {
                sst_ids_on_disk.push(id);
            } else if let Some(id) = parse_id(&name, "wal-", ".log") {
                wal_ids_on_disk.push(id);
            }
        }

        // 2. MANIFEST.
        let mpath = dir.join(MANIFEST);
        let manifest = match fs::read(&mpath) {
            Ok(bytes) => Manifest::decode(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No MANIFEST. That is a genuinely fresh database only if
                // nothing else is here; anything more means the MANIFEST was
                // lost and we must not guess. The one exception is the
                // residue of an interrupted first open (below): WAL 1,
                // created but still empty.
                if !sst_ids_on_disk.is_empty() {
                    return Err(LsmError::Corruption(format!(
                        "{} has SSTables but no MANIFEST (refusing to guess)",
                        dir.display()
                    )));
                }
                for id in &wal_ids_on_disk {
                    let len = fs::metadata(wal::wal_path(&dir, *id))?.len();
                    if *id != FIRST_WAL_ID || len != 0 {
                        return Err(LsmError::Corruption(format!(
                            "{} has WAL {id} ({len} bytes) but no MANIFEST \
                             (refusing to guess)",
                            dir.display()
                        )));
                    }
                }
                // Fresh initialisation. The WAL is made durable *before* the
                // MANIFEST so that "MANIFEST present" always implies "its
                // WAL was created"; a crash between the two steps leaves
                // only an empty WAL 1, which the check above accepts.
                if wal_ids_on_disk.is_empty() {
                    drop(WalWriter::create(&dir, FIRST_WAL_ID)?);
                }
                let m = Manifest {
                    version: 1,
                    next_file_id: FIRST_WAL_ID + 1,
                    min_wal_id: FIRST_WAL_ID,
                    live_wals: vec![FIRST_WAL_ID],
                    applied: 0,
                    files: vec![],
                };
                m.validate()?;
                durable::atomic_write(&mpath, &m.encode(), durable::LSM_MANIFEST_POINTS)?;
                wal_ids_on_disk = vec![FIRST_WAL_ID];
                m
            }
            Err(e) => return Err(e.into()),
        };

        // The MANIFEST must be internally consistent before anything in it
        // is trusted. A legacy v1 MANIFEST predates the allocator invariant
        // for ids; raising the allocator position is always safe, so heal
        // that one field instead of rejecting a directory we cannot judge.
        let mut manifest = manifest;
        if manifest.live_wals.is_empty() {
            let past_tables = manifest.files.iter().map(|f| f.id + 1).max().unwrap_or(0);
            manifest.next_file_id = manifest
                .next_file_id
                .max(past_tables)
                .max(manifest.min_wal_id + 1);
        }
        manifest.validate()?;

        // 3. Tables named by the manifest must exist and be intact.
        let mut vfiles = Vec::new();
        for meta in &manifest.files {
            let path = sst_path(&dir, meta.id);
            let reader = SstReader::open(&path, meta.id, cache.clone())?;
            if reader.size() != meta.size || reader.entries != meta.entries {
                return Err(LsmError::Corruption(format!(
                    "{}: size/entry count disagree with MANIFEST",
                    path.display()
                )));
            }
            // Reads and scans skip tables by the MANIFEST's key range; if it
            // disagrees with the table, keys would be silently missed.
            if let Some((lo, hi)) = reader.key_range()? {
                if lo != meta.min_key || hi != meta.max_key {
                    return Err(LsmError::Corruption(format!(
                        "{}: key range disagrees with MANIFEST",
                        path.display()
                    )));
                }
            }
            if opts.verify_on_open {
                reader.verify_all()?;
            }
            vfiles.push(Arc::new(VFile {
                meta: meta.clone(),
                reader,
            }));
        }

        // 4. Reconcile the WAL files on disk with the MANIFEST's WAL set.
        //
        // Nothing is modified until every check here has passed.
        //
        // Persistence protocol (v2 MANIFEST):
        //  * `live_wals` is the exact set of WALs recovery needs. Each one
        //    must exist; a missing one means acknowledged writes are gone
        //    and we refuse to start.
        //  * A new WAL is created *before* the MANIFEST that lists it is
        //    published, and no writer is switched to it until that publish
        //    succeeds. Creation writes no bytes. A crash in that window
        //    therefore leaves an unreferenced WAL that is (a) empty and
        //    (b) has an id >= the MANIFEST's `next_file_id` (ids are handed
        //    out monotonically and every publish records the allocator
        //    position). Only a file proven to be both is an orphan we may
        //    delete. Any other unreferenced WAL at or above `min_wal_id`
        //    contradicts the protocol (e.g. a restored older MANIFEST) and
        //    may hold acknowledged writes, so it fails closed.
        //  * WALs below `min_wal_id` were retired by a flush/ingest whose
        //    MANIFEST is already durable; they are garbage whatever they hold.
        //  * A legacy v1 MANIFEST has no WAL list and v1 rotation did not
        //    publish, so any WAL at or above `min_wal_id` may hold
        //    acknowledged writes. All of them are kept and recorded (see the
        //    migration below); none is ever deleted as an "orphan".
        let mut all_wals = wal_ids_on_disk;
        all_wals.sort_unstable();
        let stale_wals: Vec<u64> = all_wals
            .iter()
            .copied()
            .filter(|id| *id < manifest.min_wal_id)
            .collect();
        let candidate_live: Vec<u64> = all_wals
            .iter()
            .copied()
            .filter(|id| *id >= manifest.min_wal_id)
            .collect();

        let legacy = manifest.live_wals.is_empty(); // decode() never yields this for v2
        if legacy {
            // The WAL named by `min_wal_id` exists in every legacy state
            // (it is only ever replaced by a flush that created its
            // successor first), so its absence is certain loss.
            if candidate_live.first() != Some(&manifest.min_wal_id) {
                return Err(LsmError::Corruption(format!(
                    "{}: legacy MANIFEST requires WAL {} but it is missing (WALs on disk: {:?}); \
                     unflushed writes may be lost, refusing to start",
                    dir.display(),
                    manifest.min_wal_id,
                    candidate_live
                )));
            }
        } else {
            let mut sorted = manifest.live_wals.clone();
            sorted.sort_unstable();
            sorted.dedup();
            if sorted != manifest.live_wals || sorted.first() != Some(&manifest.min_wal_id) {
                return Err(LsmError::Corruption(format!(
                    "{}: MANIFEST live WAL set {:?} is not an increasing set starting at min_wal_id {}",
                    dir.display(),
                    manifest.live_wals,
                    manifest.min_wal_id
                )));
            }
            let missing: Vec<u64> = manifest
                .live_wals
                .iter()
                .copied()
                .filter(|id| !all_wals.contains(id))
                .collect();
            if !missing.is_empty() {
                return Err(LsmError::Corruption(format!(
                    "{}: MANIFEST requires WALs {:?} but they are missing (WALs on disk: {:?}); \
                     unflushed writes may be lost, refusing to start",
                    dir.display(),
                    missing,
                    all_wals
                )));
            }
        }

        let mut orphan_wals: Vec<u64> = Vec::new();
        for id in candidate_live
            .iter()
            .copied()
            .filter(|id| !manifest.live_wals.contains(id) && !legacy)
        {
            if id < manifest.next_file_id {
                return Err(LsmError::Corruption(format!(
                    "{}: unreferenced WAL {id} lies inside the allocated id range (< next_file_id {}); \
                     not an interrupted rotation, refusing recovery",
                    dir.display(),
                    manifest.next_file_id
                )));
            }
            let len = fs::metadata(wal::wal_path(&dir, id))?.len();
            if len != 0 {
                return Err(LsmError::Corruption(format!(
                    "{}: unreferenced WAL {id} holds {len} bytes; an interrupted rotation leaves \
                     an empty file, so this may contain acknowledged writes (stale or restored \
                     MANIFEST?), refusing to delete it",
                    dir.display()
                )));
            }
            orphan_wals.push(id);
        }

        // Legacy MANIFEST: the upgraded MANIFEST records exactly the WALs
        // found, with the allocator position beyond all of them, so that
        // every later open can detect a lost WAL. It is only *computed* here
        // and written in phase 2. This freezes in whatever set exists now; a
        // WAL already lost *before* the upgrade cannot be known.
        if legacy {
            manifest.live_wals = candidate_live.clone();
            let past_wals = candidate_live.last().map_or(0, |id| id + 1);
            manifest.next_file_id = manifest.next_file_id.max(past_wals);
            manifest.version += 1;
            manifest.validate()?;
        }
        let wal_ids = manifest.live_wals.clone();

        // 5. Replay every live WAL into the memtable, read-only.
        //
        // Phase 1 ends here: nothing on disk has been touched. A torn tail
        // is only *recorded*; whether it is acceptable depends on the WALs
        // after it, which have not been read yet when it is found.
        let mut active = MemTable::new(manifest.min_wal_id);
        let mut applied = manifest.applied;
        let mut torn_earlier: Option<PathBuf> = None;
        let mut truncations: Vec<(PathBuf, u64)> = Vec::new();
        let nwals = wal_ids.len();
        for (i, id) in wal_ids.iter().enumerate() {
            let path = wal::wal_path(&dir, *id);
            let r = wal::replay(&path)?;
            if !r.records.is_empty() {
                if let Some(p) = &torn_earlier {
                    return Err(LsmError::Corruption(format!(
                        "{} has a torn tail but a later WAL holds data",
                        p.display()
                    )));
                }
            }
            for rec in &r.records {
                // Ordering contract: a positive `applied` must exceed every
                // earlier positive one (earlier records, then earlier WALs),
                // starting above the MANIFEST's own `applied`. 0 carries no
                // index and is accepted anywhere. A violation means ops
                // would replay in a different order than they were applied
                // (swapped, duplicated or spliced WALs), so refuse.
                if rec.applied > 0 {
                    if rec.applied <= applied {
                        return Err(LsmError::Corruption(format!(
                            "{}: record with applied index {} does not exceed the preceding \
                             applied index {applied} (MANIFEST applied {}); out-of-order, \
                             duplicated or swapped WAL",
                            path.display(),
                            rec.applied,
                            manifest.applied
                        )));
                    }
                    applied = rec.applied;
                    active.max_applied = active.max_applied.max(rec.applied);
                }
                for op in &rec.ops {
                    active.insert(op);
                }
            }
            if r.valid_len < r.file_len {
                truncations.push((path.clone(), r.valid_len));
                if i + 1 < nwals {
                    torn_earlier = Some(path);
                }
            }
        }

        // Phase 2: the whole sequence is accepted, so recovery may now
        // modify the directory. Every step is idempotent, so a crash (or an
        // I/O error) part-way simply repeats the remainder on the next open.
        if legacy {
            durable::atomic_write(&mpath, &manifest.encode(), durable::LSM_MANIFEST_POINTS)?;
        }
        for (path, valid_len) in &truncations {
            wal::truncate_tail(path, *valid_len)?;
        }
        // Garbage: temp files, unreferenced tables, retired WALs, orphans.
        // Removing a file that is already gone is success, so a repeat after
        // an interruption is harmless. Temp files and tables are best effort
        // (a real I/O error is ignored, as before); WAL removals are strict.
        for p in &stale_tmp_files {
            durable::remove_for_recovery(FaultPoint::LsmRecoverRemove, p, false)?;
        }
        let live: Vec<u64> = manifest.files.iter().map(|f| f.id).collect();
        for id in &sst_ids_on_disk {
            if !live.contains(id) {
                let p = sst_path(&dir, *id);
                durable::remove_for_recovery(FaultPoint::LsmRecoverRemove, &p, false)?;
            }
        }
        for id in stale_wals.into_iter().chain(orphan_wals) {
            let p = wal::wal_path(&dir, id);
            durable::remove_for_recovery(FaultPoint::LsmRecoverRemove, &p, true)?;
        }

        let max_seen = sst_ids_on_disk
            .iter()
            .chain(wal_ids.iter())
            .copied()
            .max()
            .map_or(0, |m| m + 1);
        let next_id = manifest
            .next_file_id
            .max(max_seen)
            .max(manifest.min_wal_id + 1);
        // Non-empty by the check above: the newest surviving WAL is the
        // active one.
        let active_wal = *wal_ids.last().expect("min_wal_id WAL verified present");
        let wal = WalWriter::reopen(&dir, active_wal)?;

        let inner = Inner {
            active,
            imm: None,
            wal,
            version: Arc::new(Version::build(vfiles)),
            manifest,
            applied,
            next_id,
            epoch: 0,
            poisoned: None,
        };
        let sh = Arc::new(Shared {
            dir,
            opts: opts.clone(),
            cache,
            inner: Mutex::new(inner),
            bg_cv: Condvar::new(),
            imm_cleared: Condvar::new(),
            maintenance: Mutex::new(()),
            shutdown: AtomicBool::new(false),
            flushes: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
        });
        let bg = opts.background.then(|| {
            let s = sh.clone();
            std::thread::Builder::new()
                .name("lsm-bg".into())
                .spawn(move || background_loop(s))
                .expect("spawn lsm background thread")
        });
        Ok(Lsm { sh, bg })
    }

    /// Apply `ops` atomically. `applied` is the Raft index these ops
    /// complete (0 = not tied to an index); it is recorded in the same WAL
    /// record, so the data and its position are persisted together.
    pub fn apply(&self, ops: &[Op], applied: u64) -> Result<()> {
        {
            let mut g = self.sh.inner.lock().unwrap();
            g.check()?;
            // The `applied` ordering contract (see the module docs): a
            // positive index must exceed every earlier one. Recovery refuses
            // a WAL that violates it, so the engine must never write one:
            // reject here, before anything is appended.
            if applied > 0 && applied <= g.applied {
                return Err(LsmError::InvalidInput(format!(
                    "applied index {applied} is not greater than the current applied \
                     index {}; positive indexes must be strictly increasing (0 = no index)",
                    g.applied
                )));
            }
            let rec = wal::encode_record(applied, ops);
            if let Err(e) = g.wal.append(&rec, self.sh.opts.sync_wal) {
                if let LsmError::Io(io) = &e {
                    g.poisoned.get_or_insert_with(|| io.to_string());
                }
                return Err(e);
            }
            for op in ops {
                g.active.insert(op);
            }
            if applied > 0 {
                g.applied = g.applied.max(applied);
                g.active.max_applied = g.active.max_applied.max(applied);
            }
        }
        self.sh.rotate_if_needed()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let version = {
            let g = self.sh.inner.lock().unwrap();
            g.check()?;
            if let Some(v) = g.active.map.get(key) {
                return Ok(v.clone());
            }
            if let Some(imm) = &g.imm {
                if let Some(v) = imm.map.get(key) {
                    return Ok(v.clone());
                }
            }
            g.version.clone()
        };
        for f in &version.l0 {
            if key < f.meta.min_key.as_slice() || key > f.meta.max_key.as_slice() {
                continue;
            }
            if let Some(v) = f.reader.get(key)? {
                return Ok(v);
            }
        }
        let i = version
            .l1
            .partition_point(|f| f.meta.max_key.as_slice() < key);
        if let Some(f) = version.l1.get(i) {
            if f.meta.min_key.as_slice() <= key {
                if let Some(v) = f.reader.get(key)? {
                    return Ok(v);
                }
            }
        }
        Ok(None)
    }

    /// Live entries with `start <= key < end`, ascending, at most `limit`.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Consistent snapshot of every source under one lock acquisition.
        let (mem_a, mem_i, version) = {
            let g = self.sh.inner.lock().unwrap();
            g.check()?;
            (
                g.active.range(start, end),
                g.imm
                    .as_ref()
                    .map(|m| m.range(start, end))
                    .unwrap_or_default(),
                g.version.clone(),
            )
        };
        enum Src {
            Mem(std::vec::IntoIter<(Vec<u8>, Value)>),
            Sst(SstIter),
        }
        let mut srcs: Vec<Src> = vec![Src::Mem(mem_a.into_iter()), Src::Mem(mem_i.into_iter())];
        for f in &version.l0 {
            srcs.push(Src::Sst(f.reader.iter(start, end)));
        }
        for f in &version.l1 {
            let after_start = start.is_none_or(|s| f.meta.max_key.as_slice() >= s);
            let before_end = end.is_none_or(|e| f.meta.min_key.as_slice() < e);
            if after_start && before_end {
                srcs.push(Src::Sst(f.reader.iter(start, end)));
            }
        }
        let next = |s: &mut Src| -> Result<Option<(Vec<u8>, Value)>> {
            match s {
                Src::Mem(it) => Ok(it.next()),
                Src::Sst(it) => it.next_entry(),
            }
        };
        let mut heads: Vec<Option<(Vec<u8>, Value)>> = Vec::new();
        for s in srcs.iter_mut() {
            heads.push(next(s)?);
        }
        let mut out = Vec::new();
        while out.len() < limit {
            let mut best: Option<usize> = None;
            for (i, h) in heads.iter().enumerate() {
                if let Some((k, _)) = h {
                    match best {
                        None => best = Some(i),
                        Some(b) if k < &heads[b].as_ref().unwrap().0 => best = Some(i),
                        _ => {}
                    }
                }
            }
            let Some(b) = best else { break };
            let (key, val) = heads[b].clone().unwrap();
            for i in 0..heads.len() {
                if heads[i].as_ref().is_some_and(|(k, _)| *k == key) {
                    heads[i] = next(&mut srcs[i])?;
                }
            }
            if let Some(v) = val {
                out.push((key, v));
            }
        }
        Ok(out)
    }

    /// Highest Raft index applied (durably or in memory, whichever is later).
    pub fn applied_index(&self) -> u64 {
        self.sh.inner.lock().unwrap().applied
    }

    /// Durability barrier: everything applied so far survives a crash.
    pub fn sync(&self) -> Result<()> {
        let mut g = self.sh.inner.lock().unwrap();
        g.check()?;
        let r = g.wal.sync(FaultPoint::LsmWalAppendSync);
        if let Err(LsmError::Io(io)) = &r {
            g.poisoned.get_or_insert_with(|| io.to_string());
        }
        r
    }

    /// Force the memtable to disk as an L0 table (no-op if empty).
    pub fn flush(&self) -> Result<()> {
        loop {
            {
                let mut g = self.sh.inner.lock().unwrap();
                g.check()?;
                if g.imm.is_none() {
                    if g.active.map.is_empty() {
                        return Ok(());
                    }
                    self.sh.rotate_locked(&mut g)?;
                }
            }
            self.sh.flush_imm()?;
            let g = self.sh.inner.lock().unwrap();
            if g.imm.is_none() {
                return Ok(());
            }
        }
    }

    /// Run one compaction round if one is due. Returns whether work was done.
    pub fn compact_step(&self) -> Result<bool> {
        self.sh.compact_step()
    }

    /// Compact until no work remains.
    pub fn compact_all(&self) -> Result<()> {
        while self.sh.compact_step()? {}
        Ok(())
    }

    /// Atomically replace the entire contents with `entries` (strictly
    /// ascending keys) and set the applied index to `applied`. Used to
    /// install a Raft snapshot. A crash leaves the old contents or the new
    /// ones, never a mixture.
    pub fn replace_all<I>(&self, entries: I, applied: u64) -> Result<()>
    where
        I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        let _m = self.sh.maintenance.lock().unwrap();
        self.sh.inner.lock().unwrap().check()?;
        let mut outputs: Vec<(u64, sstable::BuiltSst)> = Vec::new();
        let mut written: Vec<PathBuf> = Vec::new();
        let build = (|| {
            let mut b = SstBuilder::new(self.sh.opts.block_bytes, self.sh.opts.bloom_bits_per_key);
            let mut finish =
                |b: SstBuilder, outputs: &mut Vec<(u64, sstable::BuiltSst)>| -> Result<()> {
                    let id = self.sh.inner.lock().unwrap().alloc_id();
                    let out = b.finish();
                    written.push(self.sh.write_sst(id, &out.bytes)?);
                    outputs.push((id, out));
                    Ok(())
                };
            for (k, v) in entries {
                b.add(&k, &Some(v));
                if b.approx_size() >= self.sh.opts.l1_file_bytes {
                    let full = std::mem::replace(
                        &mut b,
                        SstBuilder::new(self.sh.opts.block_bytes, self.sh.opts.bloom_bits_per_key),
                    );
                    finish(full, &mut outputs)?;
                }
            }
            if !b.is_empty() {
                finish(b, &mut outputs)?;
            }
            if let Some(p) = written.first() {
                durable::sync_parent_dir(FaultPoint::LsmSstDirSync, p)?;
            }
            Ok::<_, LsmError>(())
        })();
        if let Err(e) = self.sh.poison(build) {
            for p in &written {
                let _ = fs::remove_file(p);
            }
            return Err(e);
        }

        let mut g = self.sh.inner.lock().unwrap();
        g.check()?;
        let r = (|| {
            let new_wal_id = g.alloc_id();
            let new_wal = WalWriter::create(&self.sh.dir, new_wal_id)?;
            let mut files = Vec::new();
            let mut m = Manifest {
                version: g.manifest.version + 1,
                next_file_id: g.next_id,
                min_wal_id: new_wal_id,
                live_wals: vec![new_wal_id],
                applied,
                files: vec![],
            };
            for (id, out) in &outputs {
                let reader =
                    SstReader::open(&sst_path(&self.sh.dir, *id), *id, self.sh.cache.clone())?;
                let meta = FileMeta {
                    id: *id,
                    level: 1,
                    size: reader.size(),
                    entries: out.entries,
                    min_key: out.min_key.clone(),
                    max_key: out.max_key.clone(),
                };
                m.files.push(meta.clone());
                files.push(Arc::new(VFile { meta, reader }));
            }
            self.sh.publish_manifest(&m)?; // ← commit point
            Ok::<_, LsmError>((m, files, new_wal, new_wal_id))
        })();
        let (m, files, new_wal, new_wal_id) = match r {
            Ok(x) => x,
            Err(e) => {
                if let LsmError::Io(io) = &e {
                    g.poisoned.get_or_insert_with(|| io.to_string());
                }
                return Err(e);
            }
        };
        for f in g.version.all() {
            f.reader.mark_obsolete();
        }
        g.manifest = m;
        g.version = Arc::new(Version::build(files));
        g.active = MemTable::new(new_wal_id);
        g.imm = None;
        g.wal = new_wal;
        g.applied = applied;
        g.epoch += 1;
        drop(g);
        self.sh.imm_cleared.notify_all();
        self.sh.remove_old_wals(new_wal_id);
        Ok(())
    }

    pub fn stats(&self) -> Stats {
        let g = self.sh.inner.lock().unwrap();
        let mut disk = 0u64;
        if let Ok(rd) = fs::read_dir(&self.sh.dir) {
            for e in rd.flatten() {
                if let Ok(md) = e.metadata() {
                    disk += md.len();
                }
            }
        }
        Stats {
            l0_files: g.version.l0.len(),
            l1_files: g.version.l1.len(),
            sst_bytes: g.version.all().iter().map(|f| f.reader.size()).sum(),
            disk_bytes: disk,
            memtable_bytes: g.active.bytes + g.imm.as_ref().map_or(0, |m| m.bytes),
            applied: g.applied,
            flushes: self.sh.flushes.load(Ordering::Relaxed),
            compactions: self.sh.compactions.load(Ordering::Relaxed),
            cache_hits: self.sh.cache.hits(),
            cache_misses: self.sh.cache.misses(),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.sh.dir
    }

    /// Whether a previous I/O failure has poisoned this handle.
    pub fn is_poisoned(&self) -> bool {
        self.sh.inner.lock().unwrap().poisoned.is_some()
    }

    /// Dump every live entry in key order (used for Raft snapshots).
    pub fn dump(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(None, None, usize::MAX)
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        self.sh.shutdown.store(true, Ordering::Release);
        self.sh.wake_bg();
        if let Some(h) = self.bg.take() {
            let _ = h.join();
        }
        // Deliberately neither flushed nor fsynced: dropping a handle must
        // look like a crash to recovery (the WAL already holds everything).
    }
}
