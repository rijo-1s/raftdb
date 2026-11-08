//! Immutable sorted table file.
//!
//! ```text
//! [data block]*  [index block]  [bloom block]  [footer]
//!
//! data block : entries... | crc32(entries)
//! entry      : klen:u32 | vlen:u32 (u32::MAX = tombstone) | key | value
//! index block: (klen:u32 | last_key | offset:u64 | len:u32)* | crc32
//! bloom block: bloom bytes | crc32
//! footer(52) : index_off,index_len,bloom_off,bloom_len,entries,magic (6*u64)
//!              | crc32 of those 48 bytes
//! ```
//! Every region is individually checksummed, so a flipped bit anywhere is
//! detected when that region is read, and `verify_all` checks the lot.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::bloom;
use super::cache::BlockCache;
use super::error::{corrupt, LsmError, Result};
use crate::durable::crc32;

const MAGIC: u64 = 0x4c53_4d53_5354_0001; // "LSMSST" v1
const FOOTER_LEN: usize = 52;
const TOMBSTONE: u32 = u32::MAX;

/// A value or a deletion marker.
pub type Value = Option<Vec<u8>>;

pub(crate) struct SstBuilder {
    block_bytes: usize,
    bits_per_key: usize,
    out: Vec<u8>,
    cur: Vec<u8>,
    cur_last: Vec<u8>,
    index: Vec<(Vec<u8>, u64, u32)>,
    hashes: Vec<u64>,
    count: u64,
    first: Option<Vec<u8>>,
    last: Option<Vec<u8>>,
}

pub(crate) struct BuiltSst {
    pub bytes: Vec<u8>,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub entries: u64,
}

impl SstBuilder {
    pub fn new(block_bytes: usize, bits_per_key: usize) -> Self {
        Self {
            block_bytes: block_bytes.max(64),
            bits_per_key,
            out: Vec::new(),
            cur: Vec::new(),
            cur_last: Vec::new(),
            index: Vec::new(),
            hashes: Vec::new(),
            count: 0,
            first: None,
            last: None,
        }
    }

    /// Approximate size of the file so far.
    pub fn approx_size(&self) -> usize {
        self.out.len() + self.cur.len()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Keys must be added in strictly ascending order.
    pub fn add(&mut self, key: &[u8], value: &Value) {
        if let Some(l) = &self.last {
            assert!(key > l.as_slice(), "SstBuilder: keys must strictly ascend");
        }
        self.cur
            .extend_from_slice(&(key.len() as u32).to_le_bytes());
        match value {
            Some(v) => {
                self.cur.extend_from_slice(&(v.len() as u32).to_le_bytes());
                self.cur.extend_from_slice(key);
                self.cur.extend_from_slice(v);
            }
            None => {
                self.cur.extend_from_slice(&TOMBSTONE.to_le_bytes());
                self.cur.extend_from_slice(key);
            }
        }
        self.cur_last = key.to_vec();
        self.hashes.push(bloom::hash64(key));
        self.count += 1;
        if self.first.is_none() {
            self.first = Some(key.to_vec());
        }
        self.last = Some(key.to_vec());
        if self.cur.len() >= self.block_bytes {
            self.flush_block();
        }
    }

    fn flush_block(&mut self) {
        if self.cur.is_empty() {
            return;
        }
        let off = self.out.len() as u64;
        let crc = crc32(&self.cur);
        self.out.extend_from_slice(&self.cur);
        self.out.extend_from_slice(&crc.to_le_bytes());
        let len = (self.cur.len() + 4) as u32;
        self.index
            .push((std::mem::take(&mut self.cur_last), off, len));
        self.cur.clear();
    }

    pub fn finish(mut self) -> BuiltSst {
        self.flush_block();
        let index_off = self.out.len() as u64;
        let mut idx = Vec::new();
        for (k, off, len) in &self.index {
            idx.extend_from_slice(&(k.len() as u32).to_le_bytes());
            idx.extend_from_slice(k);
            idx.extend_from_slice(&off.to_le_bytes());
            idx.extend_from_slice(&len.to_le_bytes());
        }
        let c = crc32(&idx);
        idx.extend_from_slice(&c.to_le_bytes());
        let index_len = idx.len() as u64;
        self.out.extend_from_slice(&idx);

        let bloom_off = self.out.len() as u64;
        let mut bl = bloom::build(&self.hashes, self.bits_per_key);
        let c = crc32(&bl);
        bl.extend_from_slice(&c.to_le_bytes());
        let bloom_len = bl.len() as u64;
        self.out.extend_from_slice(&bl);

        let mut f = Vec::with_capacity(FOOTER_LEN);
        for v in [
            index_off, index_len, bloom_off, bloom_len, self.count, MAGIC,
        ] {
            f.extend_from_slice(&v.to_le_bytes());
        }
        let c = crc32(&f);
        f.extend_from_slice(&c.to_le_bytes());
        self.out.extend_from_slice(&f);
        BuiltSst {
            bytes: self.out,
            min_key: self.first.unwrap_or_default(),
            max_key: self.last.unwrap_or_default(),
            entries: self.count,
        }
    }
}

fn rd_u32(b: &[u8], p: &mut usize) -> Option<u32> {
    let v = u32::from_le_bytes(b.get(*p..*p + 4)?.try_into().ok()?);
    *p += 4;
    Some(v)
}
fn rd_u64(b: &[u8], p: &mut usize) -> Option<u64> {
    let v = u64::from_le_bytes(b.get(*p..*p + 8)?.try_into().ok()?);
    *p += 8;
    Some(v)
}

#[cfg(unix)]
fn read_exact_at(f: &File, buf: &mut [u8], off: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(f, buf, off)
}
#[cfg(not(unix))]
fn read_exact_at(f: &File, buf: &mut [u8], off: u64) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = f.try_clone()?;
    f.seek(SeekFrom::Start(off))?;
    f.read_exact(buf)
}

pub struct SstReader {
    pub(crate) id: u64,
    path: PathBuf,
    file: File,
    size: u64,
    index: Vec<(Vec<u8>, u64, u32)>,
    bloom: Vec<u8>,
    pub(crate) entries: u64,
    cache: Arc<BlockCache>,
    obsolete: AtomicBool,
}

impl Drop for SstReader {
    fn drop(&mut self) {
        // Deleted only when the last reader (including in-flight scans that
        // still hold the old version) is gone.
        if self.obsolete.load(Ordering::Acquire) {
            self.cache.evict_file(self.id);
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl SstReader {
    pub fn open(path: &Path, id: u64, cache: Arc<BlockCache>) -> Result<Arc<SstReader>> {
        let file = File::open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                LsmError::Corruption(format!("sstable {} is missing", path.display()))
            } else {
                LsmError::Io(e)
            }
        })?;
        let size = file.metadata()?.len();
        let name = path.display();
        if size < FOOTER_LEN as u64 {
            return corrupt(format!("sstable {name} is truncated ({size} bytes)"));
        }
        let mut foot = [0u8; FOOTER_LEN];
        read_exact_at(&file, &mut foot, size - FOOTER_LEN as u64)?;
        if crc32(&foot[..48]) != u32::from_le_bytes(foot[48..52].try_into().unwrap()) {
            return corrupt(format!("sstable {name}: footer checksum mismatch"));
        }
        let mut p = 0;
        let index_off = rd_u64(&foot, &mut p).unwrap();
        let index_len = rd_u64(&foot, &mut p).unwrap();
        let bloom_off = rd_u64(&foot, &mut p).unwrap();
        let bloom_len = rd_u64(&foot, &mut p).unwrap();
        let entries = rd_u64(&foot, &mut p).unwrap();
        let magic = rd_u64(&foot, &mut p).unwrap();
        if magic != MAGIC {
            return corrupt(format!("sstable {name}: bad magic"));
        }
        let end = size - FOOTER_LEN as u64;
        let regions_ok = index_off.checked_add(index_len) == Some(bloom_off)
            && bloom_off.checked_add(bloom_len) == Some(end)
            && index_len >= 4
            && bloom_len >= 4;
        if !regions_ok {
            return corrupt(format!("sstable {name}: inconsistent footer offsets"));
        }
        let mut idx = vec![0u8; index_len as usize];
        read_exact_at(&file, &mut idx, index_off)?;
        let (body, c) = idx.split_at(idx.len() - 4);
        if crc32(body) != u32::from_le_bytes(c.try_into().unwrap()) {
            return corrupt(format!("sstable {name}: index checksum mismatch"));
        }
        let mut index = Vec::new();
        let mut p = 0;
        while p < body.len() {
            let parsed = (|| {
                let kl = rd_u32(body, &mut p)? as usize;
                let k = body.get(p..p + kl)?.to_vec();
                p += kl;
                let off = rd_u64(body, &mut p)?;
                let len = rd_u32(body, &mut p)?;
                Some((k, off, len))
            })();
            match parsed {
                Some(e) => index.push(e),
                None => return corrupt(format!("sstable {name}: malformed index")),
            }
        }
        let mut bl = vec![0u8; bloom_len as usize];
        read_exact_at(&file, &mut bl, bloom_off)?;
        let (bbody, c) = bl.split_at(bl.len() - 4);
        if crc32(bbody) != u32::from_le_bytes(c.try_into().unwrap()) {
            return corrupt(format!("sstable {name}: bloom checksum mismatch"));
        }
        let bloom = bbody.to_vec();
        for (_, off, len) in &index {
            // `off` comes from the file: a forged (checksum-valid) index could make a
            // plain `+` overflow, so the bound check must use checked arithmetic.
            let ends_inside_data = off
                .checked_add(*len as u64)
                .is_some_and(|end| end <= index_off);
            if !ends_inside_data || *len < 4 {
                return corrupt(format!("sstable {name}: index points outside data"));
            }
        }
        Ok(Arc::new(SstReader {
            id,
            path: path.to_path_buf(),
            file,
            size,
            index,
            bloom,
            entries,
            cache,
            obsolete: AtomicBool::new(false),
        }))
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }

    fn block(&self, i: usize) -> Result<Arc<Vec<u8>>> {
        let (_, off, len) = &self.index[i];
        if let Some(b) = self.cache.get((self.id, *off)) {
            return Ok(b);
        }
        let mut buf = vec![0u8; *len as usize];
        read_exact_at(&self.file, &mut buf, *off)?;
        let (body, c) = buf.split_at(buf.len() - 4);
        if crc32(body) != u32::from_le_bytes(c.try_into().unwrap()) {
            return corrupt(format!(
                "sstable {}: data block at offset {off} failed its checksum",
                self.path.display()
            ));
        }
        buf.truncate(buf.len() - 4);
        let b = Arc::new(buf);
        self.cache.insert((self.id, *off), b.clone());
        Ok(b)
    }

    /// `Ok(None)` = key not in this table; `Ok(Some(None))` = tombstone.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        if !bloom::may_contain(&self.bloom, key) {
            return Ok(None);
        }
        let bi = self
            .index
            .partition_point(|(last, _, _)| last.as_slice() < key);
        if bi >= self.index.len() {
            return Ok(None);
        }
        let block = self.block(bi)?;
        let mut p = 0;
        while p < block.len() {
            let (k, v, np) = self.decode_entry(&block, p)?;
            if k == key {
                return Ok(Some(v));
            }
            if k > key {
                break;
            }
            p = np;
        }
        Ok(None)
    }

    fn decode_entry<'a>(&self, b: &'a [u8], mut p: usize) -> Result<(&'a [u8], Value, usize)> {
        let bad =
            || LsmError::Corruption(format!("sstable {}: malformed entry", self.path.display()));
        let kl = rd_u32(b, &mut p).ok_or_else(bad)? as usize;
        let vl = rd_u32(b, &mut p).ok_or_else(bad)?;
        let k = b.get(p..p + kl).ok_or_else(bad)?;
        p += kl;
        if vl == TOMBSTONE {
            return Ok((k, None, p));
        }
        let v = b.get(p..p + vl as usize).ok_or_else(bad)?.to_vec();
        p += vl as usize;
        Ok((k, Some(v), p))
    }

    /// Read every block, verifying checksums, ordering and entry count.
    /// The table's real `(min_key, max_key)`, or `None` for a table with no
    /// blocks. `max_key` comes from the block index; `min_key` costs one
    /// checksum-verified read of the first block. Used to cross-check the
    /// MANIFEST, which the read path trusts for pruning.
    pub fn key_range(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let (Some(first), Some(last)) = (self.index.first(), self.index.last()) else {
            return Ok(None);
        };
        let (off, len) = (first.1, first.2);
        if len < 4 {
            return corrupt(format!(
                "sstable {}: first block too short",
                self.path.display()
            ));
        }
        let mut buf = vec![0u8; len as usize];
        read_exact_at(&self.file, &mut buf, off)?;
        let (body, c) = buf.split_at(buf.len() - 4);
        if crc32(body) != u32::from_le_bytes(c.try_into().unwrap()) {
            return corrupt(format!(
                "sstable {}: first data block failed its checksum",
                self.path.display()
            ));
        }
        if body.is_empty() {
            return corrupt(format!(
                "sstable {}: empty first block",
                self.path.display()
            ));
        }
        let (k, _, _) = self.decode_entry(body, 0)?;
        Ok(Some((k.to_vec(), last.0.clone())))
    }

    pub fn verify_all(&self) -> Result<()> {
        let mut count = 0u64;
        let mut prev: Option<Vec<u8>> = None;
        for i in 0..self.index.len() {
            let (off, len) = (self.index[i].1, self.index[i].2);
            let mut buf = vec![0u8; len as usize];
            read_exact_at(&self.file, &mut buf, off)?;
            let (body, c) = buf.split_at(buf.len() - 4);
            if crc32(body) != u32::from_le_bytes(c.try_into().unwrap()) {
                return corrupt(format!(
                    "sstable {}: data block at offset {off} failed its checksum",
                    self.path.display()
                ));
            }
            let mut p = 0;
            while p < body.len() {
                let (k, _, np) = self.decode_entry(body, p)?;
                if prev.as_deref().is_some_and(|pk| pk >= k) {
                    return corrupt(format!(
                        "sstable {}: keys out of order",
                        self.path.display()
                    ));
                }
                prev = Some(k.to_vec());
                count += 1;
                p = np;
            }
            if prev.as_deref() != Some(self.index[i].0.as_slice()) {
                return corrupt(format!(
                    "sstable {}: index/last-key mismatch",
                    self.path.display()
                ));
            }
        }
        if count != self.entries {
            return corrupt(format!(
                "sstable {}: footer says {} entries, found {count}",
                self.path.display(),
                self.entries
            ));
        }
        Ok(())
    }

    /// Streaming iterator over `[start, end)` (either bound optional).
    pub fn iter(self: &Arc<Self>, start: Option<&[u8]>, end: Option<&[u8]>) -> SstIter {
        let bi = match start {
            Some(s) => self
                .index
                .partition_point(|(last, _, _)| last.as_slice() < s),
            None => 0,
        };
        SstIter {
            sst: self.clone(),
            block_idx: bi,
            block: None,
            pos: 0,
            start: start.map(|s| s.to_vec()),
            end: end.map(|e| e.to_vec()),
            done: false,
        }
    }
}

pub struct SstIter {
    sst: Arc<SstReader>,
    block_idx: usize,
    block: Option<Arc<Vec<u8>>>,
    pos: usize,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    done: bool,
}

impl SstIter {
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Value)>> {
        loop {
            if self.done {
                return Ok(None);
            }
            if self.block.is_none() {
                if self.block_idx >= self.sst.index.len() {
                    self.done = true;
                    return Ok(None);
                }
                self.block = Some(self.sst.block(self.block_idx)?);
                self.pos = 0;
            }
            let b = self.block.clone().unwrap();
            if self.pos >= b.len() {
                self.block = None;
                self.block_idx += 1;
                continue;
            }
            let (k, v, np) = self.sst.decode_entry(&b, self.pos)?;
            self.pos = np;
            if self.start.as_deref().is_some_and(|s| k < s) {
                continue;
            }
            if self.end.as_deref().is_some_and(|e| k >= e) {
                self.done = true;
                return Ok(None);
            }
            return Ok(Some((k.to_vec(), v)));
        }
    }
}
