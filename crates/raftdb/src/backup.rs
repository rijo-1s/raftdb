//! `raftdb backup` and `raftdb restore`.
//!
//! * `backup` asks a RUNNING node for a point-in-time export (taken inside its
//!   event loop, so it is consistent) and writes it atomically.
//! * `restore` is OFFLINE: it builds a NEW data directory holding a
//!   single-node cluster from a backup. More nodes are added afterwards with
//!   the normal `add-node` procedure. See docs/backup-restore.md.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kv::backup::{Backup, Summary, MAX_MESSAGE_BYTES};
use raft_core::{LogStore, NodeId};
use raftdb_proto as proto;
use storage::WalLog;

type Res<T> = Result<T, Box<dyn std::error::Error>>;

fn parent_of(p: &Path) -> PathBuf {
    match p.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn fsync_dir(dir: &Path) -> Res<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| format!("fsync directory {}: {e}", dir.display()))?;
    Ok(())
}

/// Write `bytes` to `out`: temp file + fsync + atomic publish. Never
/// overwrites an existing `out` (a hard link fails if the name exists).
pub fn write_atomic_no_clobber(out: &Path, bytes: &[u8]) -> Res<()> {
    if fs::symlink_metadata(out).is_ok() {
        return Err(format!(
            "{} already exists (backups are never overwritten)",
            out.display()
        )
        .into());
    }
    let parent = parent_of(out);
    if !parent.is_dir() {
        return Err(format!("directory {} does not exist", parent.display()).into());
    }
    let name = out
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "backup".into());
    let tmp = parent.join(format!(".{name}.tmp-{}", std::process::id()));
    let result = (|| -> Res<()> {
        let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fs::hard_link(&tmp, out).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                format!(
                    "{} appeared while the backup was being written",
                    out.display()
                )
            } else {
                format!("publish {}: {e}", out.display())
            }
        })?;
        Ok(())
    })();
    let _ = fs::remove_file(&tmp);
    result?;
    fsync_dir(&parent)
}

pub async fn run_backup(addr: &str, out: &Path) -> Res<()> {
    if fs::symlink_metadata(out).is_ok() {
        return Err(format!(
            "{} already exists (backups are never overwritten)",
            out.display()
        )
        .into());
    }
    let ep = tonic::transport::Endpoint::from_shared(format!("{}://{addr}", crate::tls::scheme()))?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(600));
    let ep = crate::tls::apply(ep)?;
    let channel = ep
        .connect()
        .await
        .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
    let mut client = proto::client_api_client::ClientApiClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_BYTES);
    let reply = client
        .backup(proto::BackupRequest {})
        .await
        .map_err(|e| format!("backup request failed: {e}"))?
        .into_inner();
    if !reply.ok {
        return Err(format!("the node refused to produce a backup: {}", reply.error).into());
    }
    // Verify what arrived BEFORE it is given a name on disk.
    let b = Backup::decode(&reply.data).map_err(|e| format!("received a damaged backup: {e}"))?;
    let s = b
        .check()
        .map_err(|e| format!("received an unusable backup: {e}"))?;
    write_atomic_no_clobber(out, &reply.data)?;
    // And once more from disk, so "written" means "readable".
    let back = fs::read(out)?;
    Backup::decode(&back)?.check()?;
    println!("backup written: {} ({} bytes)", out.display(), back.len());
    println!(
        "  consistent as of log index {} (term {}), node {}",
        b.last_applied, b.last_applied_term, b.source_node
    );
    println!(
        "  membership {:?}; {} keys, {} client sessions",
        b.peers, s.keys, s.sessions
    );
    Ok(())
}

#[derive(Debug)]
pub struct RestoreReport {
    pub last_applied: u64,
    pub last_applied_term: u64,
    pub summary: Summary,
}

struct Staging {
    path: PathBuf,
    armed: bool,
}
impl Drop for Staging {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Build `data_dir` (which must not exist, or be an empty directory) as a
/// single-node cluster `{node_id}` at the backup's applied index.
///
/// Everything is verified before anything is created; the directory is built
/// under a hidden sibling name, read back with the real recovery code, and
/// published with ONE rename, so a rejected or failed restore leaves nothing.
pub fn run_restore(
    backup_path: &Path,
    data_dir: &Path,
    node_id: NodeId,
) -> Result<RestoreReport, String> {
    if node_id == 0 {
        return Err("node id 0 is not valid".into());
    }
    let bytes =
        fs::read(backup_path).map_err(|e| format!("read {}: {e}", backup_path.display()))?;
    let backup = Backup::decode(&bytes).map_err(|e| format!("{}: {e}", backup_path.display()))?;
    let summary = backup
        .check()
        .map_err(|e| format!("{}: {e}", backup_path.display()))?;

    match fs::symlink_metadata(data_dir) {
        Ok(m) if m.is_dir() => {
            let empty = fs::read_dir(data_dir)
                .map_err(|e| format!("read {}: {e}", data_dir.display()))?
                .next()
                .is_none();
            if !empty {
                return Err(format!(
                    "{} is not empty: restore never overwrites existing data. Use a fresh data directory",
                    data_dir.display()
                ));
            }
        }
        Ok(_) => {
            return Err(format!(
                "{} exists and is not a directory",
                data_dir.display()
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("stat {}: {e}", data_dir.display())),
    }
    let parent = parent_of(data_dir);
    if !parent.is_dir() {
        return Err(format!("directory {} does not exist", parent.display()));
    }

    let name = data_dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "restore".into());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut staging = Staging {
        path: parent.join(format!(".{name}.restoring-{}-{nanos}", std::process::id())),
        armed: true,
    };
    fs::create_dir(&staging.path).map_err(|e| format!("create staging directory: {e}"))?;

    // WalLog treats a failed write as fatal and panics (right for a running
    // node). For restore it just means "this restore failed": catch it, and
    // the Staging guard removes the half-built directory.
    let stage = staging.path.clone();
    let (idx, term, state, peers) = (
        backup.last_applied,
        backup.last_applied_term,
        backup.state.clone(),
        vec![node_id],
    );
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        move || -> Result<(), String> {
            let mut log =
                WalLog::open(stage.join("node.wal")).map_err(|e| format!("create log: {e}"))?;
            log.install_snapshot(idx, term, state, peers);
            // WalLog refuses a directory whose snapshot term exceeds the persisted
            // current term, and refuses a snapshot with no hard state at all.
            log.save_hard_state(term, None);
            Ok(())
        },
    ))
    .map_err(|_| {
        "storage write failed while building the restored directory (nothing was published)"
            .to_string()
    })??;

    // Read it back with the real recovery code and compare.
    let log = std::panic::catch_unwind(|| WalLog::open(staging.path.join("node.wal")))
        .map_err(|_| "recovery panicked on the restored directory".to_string())?
        .map_err(|e| format!("the restored directory does not recover: {e}"))?;
    let ok = log.last_included_index() == backup.last_applied
        && log.last_included_term() == backup.last_applied_term
        && log.snapshot_data() == Some(&backup.state[..])
        && log.snapshot_peers() == [node_id]
        && log.load_hard_state() == (backup.last_applied_term, None)
        && log.last_index() == backup.last_applied;
    drop(log);
    if !ok {
        return Err("the restored directory does not read back as written".into());
    }

    fsync_dir(&staging.path).map_err(|e| e.to_string())?;
    // An empty existing target is replaced atomically by the rename.
    fs::rename(&staging.path, data_dir)
        .map_err(|e| format!("publish {}: {e}", data_dir.display()))?;
    staging.armed = false;
    fsync_dir(&parent).map_err(|e| e.to_string())?;
    Ok(RestoreReport {
        last_applied: backup.last_applied,
        last_applied_term: backup.last_applied_term,
        summary,
    })
}

pub fn print_restore(r: &RestoreReport, data_dir: &Path, node_id: NodeId) {
    println!("restored to {}", data_dir.display());
    println!(
        "  single-node cluster {{{node_id}}}, as of log index {} (term {})",
        r.last_applied, r.last_applied_term
    );
    println!(
        "  {} keys, {} client sessions",
        r.summary.keys, r.summary.sessions
    );
    println!();
    println!("start it (fresh ports; NEVER the old cluster's addresses):");
    println!(
        "  raftdb start --id {node_id} --addr <NEW_ADDR> --data-dir {}",
        data_dir.display()
    );
    println!("then grow the cluster with the normal `add-node` procedure.");
}
