//! Durable-I/O primitives. Everything that must survive a crash goes
//! through here, so there is exactly one implementation of "write a file
//! atomically" and one place where fault injection hooks in.
//!
//! The atomic-replace protocol (used for the HardState, snapshot and WAL
//! rewrite) is the classic one:
//!
//! 1. write the full new contents to `<path>.tmp` (truncating any stale
//!    temp file left by an earlier crash);
//! 2. `fsync` the temp file, so its *contents* are durable;
//! 3. `rename` it over `<path>` - atomic on POSIX; `MoveFileExW` with
//!    `REPLACE_EXISTING | WRITE_THROUGH` on Windows;
//! 4. `fsync` the parent directory, so the *rename itself* is durable.
//!
//! A crash before step 3 leaves the old file fully intact; a crash after
//! it leaves the new file fully intact. Readers can never observe a
//! half-written file under the final name.
//!
//! Limitation: on platforms other than Unix and Windows there is no
//! portable directory fsync in `std`, so step 4 is a no-op there.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::fault::{self, FaultAction, FaultPoint};

// ---------------------------------------------------------------------
// CRC32 (IEEE 802.3, reflected, poly 0xEDB88320)
// ---------------------------------------------------------------------

const fn make_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = make_crc_table();

/// Table-driven CRC32. Produces identical output to the bitwise
/// implementation it replaced (`crc32(b"123456789") == 0xCBF43926`), so
/// existing on-disk frames stay valid.
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in bytes {
        crc = CRC_TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

// ---------------------------------------------------------------------
// Fault-aware primitives
// ---------------------------------------------------------------------

/// `write_all` that honors `FailBefore` / `PartialWrite(n)` / `FailAfter`.
pub(crate) fn write_all(point: FaultPoint, file: &mut File, buf: &[u8]) -> io::Result<()> {
    match fault::poll(point) {
        None => file.write_all(buf),
        Some(FaultAction::FailBefore) => Err(injected(point)),
        Some(FaultAction::PartialWrite(n)) => {
            file.write_all(&buf[..n.min(buf.len())])?;
            Err(injected(point))
        }
        Some(FaultAction::FailAfter) => {
            file.write_all(buf)?;
            Err(injected(point))
        }
    }
}

/// `fsync` (data + metadata).
pub(crate) fn sync_file(point: FaultPoint, file: &File) -> io::Result<()> {
    match fault::poll(point) {
        None => file.sync_all(),
        Some(FaultAction::FailBefore) | Some(FaultAction::PartialWrite(_)) => Err(injected(point)),
        Some(FaultAction::FailAfter) => {
            file.sync_all()?;
            Err(injected(point))
        }
    }
}

pub(crate) fn rename_impl(point: FaultPoint, from: &Path, to: &Path) -> io::Result<()> {
    match fault::poll(point) {
        None => replace_file(from, to),
        Some(FaultAction::FailBefore) | Some(FaultAction::PartialWrite(_)) => Err(injected(point)),
        Some(FaultAction::FailAfter) => {
            replace_file(from, to)?;
            Err(injected(point))
        }
    }
}

/// fsync the directory containing `path`, making a prior create/rename of
/// an entry in it durable.
pub(crate) fn sync_parent_dir(point: FaultPoint, path: &Path) -> io::Result<()> {
    match fault::poll(point) {
        None => sync_parent_dir_raw(path),
        Some(FaultAction::FailBefore) | Some(FaultAction::PartialWrite(_)) => Err(injected(point)),
        Some(FaultAction::FailAfter) => {
            sync_parent_dir_raw(path)?;
            Err(injected(point))
        }
    }
}

#[cfg(any(test, feature = "fault-injection"))]
fn injected(point: FaultPoint) -> io::Error {
    fault::injected_error(point)
}

#[cfg(not(any(test, feature = "fault-injection")))]
fn injected(_point: FaultPoint) -> io::Error {
    // Unreachable: `fault::poll` never returns Some when compiled out.
    io::Error::other("fault injection is compiled out")
}

#[cfg(unix)]
fn sync_parent_dir_raw(path: &Path) -> io::Result<()> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir_raw(_path: &Path) -> io::Result<()> {
    // No portable directory fsync in `std`. On Windows the rename uses
    // MOVEFILE_WRITE_THROUGH instead; elsewhere this is best-effort only.
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

#[cfg(windows)]
fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    extern "system" {
        fn MoveFileExW(existing: *const u16, new: *const u16, flags: u32) -> i32;
    }

    let from: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    let ok = unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Atomic replace
// ---------------------------------------------------------------------

/// The four fault points of one atomic-replace operation.
#[derive(Clone, Copy)]
pub(crate) struct AtomicPoints {
    pub tmp_write: FaultPoint,
    pub tmp_sync: FaultPoint,
    pub rename: FaultPoint,
    pub dir_sync: FaultPoint,
}

pub(crate) const HARD_STATE_POINTS: AtomicPoints = AtomicPoints {
    tmp_write: FaultPoint::HardStateTmpWrite,
    tmp_sync: FaultPoint::HardStateTmpSync,
    rename: FaultPoint::HardStateRename,
    dir_sync: FaultPoint::HardStateDirSync,
};

pub(crate) const SNAPSHOT_POINTS: AtomicPoints = AtomicPoints {
    tmp_write: FaultPoint::SnapshotTmpWrite,
    tmp_sync: FaultPoint::SnapshotTmpSync,
    rename: FaultPoint::SnapshotRename,
    dir_sync: FaultPoint::SnapshotDirSync,
};

pub(crate) const WAL_REWRITE_POINTS: AtomicPoints = AtomicPoints {
    tmp_write: FaultPoint::WalRewriteTmpWrite,
    tmp_sync: FaultPoint::WalRewriteTmpSync,
    rename: FaultPoint::WalRewriteRename,
    dir_sync: FaultPoint::WalRewriteDirSync,
};

pub(crate) const LSM_MANIFEST_POINTS: AtomicPoints = AtomicPoints {
    tmp_write: FaultPoint::LsmManifestTmpWrite,
    tmp_sync: FaultPoint::LsmManifestTmpSync,
    rename: FaultPoint::LsmManifestRename,
    dir_sync: FaultPoint::LsmManifestDirSync,
};

/// `<path>.tmp`, next to `path` (same directory => same filesystem, which
/// `rename` requires to be atomic).
pub(crate) fn tmp_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".tmp");
    PathBuf::from(p)
}

/// Atomically replace `path` with `contents`. See the module docs for the
/// protocol and its crash behavior.
pub(crate) fn atomic_write(path: &Path, contents: &[u8], points: AtomicPoints) -> io::Result<()> {
    let tmp = tmp_path(path);
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    write_all(points.tmp_write, &mut f, contents)?;
    sync_file(points.tmp_sync, &f)?;
    drop(f);
    rename_impl(points.rename, &tmp, path)?;
    sync_parent_dir(points.dir_sync, path)?;
    Ok(())
}

/// Remove `path` as part of recovery clean-up. A file that is already gone
/// counts as success. A real I/O error is returned only when `strict`;
/// best-effort callers ignore it. An *injected* fault is always returned,
/// because it models the process dying at that instant: `FailBefore` leaves
/// the file, `FailAfter` removes it first.
pub(crate) fn remove_for_recovery(point: FaultPoint, path: &Path, strict: bool) -> io::Result<()> {
    match fault::poll(point) {
        None => {}
        Some(FaultAction::FailBefore) | Some(FaultAction::PartialWrite(_)) => {
            return Err(injected(point));
        }
        Some(FaultAction::FailAfter) => {
            let _ = fs::remove_file(path);
            return Err(injected(point));
        }
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) if strict => Err(e),
        Err(_) => Ok(()),
    }
}

/// Remove a stale `<path>.tmp` left by a crash before the rename step.
/// Safe because the live file is never the temp file. Best effort for real
/// I/O errors; an injected fault is propagated (see `remove_for_recovery`).
pub(crate) fn remove_stale_tmp(path: &Path) -> io::Result<()> {
    remove_for_recovery(FaultPoint::RecoverRemoveTmp, &tmp_path(path), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn atomic_write_replaces_contents() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        atomic_write(&p, b"one", HARD_STATE_POINTS).unwrap();
        atomic_write(&p, b"two", HARD_STATE_POINTS).unwrap();
        assert_eq!(fs::read(&p).unwrap(), b"two");
        assert!(!tmp_path(&p).exists());
    }
}
