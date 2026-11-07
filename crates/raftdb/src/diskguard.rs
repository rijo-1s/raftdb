//! Free-space protection for the data volume.
//!
//! Replication means every node's disk fills at about the same rate, so a full
//! volume tends to hit the whole cluster at once. Writing into a nearly full
//! volume is what kills a node: a snapshot or WAL rewrite needs extra room
//! while it runs, and an out-of-space error there is fatal by design. The
//! guard turns that into a clean, visible refusal *before* the volume fills:
//! below a floor the node rejects new client writes (reads and membership
//! changes still work) and pauses compaction, then resumes by itself once
//! space has been freed.

use std::path::Path;

/// Bytes available to unprivileged writers on the filesystem holding `path`,
/// or `None` when that cannot be determined (not Unix, or the call failed).
// The field types are `u64` on 64-bit Linux but narrower on 32-bit and some other
// Unixes, so the casts are needed for portability even though clippy sees them
// as no-ops on the platform it runs on.
#[allow(clippy::unnecessary_cast)]
#[cfg(unix)]
pub fn free_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `c_path` is a valid NUL-terminated string and `st` points to
    // writable memory large enough for one `statvfs`.
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), st.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: statvfs returned 0, so it initialised the struct.
    let st = unsafe { st.assume_init() };
    Some((st.f_bavail as u64).saturating_mul(st.f_frsize as u64))
}

#[cfg(not(unix))]
pub fn free_bytes(_path: &Path) -> Option<u64> {
    None
}

/// Decides when writes are refused. Pure state, so it is easy to test.
#[derive(Debug, Clone)]
pub struct DiskGuard {
    min_free: u64,
    protected: bool,
}

impl DiskGuard {
    /// `min_free` of 0 turns the protection off.
    pub fn new(min_free: u64) -> Self {
        Self {
            min_free,
            protected: false,
        }
    }

    pub fn enabled(&self) -> bool {
        self.min_free > 0
    }

    pub fn protected(&self) -> bool {
        self.protected
    }

    pub fn min_free(&self) -> u64 {
        self.min_free
    }

    /// Feed a fresh free-space reading. Returns `Some(new_state)` when the
    /// state changed. Protection engages below the floor and releases only
    /// once free space is 25% above it, so a volume hovering at the limit does
    /// not flap between accepting and refusing writes.
    pub fn observe(&mut self, free: u64) -> Option<bool> {
        if !self.enabled() {
            return None;
        }
        let release_at = self.min_free.saturating_add(self.min_free / 4);
        let next = if self.protected {
            free < release_at
        } else {
            free < self.min_free
        };
        if next == self.protected {
            None
        } else {
            self.protected = next;
            Some(next)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engages_below_the_floor_and_releases_with_hysteresis() {
        let mut g = DiskGuard::new(1000);
        assert_eq!(g.observe(5000), None);
        assert_eq!(g.observe(1000), None, "exactly at the floor is still fine");
        assert_eq!(g.observe(999), Some(true));
        assert!(g.protected());
        assert_eq!(
            g.observe(1100),
            None,
            "above the floor but inside the hysteresis band"
        );
        assert_eq!(g.observe(1249), None);
        assert_eq!(g.observe(1250), Some(false), "25% above the floor releases");
        assert!(!g.protected());
        assert_eq!(
            g.observe(1100),
            None,
            "and does not re-engage until below the floor again"
        );
    }

    #[test]
    fn a_floor_of_zero_means_off() {
        let mut g = DiskGuard::new(0);
        assert!(!g.enabled());
        assert_eq!(g.observe(0), None);
        assert!(!g.protected());
    }

    #[test]
    fn the_largest_floor_cannot_overflow() {
        let mut g = DiskGuard::new(u64::MAX);
        assert_eq!(g.observe(u64::MAX - 1), Some(true));
        assert_eq!(g.observe(u64::MAX - 1), None);
    }

    #[cfg(unix)]
    #[test]
    fn free_space_of_a_real_directory_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        assert!(free_bytes(dir.path()).is_some_and(|b| b > 0));
        assert_eq!(free_bytes(&dir.path().join("does-not-exist")), None);
    }
}
