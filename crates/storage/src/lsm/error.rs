use std::fmt;
use std::io;

#[derive(Debug)]
pub enum LsmError {
    Io(io::Error),
    /// On-disk data failed validation (checksum, structure, missing file).
    /// The directory is left untouched; the caller decides how to recover.
    Corruption(String),
    /// A previous I/O error left in-memory and on-disk state possibly
    /// divergent. The handle refuses all further work; reopen to recover.
    Poisoned(String),
    /// The caller passed arguments that contradict the engine's contract
    /// (for example a non-increasing `applied` index). Nothing was written
    /// and the handle is still usable.
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, LsmError>;

impl fmt::Display for LsmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LsmError::Io(e) => write!(f, "lsm i/o error: {e}"),
            LsmError::Corruption(m) => write!(f, "lsm corruption: {m}"),
            LsmError::Poisoned(m) => write!(f, "lsm handle poisoned by earlier failure: {m}"),
            LsmError::InvalidInput(m) => write!(f, "lsm invalid input: {m}"),
        }
    }
}

impl std::error::Error for LsmError {}

impl From<io::Error> for LsmError {
    fn from(e: io::Error) -> Self {
        LsmError::Io(e)
    }
}

pub(crate) fn corrupt<T>(msg: impl Into<String>) -> Result<T> {
    Err(LsmError::Corruption(msg.into()))
}
