//! Structured logging for RaftDB.
//!
//! * One event per line. In JSON mode every line is a single, valid JSON
//!   object, no matter what the message contains (newlines and quotes are
//!   escaped, so a hostile key or error string cannot forge extra events).
//! * Stable, machine-readable event names live in [`events`]; alert rules and
//!   log pipelines key on the `event` field, not on message text.
//! * [`install_panic_hook`] turns a panic on any thread into a structured
//!   `event="panic"` line before the default hook runs.
//!
//! Configuration comes from the environment so no CLI change is required:
//!
//! ```text
//! RAFTDB_LOG=info                  # EnvFilter syntax, e.g. "warn,raftdb=debug"
//! RAFTDB_LOG_FORMAT=json|text      # default text
//! ```

use std::fmt;
use std::str::FromStr;

use tracing::Dispatch;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::EnvFilter;

pub mod events;

/// Output format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// One JSON object per line.
    Json,
    /// Human-readable single lines.
    Text,
}

impl FromStr for Format {
    type Err = LogError;
    fn from_str(s: &str) -> Result<Self, LogError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Format::Json),
            "text" | "plain" => Ok(Format::Text),
            other => Err(LogError::BadFormat(other.to_string())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LogConfig {
    pub format: Format,
    /// `tracing_subscriber::EnvFilter` directives, e.g. `info` or `warn,raftdb=debug`.
    pub filter: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            format: Format::Text,
            filter: "info".to_string(),
        }
    }
}

impl LogConfig {
    /// Read `RAFTDB_LOG` and `RAFTDB_LOG_FORMAT`. Unset variables keep the
    /// defaults; a set-but-invalid format is an error (never silently ignored).
    pub fn from_env() -> Result<Self, LogError> {
        Self::from_vars(
            std::env::var("RAFTDB_LOG").ok(),
            std::env::var("RAFTDB_LOG_FORMAT").ok(),
        )
    }

    /// The pure form of [`from_env`](Self::from_env), for tests.
    pub fn from_vars(filter: Option<String>, format: Option<String>) -> Result<Self, LogError> {
        let mut c = Self::default();
        if let Some(f) = filter.filter(|f| !f.trim().is_empty()) {
            c.filter = f;
        }
        if let Some(f) = format.filter(|f| !f.trim().is_empty()) {
            c.format = f.parse()?;
        }
        Ok(c)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LogError {
    BadFilter(String),
    BadFormat(String),
    AlreadyInitialized,
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogError::BadFilter(e) => write!(f, "invalid RAFTDB_LOG filter: {e}"),
            LogError::BadFormat(v) => write!(f, "invalid log format {v:?} (expected json or text)"),
            LogError::AlreadyInitialized => write!(f, "a global logger is already installed"),
        }
    }
}

impl std::error::Error for LogError {}

/// Build (but do not install) a logger writing to `writer`.
pub fn build<W>(cfg: &LogConfig, writer: W) -> Result<Dispatch, LogError>
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let filter = EnvFilter::try_new(&cfg.filter).map_err(|e| LogError::BadFilter(e.to_string()))?;
    let base = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter(filter)
        .with_ansi(false);
    Ok(match cfg.format {
        Format::Json => Dispatch::new(
            base.json()
                // `message` and every field become top-level keys.
                .flatten_event(true)
                // the innermost span (e.g. `node`) is attached to each event
                .with_current_span(true)
                .with_span_list(false)
                .finish(),
        ),
        Format::Text => Dispatch::new(base.finish()),
    })
}

/// Install the process-wide logger writing to stderr. Fails (without
/// panicking) if one is already installed.
pub fn init(cfg: &LogConfig) -> Result<(), LogError> {
    let d = build(cfg, std::io::stderr)?;
    tracing::dispatcher::set_global_default(d).map_err(|_| LogError::AlreadyInitialized)
}

/// A span carrying the node id; attach it to the node loop's task/thread
/// (`.instrument(raftdb_log::node_span(id))`) so every event inside is tagged.
pub fn node_span(node: u64) -> tracing::Span {
    tracing::info_span!("node", node)
}

/// Log panics as structured `event="panic"` lines, then run the previous hook
/// (so the default stderr backtrace behaviour is unchanged).
pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());
        let payload = info.payload();
        let message = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_string());
        let thread = std::thread::current();
        tracing::error!(
            event = "panic",
            thread = thread.name().unwrap_or("unnamed"),
            location = %location,
            panic_message = %message,
            "thread panicked"
        );
        previous(info);
    }));
}
