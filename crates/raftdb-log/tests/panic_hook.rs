//! One test in its own process: the panic hook and the global logger are
//! process-wide.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use raftdb_log::{build, install_panic_hook, Format, LogConfig};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);
impl io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<'a> MakeWriter<'a> for Buf {
    type Writer = Buf;
    fn make_writer(&'a self) -> Buf {
        self.clone()
    }
}

static PREVIOUS_HOOK_RAN: AtomicBool = AtomicBool::new(false);

#[test]
fn a_panic_on_any_thread_becomes_one_structured_error_line_and_the_old_hook_still_runs() {
    let buf = Buf::default();
    let d = build(
        &LogConfig {
            format: Format::Json,
            filter: "info".into(),
        },
        buf.clone(),
    )
    .unwrap();
    tracing::dispatcher::set_global_default(d).unwrap();
    std::panic::set_hook(Box::new(|_| {
        PREVIOUS_HOOK_RAN.store(true, Ordering::SeqCst)
    }));
    install_panic_hook();

    let h = std::thread::Builder::new()
        .name("worker-x".into())
        .spawn(|| panic!("kaboom \"quoted\"\nsecond line"))
        .unwrap();
    assert!(h.join().is_err());
    let _ = std::thread::spawn(|| std::panic::panic_any(42u32)).join(); // non-string payload

    let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2, "one line per panic:\n{text}");
    let l = &lines[0];
    assert_eq!(l["event"], "panic");
    assert_eq!(l["level"], "ERROR");
    assert_eq!(l["thread"], "worker-x");
    assert_eq!(l["panic_message"], "kaboom \"quoted\"\nsecond line");
    assert!(
        l["location"].as_str().unwrap().contains("panic_hook.rs"),
        "{l}"
    );
    assert_eq!(lines[1]["panic_message"], "non-string panic payload");
    assert!(
        PREVIOUS_HOOK_RAN.load(Ordering::SeqCst),
        "the previous hook must still be called"
    );
}
