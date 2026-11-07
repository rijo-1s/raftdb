//! The logging contract: one valid JSON object per line, stable event names,
//! no log forging, correct filtering, safe under concurrency.

use std::io;
use std::sync::{Arc, Mutex};

use raftdb_log::{build, events, Format, LogConfig, LogError};
use serde_json::Value;
use tracing::dispatcher::with_default;
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
impl Buf {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

fn cfg(format: Format, filter: &str) -> LogConfig {
    LogConfig {
        format,
        filter: filter.to_string(),
    }
}

/// Run `f` under a JSON logger; return every line parsed.
fn capture_json(filter: &str, f: impl FnOnce()) -> Vec<Value> {
    let buf = Buf::default();
    let d = build(&cfg(Format::Json, filter), buf.clone()).unwrap();
    with_default(&d, f);
    buf.text()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not valid JSON ({e}): {l:?}")))
        .collect()
}

fn capture_text(f: impl FnOnce()) -> String {
    let buf = Buf::default();
    let d = build(&cfg(Format::Text, "info"), buf.clone()).unwrap();
    with_default(&d, f);
    buf.text()
}

#[test]
fn every_line_is_one_json_object_with_the_core_fields() {
    let lines = capture_json("info", || {
        tracing::info!(answer = 42u64, ok = true, name = "x", "hello");
        tracing::warn!("second");
    });
    assert_eq!(lines.len(), 2);
    let l = &lines[0];
    assert_eq!(l["level"], "INFO");
    assert_eq!(
        l["message"], "hello",
        "flattened: the message is a top-level key"
    );
    assert_eq!(l["answer"], 42, "numbers stay numbers");
    assert_eq!(l["ok"], true, "booleans stay booleans");
    assert_eq!(l["name"], "x");
    assert!(l["target"].as_str().unwrap().contains("json_logs"));
    assert_eq!(lines[1]["level"], "WARN");
    // RFC 3339-shaped UTC timestamp
    let ts = l["timestamp"].as_str().expect("timestamp is a string");
    let b = ts.as_bytes();
    assert!(
        b.len() >= 20 && b[4] == b'-' && b[7] == b'-' && b[10] == b'T' && ts.ends_with('Z'),
        "timestamp {ts:?}"
    );
}

#[test]
fn hostile_content_cannot_forge_or_break_lines() {
    let nasty = "line1\nline2\r\n\"} {\"level\":\"FATAL\",\"event\":\"forged\"}\t\u{1}\u{7f}é✓";
    let lines = capture_json("info", || {
        tracing::info!(key = nasty, "msg {nasty}");
        tracing::info!("after");
    });
    assert_eq!(
        lines.len(),
        2,
        "the hostile value must not create extra lines"
    );
    assert_eq!(
        lines[0]["key"], nasty,
        "the value round-trips exactly (escaped on the wire)"
    );
    assert!(lines[0].get("event").is_none(), "no forged field");
    assert_eq!(lines[1]["message"], "after");
}

#[test]
fn the_filter_is_honoured_and_a_bad_filter_is_an_error_not_a_panic() {
    let at_info = capture_json("info", || {
        tracing::debug!("quiet");
        tracing::info!("loud");
    });
    assert_eq!(at_info.len(), 1);
    let at_debug = capture_json("debug", || {
        tracing::debug!("quiet");
        tracing::info!("loud");
    });
    assert_eq!(at_debug.len(), 2);
    let per_target = capture_json("error,json_logs=debug", || {
        tracing::debug!("only my target")
    });
    assert_eq!(per_target.len(), 1, "per-target directives work");
    for bad in ["info,foo=notalevel", "[unclosed", "foo=="] {
        assert!(
            matches!(
                build(&cfg(Format::Json, bad), Buf::default()),
                Err(LogError::BadFilter(_))
            ),
            "{bad:?}"
        );
    }
}

#[test]
fn the_node_span_tags_every_event_inside_it() {
    let lines = capture_json("info", || {
        let _g = raftdb_log::node_span(7).entered();
        tracing::info!("inside");
    });
    assert_eq!(lines[0]["span"]["name"], "node");
    assert_eq!(lines[0]["span"]["node"], 7);
}

#[test]
fn every_catalogued_event_has_its_stable_name_level_and_fields() {
    let err = io::Error::other("disk on fire");
    let lines = capture_json("info", || {
        events::node_started(1, "127.0.0.1:7001", &[2, 3], "/data/n1", "lsm", false);
        events::became_leader(1, 5);
        events::stepped_down(1, 6);
        events::removed_from_cluster(1);
        events::readiness_changed(1, true, "ok");
        events::readiness_changed(1, false, "leader_known");
        events::slow_fsync(1, 900_000, 500_000);
        events::compaction_finished(1, 120, 4, 9);
        events::storage_failure(1, "wal_append", &err);
        events::recovery_refused(1, "/data/n1/MANIFEST", &"checksum mismatch");
        events::lsm_quarantined(1, &"bad magic", "/data/n1/lsm.quarantine-1");
        events::metrics_listening(1, "127.0.0.1:9001");
        events::shutdown_started(1);
        events::shutdown_complete(1);
    });
    let by = |i: usize| (&lines[i]["event"], &lines[i]["level"]);
    let expect: &[(&str, &str)] = &[
        ("node_started", "INFO"),
        ("became_leader", "INFO"),
        ("stepped_down", "INFO"),
        ("removed_from_cluster", "WARN"),
        ("readiness_changed", "INFO"),
        ("readiness_changed", "WARN"),
        ("slow_fsync", "WARN"),
        ("compaction_finished", "INFO"),
        ("storage_failure", "ERROR"),
        ("recovery_refused", "ERROR"),
        ("lsm_quarantined", "WARN"),
        ("metrics_listening", "INFO"),
        ("shutdown_started", "INFO"),
        ("shutdown_complete", "INFO"),
    ];
    assert_eq!(lines.len(), expect.len());
    for (i, (name, level)) in expect.iter().enumerate() {
        assert_eq!(
            (by(i).0, by(i).1),
            (&Value::from(*name), &Value::from(*level)),
            "event #{i}"
        );
        assert_eq!(lines[i]["node"], 1, "every event carries `node`");
    }
    assert_eq!(lines[0]["peers"], "2,3");
    assert_eq!(lines[0]["bind"], "127.0.0.1:7001");
    assert_eq!(lines[0]["join"], false);
    assert_eq!(lines[1]["term"], 5);
    assert_eq!(lines[4]["ready"], true);
    assert_eq!(lines[5]["reason"], "leader_known");
    assert_eq!(lines[6]["micros"], 900_000);
    assert_eq!(lines[6]["threshold_micros"], 500_000);
    assert_eq!(lines[7]["l1_files"], 9);
    assert_eq!(lines[8]["error"], "disk on fire");
    assert_eq!(lines[8]["operation"], "wal_append");
    assert_eq!(lines[9]["path"], "/data/n1/MANIFEST");
    assert_eq!(lines[10]["moved_to"], "/data/n1/lsm.quarantine-1");
    assert_eq!(lines[10]["reason"], "bad magic");
    assert_eq!(lines[11]["addr"], "127.0.0.1:9001");
}

#[test]
fn event_helpers_cannot_forge_lines_even_in_text_mode() {
    let evil = "boom\n2099-01-01T00:00:00Z ERROR forged line\r\nmore";
    let out = capture_text(|| {
        events::storage_failure(1, "op\nx", &evil);
        events::recovery_refused(1, "/p\n/q", &evil);
        events::readiness_changed(1, false, evil);
        events::node_started(1, "a\nb", &[1], "d\ne", "lsm\n", true);
    });
    assert_eq!(
        out.lines().count(),
        4,
        "four events must be exactly four lines, got:\n{out}"
    );
    assert!(
        out.contains("boom\\n2099"),
        "newlines are visible escapes, not real newlines"
    );
    // and in JSON the same helpers still produce valid, single-line objects
    let lines = capture_json("info", || events::storage_failure(1, "op", &evil));
    assert_eq!(lines.len(), 1);
}

#[test]
fn clean_escapes_control_characters_and_leaves_the_rest_alone() {
    assert_eq!(events::clean("plain é✓ text"), "plain é✓ text");
    assert_eq!(events::clean("a\nb\rc\td"), "a\\nb\\rc\\td");
    assert_eq!(events::clean("\u{1b}[31m"), "\\u{001b}[31m");
    assert!(!events::clean("x\u{0}y\u{85}z")
        .chars()
        .any(char::is_control));
}

#[test]
fn concurrent_writers_never_interleave_a_line() {
    let buf = Buf::default();
    let d = build(&cfg(Format::Json, "info"), buf.clone()).unwrap();
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let d = d.clone();
            std::thread::spawn(move || {
                with_default(&d, || {
                    for i in 0..250u64 {
                        events::became_leader(t, i);
                        tracing::info!(thread = t, i, payload = "x".repeat(200).as_str(), "bulk");
                    }
                })
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let text = buf.text();
    let mut n = 0;
    for l in text.lines() {
        serde_json::from_str::<Value>(l)
            .unwrap_or_else(|e| panic!("interleaved/corrupt line ({e}): {l:?}"));
        n += 1;
    }
    assert_eq!(n, 8 * 250 * 2, "no event lost");
}

#[test]
fn format_and_env_parsing() {
    assert_eq!("json".parse::<Format>(), Ok(Format::Json));
    assert_eq!(" JSON ".parse::<Format>(), Ok(Format::Json));
    assert_eq!("text".parse::<Format>(), Ok(Format::Text));
    assert!(matches!(
        "yaml".parse::<Format>(),
        Err(LogError::BadFormat(_))
    ));

    let d = LogConfig::from_vars(None, None).unwrap();
    assert_eq!((d.format, d.filter.as_str()), (Format::Text, "info"));
    let c = LogConfig::from_vars(Some("warn,raftdb=debug".into()), Some("json".into())).unwrap();
    assert_eq!(
        (c.format, c.filter.as_str()),
        (Format::Json, "warn,raftdb=debug")
    );
    let blank = LogConfig::from_vars(Some("  ".into()), Some("".into())).unwrap();
    assert_eq!(
        (blank.format, blank.filter.as_str()),
        (Format::Text, "info"),
        "blank = unset"
    );
    assert!(
        matches!(
            LogConfig::from_vars(None, Some("xml".into())),
            Err(LogError::BadFormat(_))
        ),
        "a bad format must not be silently ignored"
    );
}

#[test]
fn text_mode_is_one_readable_line_per_event() {
    let out = capture_text(|| events::became_leader(3, 9));
    assert_eq!(out.lines().count(), 1);
    assert!(
        out.contains("became leader") && out.contains("term=9") && out.contains("node=3"),
        "{out}"
    );
}
