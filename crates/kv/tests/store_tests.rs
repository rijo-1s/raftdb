use kv::{Command, KvStore};

#[test]
fn set_command_round_trips() {
    let cmd = Command::Set {
        key: "foo".into(),
        value: "bar".into(),
    };
    let encoded = cmd.encode();
    assert_eq!(Command::decode(&encoded), Some(cmd));
}

#[test]
fn delete_command_round_trips() {
    let cmd = Command::Delete { key: "foo".into() };
    let encoded = cmd.encode();
    assert_eq!(Command::decode(&encoded), Some(cmd));
}

#[test]
fn command_survives_values_containing_arbitrary_bytes() {
    // Length-prefixed encoding, not delimiter-based - a value containing
    // null bytes or anything else must not corrupt decoding.
    let cmd = Command::Set {
        key: "k".into(),
        value: "line1\0line2\nline3".into(),
    };
    let encoded = cmd.encode();
    assert_eq!(Command::decode(&encoded), Some(cmd));
}

#[test]
fn malformed_bytes_fail_to_decode() {
    assert_eq!(Command::decode(&[]), None);
    assert_eq!(Command::decode(&[99]), None); // unknown tag
    assert_eq!(Command::decode(&[1, 0, 0, 0]), None); // truncated Set
}

#[test]
fn kv_store_apply_set_and_get() {
    let mut store = KvStore::new();
    store
        .apply(
            &Command::Set {
                key: "a".into(),
                value: "1".into(),
            }
            .encode(),
        )
        .unwrap();
    store
        .apply(
            &Command::Set {
                key: "b".into(),
                value: "2".into(),
            }
            .encode(),
        )
        .unwrap();
    assert_eq!(store.get("a"), Some(&"1".to_string()));
    assert_eq!(store.get("b"), Some(&"2".to_string()));
    assert_eq!(store.get("c"), None);
}

#[test]
fn kv_store_apply_delete() {
    let mut store = KvStore::new();
    store
        .apply(
            &Command::Set {
                key: "a".into(),
                value: "1".into(),
            }
            .encode(),
        )
        .unwrap();
    store
        .apply(&Command::Delete { key: "a".into() }.encode())
        .unwrap();
    assert_eq!(store.get("a"), None);
}

#[test]
fn kv_store_set_overwrites() {
    let mut store = KvStore::new();
    store
        .apply(
            &Command::Set {
                key: "a".into(),
                value: "1".into(),
            }
            .encode(),
        )
        .unwrap();
    store
        .apply(
            &Command::Set {
                key: "a".into(),
                value: "2".into(),
            }
            .encode(),
        )
        .unwrap();
    assert_eq!(store.get("a"), Some(&"2".to_string()));
}

#[test]
fn kv_store_apply_rejects_malformed_bytes() {
    let mut store = KvStore::new();
    assert!(store.apply(&[99]).is_err());
}

#[test]
fn snapshot_and_restore_round_trip() {
    let mut store = KvStore::new();
    store
        .apply(
            &Command::Set {
                key: "a".into(),
                value: "1".into(),
            }
            .encode(),
        )
        .unwrap();
    store
        .apply(
            &Command::Set {
                key: "b".into(),
                value: "2".into(),
            }
            .encode(),
        )
        .unwrap();
    store
        .apply(
            &Command::Set {
                key: "c".into(),
                value: "3".into(),
            }
            .encode(),
        )
        .unwrap();
    store
        .apply(&Command::Delete { key: "b".into() }.encode())
        .unwrap();

    let snapshot = store.snapshot();
    let restored = KvStore::restore(&snapshot).unwrap();

    assert_eq!(restored, store);
    assert_eq!(restored.get("a"), Some(&"1".to_string()));
    assert_eq!(restored.get("b"), None);
    assert_eq!(restored.get("c"), Some(&"3".to_string()));
    assert_eq!(restored.len(), 2);
}

#[test]
fn empty_store_snapshot_round_trips() {
    let store = KvStore::new();
    let snapshot = store.snapshot();
    let restored = KvStore::restore(&snapshot).unwrap();
    assert!(restored.is_empty());
}
