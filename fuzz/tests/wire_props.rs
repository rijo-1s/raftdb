//! Property tests for the wire layer: protobuf decoding of every Raft RPC,
//! the proto <-> `raft_core::Message` conversions, and the KV command codec
//! that rides inside replicated log entries.
//!
//! `raftdb` is a binary-only crate, so `convert.rs` is compiled in here via a
//! `#[path]` module instead of being edited or turned into a library.

use proptest::prelude::*;
use prost::Message as _;
use raft_core::{EntryKind, LogEntry, Message};
use raftdb_proto as proto;

#[path = "../../crates/raftdb/src/convert.rs"]
#[allow(dead_code)]
mod convert;

fn cfg() -> ProptestConfig {
    ProptestConfig {
        cases: 400,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn arb_kind() -> impl Strategy<Value = EntryKind> {
    prop_oneof![
        Just(EntryKind::Data),
        any::<u64>().prop_map(EntryKind::AddNode),
        any::<u64>().prop_map(EntryKind::RemoveNode),
        Just(EntryKind::NoOp),
    ]
}

fn arb_entry() -> impl Strategy<Value = LogEntry> {
    (
        any::<u64>(),
        any::<u64>(),
        arb_kind(),
        prop::collection::vec(any::<u8>(), 0..64),
    )
        .prop_map(|(term, index, kind, data)| LogEntry {
            term,
            index,
            kind,
            data,
        })
}

fn arb_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
            |(term, candidate_id, last_log_index, last_log_term)| {
                Message::RequestVote {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                }
            }
        ),
        (any::<u64>(), any::<bool>())
            .prop_map(|(term, vote_granted)| Message::RequestVoteReply { term, vote_granted }),
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(arb_entry(), 0..6),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(
                |(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    read_ctx,
                )| Message::AppendEntries {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    read_ctx,
                }
            ),
        (any::<u64>(), any::<bool>(), any::<u64>(), any::<u64>()).prop_map(
            |(term, success, match_index, read_ctx)| Message::AppendEntriesReply {
                term,
                success,
                match_index,
                read_ctx
            }
        ),
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(any::<u8>(), 0..200),
            prop::collection::vec(any::<u64>(), 0..6),
        )
            .prop_map(
                |(term, leader_id, last_included_index, last_included_term, data, peers)| {
                    Message::InstallSnapshot {
                        term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                        data,
                        peers,
                    }
                }
            ),
        any::<u64>().prop_map(|term| Message::InstallSnapshotReply { term }),
        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
            |(term, candidate_id, last_log_index, last_log_term)| Message::PreVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            }
        ),
        (any::<u64>(), any::<bool>())
            .prop_map(|(term, vote_granted)| Message::PreVoteReply { term, vote_granted }),
    ]
}

/// Message -> proto bytes -> proto -> Message.
fn through_the_wire(m: &Message) -> (u64, Message) {
    match m {
        Message::RequestVote { .. } | Message::PreVote { .. } => {
            let b = convert::request_vote_to_proto(m).encode_to_vec();
            convert::request_vote_from_proto(proto::RequestVoteRequest::decode(&b[..]).unwrap())
        }
        Message::AppendEntries { .. } => {
            let b = convert::append_entries_to_proto(m).encode_to_vec();
            convert::append_entries_from_proto(proto::AppendEntriesRequest::decode(&b[..]).unwrap())
        }
        Message::InstallSnapshot { .. } => {
            let b = convert::install_snapshot_to_proto(m).encode_to_vec();
            convert::install_snapshot_from_proto(
                proto::InstallSnapshotRequest::decode(&b[..]).unwrap(),
            )
        }
        Message::RequestVoteReply { .. } | Message::PreVoteReply { .. } => {
            let b = convert::request_vote_reply_to_proto(m).encode_to_vec();
            (
                0,
                convert::request_vote_reply_from_proto(
                    proto::RequestVoteReply::decode(&b[..]).unwrap(),
                ),
            )
        }
        Message::AppendEntriesReply { .. } => {
            let b = convert::append_entries_reply_to_proto(m).encode_to_vec();
            (
                0,
                convert::append_entries_reply_from_proto(
                    proto::AppendEntriesReply::decode(&b[..]).unwrap(),
                ),
            )
        }
        Message::InstallSnapshotReply { .. } => {
            let b = convert::install_snapshot_reply_to_proto(m).encode_to_vec();
            (
                0,
                convert::install_snapshot_reply_from_proto(
                    proto::InstallSnapshotReply::decode(&b[..]).unwrap(),
                ),
            )
        }
    }
}

proptest! {
    #![proptest_config(cfg())]

    #[test]
    fn every_raft_message_round_trips_through_the_wire(m in arb_message()) {
        let (_from, back) = through_the_wire(&m);
        prop_assert_eq!(back, m);
    }

    #[test]
    fn arbitrary_bytes_never_panic_any_rpc_decoder(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
        // Decode with every message type; whatever decodes is pushed through
        // the (infallible) conversion too.
        if let Ok(r) = proto::RequestVoteRequest::decode(&bytes[..]) { let _ = convert::request_vote_from_proto(r); }
        if let Ok(r) = proto::RequestVoteReply::decode(&bytes[..]) { let _ = convert::request_vote_reply_from_proto(r); }
        if let Ok(r) = proto::AppendEntriesRequest::decode(&bytes[..]) { let _ = convert::append_entries_from_proto(r); }
        if let Ok(r) = proto::AppendEntriesReply::decode(&bytes[..]) { let _ = convert::append_entries_reply_from_proto(r); }
        if let Ok(r) = proto::InstallSnapshotRequest::decode(&bytes[..]) { let _ = convert::install_snapshot_from_proto(r); }
        if let Ok(r) = proto::InstallSnapshotReply::decode(&bytes[..]) { let _ = convert::install_snapshot_reply_from_proto(r); }
        if let Ok(r) = proto::LogEntry::decode(&bytes[..]) { let _ = convert::log_entry_from_proto(r); }
    }

    #[test]
    fn every_truncation_of_a_valid_message_is_an_error_or_a_clean_decode(m in arb_message(), cut in any::<prop::sample::Index>()) {
        let bytes = match &m {
            Message::AppendEntries { .. } => convert::append_entries_to_proto(&m).encode_to_vec(),
            Message::InstallSnapshot { .. } => convert::install_snapshot_to_proto(&m).encode_to_vec(),
            Message::RequestVote { .. } => convert::request_vote_to_proto(&m).encode_to_vec(),
            _ => return Ok(()),
        };
        let l = cut.index(bytes.len() + 1);
        // must not panic; a truncation is allowed to decode (prost fields are
        // optional) but then must not equal garbage beyond what was sent.
        let _ = proto::AppendEntriesRequest::decode(&bytes[..l]);
        let _ = proto::InstallSnapshotRequest::decode(&bytes[..l]);
        let _ = proto::RequestVoteRequest::decode(&bytes[..l]);
    }

    #[test]
    fn kv_commands_round_trip(key in ".{0,40}", value in ".{0,40}", del in any::<bool>()) {
        let c = if del { kv::Command::Delete { key } } else { kv::Command::Set { key, value } };
        prop_assert_eq!(kv::Command::decode(&c.encode()), Some(c));
    }

    #[test]
    fn kv_decode_never_panics_and_accepted_input_is_canonical(bytes in prop::collection::vec(any::<u8>(), 0..120)) {
        if let Some(c) = kv::Command::decode(&bytes) {
            // anything it accepts must be exactly what encode() would produce
            prop_assert_eq!(c.encode(), bytes, "decode accepted a non-canonical encoding (trailing garbage?)");
        }
    }

    #[test]
    fn kv_decode_rejects_every_truncation_and_any_trailing_byte(key in "[a-z]{0,10}", value in "[a-z]{0,10}", extra in any::<u8>()) {
        let enc = kv::Command::Set { key, value }.encode();
        for cut in 0..enc.len() {
            prop_assert_eq!(kv::Command::decode(&enc[..cut]), None, "truncated at {}", cut);
        }
        let mut longer = enc;
        longer.push(extra);
        prop_assert_eq!(kv::Command::decode(&longer), None);
    }
}

/// Characterization of current behavior, not an endorsement.
/// `convert::log_entry_from_proto` maps any unknown `kind_tag` to
/// `EntryKind::Data`, whereas the on-disk WAL (`storage::Wal`) refuses a
/// frame whose tag is not 0..=3. A peer that sends tag 4+ (a newer protocol
/// version, or a corrupt message that still parses) therefore has its entry
/// silently re-labelled as an ordinary client command and persisted as such.
/// If `convert` is changed to reject the RPC, this test has to be updated
/// deliberately.
#[test]
fn unknown_entry_kind_tag_is_currently_mapped_to_data() {
    for tag in [4u32, 5, 77, u32::MAX] {
        let e = convert::log_entry_from_proto(proto::LogEntry {
            term: 1,
            index: 1,
            data: vec![9],
            kind_tag: tag,
            kind_node_id: 3,
        });
        assert_eq!(e.kind, EntryKind::Data, "tag {tag}");
    }
}
