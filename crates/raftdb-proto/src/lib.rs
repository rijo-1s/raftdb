//! Generated gRPC/protobuf code for raftdb's two services:
//! `RaftTransport` (node-to-node consensus RPCs) and `ClientApi` (what the
//! CLI talks to). See `proto/raftdb.proto` for the source of truth.

// The `Result<_, tonic::Status>` signatures below are emitted by tonic-build and
// cannot be changed here; `Status` is simply a large type.
#![allow(clippy::result_large_err)]

tonic::include_proto!("raftdb");
