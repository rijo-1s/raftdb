//! The two gRPC services a running node exposes. Both are thin: decode the
//! proto request, hand it to the node's event loop via its channel, wait
//! for the loop's answer, encode the proto reply. No Raft logic lives
//! here - see `node_loop.rs` for that.

use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use raftdb_proto as proto;

use crate::convert;
use crate::node_loop::LoopEvent;

pub struct RaftTransportService {
    pub tx: mpsc::Sender<LoopEvent>,
    /// This node's id (for log lines).
    pub node_id: u64,
}

#[tonic::async_trait]
impl proto::raft_transport_server::RaftTransport for RaftTransportService {
    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteReply>, Status> {
        let principal = request
            .extensions()
            .get::<crate::authz::Principal>()
            .cloned();
        let (from, msg) = convert::request_vote_from_proto(request.into_inner());
        crate::authz::check_sender(principal.as_ref(), from, self.node_id)?;
        let reply_msg = send_inbound(&self.tx, from, msg).await?;
        Ok(Response::new(convert::request_vote_reply_to_proto(
            &reply_msg,
        )))
    }

    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesReply>, Status> {
        let principal = request
            .extensions()
            .get::<crate::authz::Principal>()
            .cloned();
        let (from, msg) = convert::append_entries_from_proto(request.into_inner());
        crate::authz::check_sender(principal.as_ref(), from, self.node_id)?;
        let reply_msg = send_inbound(&self.tx, from, msg).await?;
        Ok(Response::new(convert::append_entries_reply_to_proto(
            &reply_msg,
        )))
    }

    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotReply>, Status> {
        let principal = request
            .extensions()
            .get::<crate::authz::Principal>()
            .cloned();
        let (from, msg) = convert::install_snapshot_from_proto(request.into_inner());
        crate::authz::check_sender(principal.as_ref(), from, self.node_id)?;
        let reply_msg = send_inbound(&self.tx, from, msg).await?;
        Ok(Response::new(convert::install_snapshot_reply_to_proto(
            &reply_msg,
        )))
    }
}

// `Status` is tonic's own error type and is what every gRPC handler returns, so
// the large-`Err` lint is not actionable here.
#[allow(clippy::result_large_err)]
async fn send_inbound(
    tx: &mpsc::Sender<LoopEvent>,
    from: raft_core::NodeId,
    msg: raft_core::Message,
) -> Result<raft_core::Message, Status> {
    let (respond, rx) = oneshot::channel();
    tx.send(LoopEvent::Inbound { from, msg, respond })
        .await
        .map_err(|_| Status::internal("node event loop is not running"))?;
    rx.await
        .map_err(|_| Status::internal("node event loop dropped the request"))
}

pub struct ClientApiService {
    pub metrics: std::sync::Arc<crate::metrics::Metrics>,
    pub tx: mpsc::Sender<LoopEvent>,
}

#[tonic::async_trait]
impl proto::client_api_server::ClientApi for ClientApiService {
    async fn put(
        &self,
        request: Request<proto::PutRequest>,
    ) -> Result<Response<proto::PutReply>, Status> {
        let t0 = std::time::Instant::now();
        let req = request.into_inner();
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(LoopEvent::ClientPut {
                key: req.key,
                value: req.value,
                client_id: req.client_id,
                seq: req.seq,
                respond,
            })
            .await
            .map_err(|_| Status::internal("node event loop is not running"))?;
        match rx
            .await
            .map_err(|_| Status::internal("node event loop dropped the request"))?
        {
            Ok(()) => {
                self.metrics.observe_write_latency(t0.elapsed());
                Ok(Response::new(proto::PutReply {
                    ok: true,
                    error: String::new(),
                    leader_hint: None,
                }))
            }
            Err(e) => {
                let hint = e.leader_hint;
                Ok(Response::new(proto::PutReply {
                    ok: false,
                    error: e.message,
                    leader_hint: hint,
                }))
            }
        }
    }

    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetReply>, Status> {
        let t0 = std::time::Instant::now();
        let req = request.into_inner();
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(LoopEvent::ClientGet {
                key: req.key,
                respond,
            })
            .await
            .map_err(|_| Status::internal("node event loop is not running"))?;
        match rx
            .await
            .map_err(|_| Status::internal("node event loop dropped the request"))?
        {
            Ok(Some(value)) => {
                self.metrics.observe_read_latency(t0.elapsed());
                Ok(Response::new(proto::GetReply {
                    found: true,
                    value,
                    error: String::new(),
                    leader_hint: None,
                }))
            }
            Ok(None) => Ok(Response::new(proto::GetReply {
                found: false,
                value: String::new(),
                error: String::new(),
                leader_hint: None,
            })),
            Err(e) => Ok(Response::new(proto::GetReply {
                found: false,
                value: String::new(),
                error: e.message,
                leader_hint: e.leader_hint,
            })),
        }
    }

    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteReply>, Status> {
        let req = request.into_inner();
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(LoopEvent::ClientDelete {
                key: req.key,
                client_id: req.client_id,
                seq: req.seq,
                respond,
            })
            .await
            .map_err(|_| Status::internal("node event loop is not running"))?;
        match rx
            .await
            .map_err(|_| Status::internal("node event loop dropped the request"))?
        {
            Ok(()) => Ok(Response::new(proto::DeleteReply {
                ok: true,
                error: String::new(),
                leader_hint: None,
            })),
            Err(e) => {
                let hint = e.leader_hint;
                Ok(Response::new(proto::DeleteReply {
                    ok: false,
                    error: e.message,
                    leader_hint: hint,
                }))
            }
        }
    }

    async fn backup(
        &self,
        _request: Request<proto::BackupRequest>,
    ) -> Result<Response<proto::BackupReply>, Status> {
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(LoopEvent::Backup { respond })
            .await
            .map_err(|_| Status::internal("node event loop is not running"))?;
        match rx
            .await
            .map_err(|_| Status::internal("node event loop dropped the request"))?
        {
            Ok(backup) => {
                // Off the node loop: validate (never hand out something
                // restore would refuse) and checksum the payload.
                let last_applied = backup.last_applied;
                let encoded =
                    tokio::task::spawn_blocking(move || backup.check().map(|_| backup.encode()))
                        .await
                        .map_err(|_| Status::internal("backup encoding task failed"))?;
                match encoded {
                    Ok(data) => Ok(Response::new(proto::BackupReply {
                        ok: true,
                        error: String::new(),
                        data,
                        last_applied,
                    })),
                    Err(error) => Ok(Response::new(proto::BackupReply {
                        ok: false,
                        error: format!("the exported state failed validation: {error}"),
                        data: Vec::new(),
                        last_applied: 0,
                    })),
                }
            }
            Err(error) => Ok(Response::new(proto::BackupReply {
                ok: false,
                error,
                data: Vec::new(),
                last_applied: 0,
            })),
        }
    }

    async fn add_node(
        &self,
        request: Request<proto::AddNodeRequest>,
    ) -> Result<Response<proto::AddNodeReply>, Status> {
        let req = request.into_inner();
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(LoopEvent::ClientAddNode {
                node_id: req.node_id,
                respond,
            })
            .await
            .map_err(|_| Status::internal("node event loop is not running"))?;
        match rx
            .await
            .map_err(|_| Status::internal("node event loop dropped the request"))?
        {
            Ok(()) => Ok(Response::new(proto::AddNodeReply {
                ok: true,
                error: String::new(),
                leader_hint: None,
            })),
            Err(e) => {
                let hint = e.leader_hint;
                Ok(Response::new(proto::AddNodeReply {
                    ok: false,
                    error: e.message,
                    leader_hint: hint,
                }))
            }
        }
    }

    async fn remove_node(
        &self,
        request: Request<proto::RemoveNodeRequest>,
    ) -> Result<Response<proto::RemoveNodeReply>, Status> {
        let req = request.into_inner();
        let (respond, rx) = oneshot::channel();
        self.tx
            .send(LoopEvent::ClientRemoveNode {
                node_id: req.node_id,
                respond,
            })
            .await
            .map_err(|_| Status::internal("node event loop is not running"))?;
        match rx
            .await
            .map_err(|_| Status::internal("node event loop dropped the request"))?
        {
            Ok(()) => Ok(Response::new(proto::RemoveNodeReply {
                ok: true,
                error: String::new(),
                leader_hint: None,
            })),
            Err(e) => {
                let hint = e.leader_hint;
                Ok(Response::new(proto::RemoveNodeReply {
                    ok: false,
                    error: e.message,
                    leader_hint: hint,
                }))
            }
        }
    }
}
