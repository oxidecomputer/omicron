// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::server;
use crate::EreportData;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot, watch};
use uuid::Uuid;

pub(crate) enum ServerReq {
    TruncateTo {
        generation: Generation,
        seq: Generation,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    List {
        generation: Generation,
        start_seq: Option<Generation>,
        limit: usize,
        tx: oneshot::Sender<Result<Vec<ereporter_api::Entry>, Error>>,
    },
}

pub(crate) struct BufferWorker {
    seq: Generation,
    id: ereporter_api::ReporterId,
    buf: VecDeque<ereporter_api::Entry>,
    log: slog::Logger,
    ereports: mpsc::Receiver<EreportData>,
    requests: mpsc::Receiver<ServerReq>,
}

#[derive(Debug)]
pub(crate) struct Handle {
    pub(crate) requests: mpsc::Sender<ServerReq>,
    pub(crate) ereports: mpsc::Sender<EreportData>,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl BufferWorker {
    pub(crate) fn spawn(
        id: Uuid,
        log: &slog::Logger,
        buffer_capacity: usize,
        mut server: watch::Receiver<Option<server::State>>,
    ) -> Handle {
        let (requests_tx, requests) = mpsc::channel(128);
        let (ereports_tx, ereports) = mpsc::channel(buffer_capacity);
        let log = log.new(slog::o!("reporter_id" => id.to_string()));
        let task = tokio::task::spawn(async move {
            // Wait for the server to come up, and then register the reporter.
            let nexus_client::types::EreporterRegistered { seq, generation } = loop {
                let state = server.borrow_and_update().as_ref().cloned();
                if let Some(server) = state {
                    break server.register_reporter(&log, id).await;
                }
                if server.changed().await.is_err() {
                    slog::warn!(log, "server disappeared surprisingly before we could register the reporter!");
                    return;
                }
            };
            // Start running the buffer worker.
            let worker = Self {
                seq,
                id: ereporter_api::ReporterId { id, generation },
                buf: VecDeque::with_capacity(buffer_capacity),
                log: log.new(slog::o!("reporter_gen" => u64::from(generation))),
                ereports,
                requests,
            };
            worker.run().await
        });
        Handle { ereports: ereports_tx, requests: requests_tx, task }
    }

    pub(crate) async fn run(mut self) {
        while let Some(req) = self.requests.recv().await {
            match req {
                // Asked to list ereports at a previous generation --- we must
                // have been restarted since then. Indicate that that previous
                // version of ourself is lost and gone forever.
                ServerReq::List { generation, tx, .. }
                    if generation < self.id.generation =>
                {
                    slog::info!(self.log, "requested generation has been lost to the sands of time"; "requested_gen" => %generation);

                    if tx.send(Err(Error::Gone)).is_err() {
                        slog::warn!(self.log, "client canceled list request");
                    }
                }
                // Asked to list ereports!
                ServerReq::List { start_seq, limit, tx, .. } => {
                    // First, grab any new ereports and stick them in our cache.
                    while let Ok(ereport) = self.ereports.try_recv() {
                        self.push_ereport(ereport);
                    }

                    let mut list = {
                        let cap = std::cmp::min(limit, self.buf.len());
                        Vec::with_capacity(cap)
                    };

                    match start_seq {
                        // Start at lowest sequence number.
                        None => {
                            list.extend(
                                self.buf.iter().by_ref().take(limit).cloned(),
                            );
                        }
                        Some(seq) => {
                            todo!(
                                "eliza: draw the rest of the pagination {seq}"
                            )
                        }
                    }
                    slog::info!(
                        self.log,
                        "produced ereport batch from {start_seq:?}";
                        "start" => ?start_seq,
                        "len" => list.len(),
                        "limit" => limit
                    );
                    if tx.send(Ok(list)).is_err() {
                        slog::warn!(self.log, "client canceled list request");
                    }
                }
                // They asked for our previous generation, but we have been restarted!
                ServerReq::TruncateTo { generation, tx, .. }
                    if generation < self.id.generation =>
                {
                    slog::info!(
                        self.log,
                        "asked to discard ereports from a previous version of
                         ourself";
                        "requested_generation" => %generation,
                    );
                    if tx.send(Err(Error::Gone)).is_err() {
                        // If the receiver no longer cares about the response to
                        // this request, no biggie.
                        slog::warn!(
                            self.log,
                            "client canceled truncate request"
                        );
                    }
                }
                ServerReq::TruncateTo { seq, tx, .. } if seq > self.seq => {
                    if tx.send(Err(Error::invalid_value(
                        "seq",
                        "cannot truncate to a sequence number greater than the current maximum"
                    ))).is_err() {
                        // If the receiver no longer cares about the response to
                        // this request, no biggie.
                        slog::warn!(self.log, "client canceled truncate request");
                    }
                }
                ServerReq::TruncateTo { seq, tx, .. } => {
                    let prev_len = self.buf.len();
                    self.buf.retain(|ereport| ereport.seq > seq);

                    slog::info!(
                        self.log,
                        "truncated ereports up to {seq}";
                        "seq" => ?seq,
                        "dropped" => prev_len - self.buf.len(),
                        "remaining" => self.buf.len(),
                    );

                    if tx.send(Ok(())).is_err() {
                        // If the receiver no longer cares about the response to
                        // this request, no biggie.
                        slog::warn!(
                            self.log,
                            "client canceled truncate request"
                        );
                    }
                }
            }
        }

        slog::info!(self.log, "server requests channel closed, shutting down");
    }

    fn push_ereport(&mut self, ereport: EreportData) {
        let EreportData { facts, class, time_created } = ereport;
        let seq = self.seq;
        self.buf.push_back(ereporter_api::Entry {
            seq,
            reporter: self.id,
            value: ereporter_api::EntryKind::Ereport(ereporter_api::Ereport {
                facts,
                class,
                time_created,
            }),
        });
        self.seq = seq.next();
        slog::trace!(
            self.log,
            "recorded ereport";
            "seq" => %seq,
        );
    }
}
