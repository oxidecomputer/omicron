// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::EreportData;
use crate::ReporterError;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub(crate) enum ServerReq {
    Ack {
        generation: Generation,
        seq: Generation,
        tx: oneshot::Sender<Result<(), ReporterError>>,
    },
    List {
        generation: Generation,
        start_seq: Option<Generation>,
        limit: usize,
        tx: oneshot::Sender<Result<Vec<ereporter_api::Entry>, ReporterError>>,
    },
    Register {
        generation: Generation,
        generation_id: Uuid,
        tx: oneshot::Sender<Result<(), Error>>,
    },
}

pub(crate) struct BufferWorker {
    id: Uuid,
    buf: VecDeque<ereporter_api::Entry>,
    log: slog::Logger,
    seq: Generation,
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
    ) -> Handle {
        let (requests_tx, requests) = mpsc::channel(128);
        let (ereports_tx, ereports) = mpsc::channel(buffer_capacity);
        let log = log.new(slog::o!("reporter_id" => id.to_string()));
        let task = tokio::task::spawn(async move {
            // Start running the buffer worker.
            let worker = Self {
                id,
                buf: VecDeque::with_capacity(buffer_capacity),
                log,
                seq: Generation::new(),
                ereports,
                requests,
            };
            worker.run().await
        });
        Handle { ereports: ereports_tx, requests: requests_tx, task }
    }

    pub(crate) async fn run(mut self) {
        let my_generation_id = Uuid::new_v4();
        let generation = loop {
            let Some(req) = self.requests.recv().await else {
                slog::warn!(self.log, "server channel closed before this ereporter was registered");
                return;
            };
            match req {
                ServerReq::List { tx, .. } => {
                    if tx
                        .send(Err(ReporterError::unregistered(
                            my_generation_id,
                        )))
                        .is_err()
                    {
                        slog::warn!(self.log, "client canceled list request");
                    }
                }
                ServerReq::Ack { tx, .. } => {
                    if tx
                        .send(Err(ReporterError::unregistered(
                            my_generation_id,
                        )))
                        .is_err()
                    {
                        slog::warn!(
                            self.log,
                            "client canceled acknowledge request"
                        );
                    }
                }
                ServerReq::Register { generation, generation_id, tx }
                    if generation_id == my_generation_id =>
                {
                    if tx.send(Ok(())).is_err() {
                        slog::warn!(
                            self.log,
                            "client canceled register request"
                        );
                        // If we didn't successfully register, keep waiting.
                        continue;
                    }
                    slog::info!(
                        self.log,
                        "ereporter registered with Nexus";
                        "generation" => %generation,
                        "generation_id" => %generation_id,
                    );

                    break generation;
                }

                ServerReq::Register { generation, generation_id, tx } => {
                    slog::warn!(
                        self.log,
                        "nexus is trying to register this reporter at the \
                         wrong generation ID";
                        "my_generation_id" => %my_generation_id,
                        "generation_id" => %generation_id,
                        "generation" => %generation,
                    );
                    let error = Error::invalid_value(
                        "generation ID",
                        format!(
                            "attempted to register at {generation_id}, but \
                             the current generation is {my_generation_id}",
                        ),
                    );
                    if tx.send(Err(error)).is_err() {
                        slog::warn!(
                            self.log,
                            "client canceled register request"
                        );
                        // If we didn't successfully register, keep waiting.
                        continue;
                    }
                }
            }
        };
        let reporter_id = ereporter_api::ReporterId { generation, id: self.id };
        while let Some(req) = self.requests.recv().await {
            match req {
                // Asked to list ereports at a previous generation --- we must
                // have been restarted since then. Indicate that that previous
                // version of ourself is lost and gone forever.
                ServerReq::List { generation, tx, .. }
                    if generation < reporter_id.generation =>
                {
                    slog::info!(self.log, "requested generation has been lost to the sands of time"; "requested_gen" => %generation);

                    if tx.send(Err(Error::Gone.into())).is_err() {
                        slog::warn!(self.log, "client canceled list request");
                    }
                }
                // Asked to list ereports!
                ServerReq::List { start_seq, limit, tx, .. } => {
                    // First, grab any new ereports and stick them in our cache.
                    while let Ok(ereport) = self.ereports.try_recv() {
                        self.push_ereport(ereport, reporter_id);
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
                ServerReq::Ack { generation, tx, .. }
                    if generation < reporter_id.generation =>
                {
                    slog::info!(
                        self.log,
                        "asked to discard ereports from a previous version of
                         ourself";
                        "requested_generation" => %generation,
                    );
                    if tx.send(Err(Error::Gone.into())).is_err() {
                        // If the receiver no longer cares about the response to
                        // this request, no biggie.
                        slog::warn!(
                            self.log,
                            "client canceled truncate request"
                        );
                    }
                }
                ServerReq::Ack { seq, tx, .. } if seq > self.seq => {
                    if tx.send(Err(Error::invalid_value(
                        "seq",
                        "cannot truncate to a sequence number greater than the current maximum"
                    ).into())).is_err() {
                        // If the receiver no longer cares about the response to
                        // this request, no biggie.
                        slog::warn!(self.log, "client canceled truncate request");
                    }
                }
                ServerReq::Ack { seq, tx, .. } => {
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
                ServerReq::Register { generation, generation_id, tx } => {
                    let rsp = if generation == reporter_id.generation
                        && generation_id == my_generation_id
                    {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "reporter {} already registered at generation {} ({})",
                            reporter_id.id, reporter_id.generation, my_generation_id
                        )))
                    };
                    if tx.send(rsp).is_err() {
                        slog::warn!(
                            self.log,
                            "client canceled register request for generation {generation}"
                        );
                    }
                }
            }
        }

        slog::info!(self.log, "server requests channel closed, shutting down");
    }

    fn push_ereport(
        &mut self,
        ereport: EreportData,
        reporter: ereporter_api::ReporterId,
    ) {
        let EreportData { facts, class, time_created } = ereport;
        let seq = self.seq;
        self.buf.push_back(ereporter_api::Entry {
            seq,
            reporter,
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
