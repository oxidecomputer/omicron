// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::EreportData;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub(crate) enum ServerReq {
    TruncateTo {
        seq: Generation,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    List {
        start_seq: Option<Generation>,
        limit: usize,
        tx: oneshot::Sender<Vec<ereporter_api::Ereport>>,
    },
}

pub(crate) struct Buffer {
    pub(crate) seq: Generation,
    pub(crate) buf: VecDeque<ereporter_api::Ereport>,
    pub(crate) log: slog::Logger,
    pub(crate) id: Uuid,
    pub(crate) ereports: mpsc::Receiver<EreportData>,
    pub(crate) requests: mpsc::Receiver<ServerReq>,
}

impl Buffer {
    pub(crate) async fn run(mut self) {
        while let Some(req) = self.requests.recv().await {
            match req {
                // Asked to list ereports!
                ServerReq::List { start_seq, limit, tx } => {
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
                    if tx.send(list).is_err() {
                        slog::warn!(self.log, "client canceled list request");
                    }
                }
                ServerReq::TruncateTo { seq, tx } if seq > self.seq => {
                    if tx.send(Err(Error::invalid_value(
                    "seq",
                    "cannot truncate to a sequence number greater than the current maximum"
                ))).is_err() {
                    // If the receiver no longer cares about the response to
                    // this request, no biggie.
                    slog::warn!(self.log, "client canceled truncate request");
                }
                }
                ServerReq::TruncateTo { seq, tx } => {
                    let prev_len = self.buf.len();
                    self.buf.retain(|ereport| ereport.seq <= seq);

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
                    todo!()
                }
            }
        }

        slog::info!(self.log, "server requests channel closed, shutting down");
    }

    fn push_ereport(&mut self, ereport: EreportData) {
        let EreportData { facts, class, time_created } = ereport;
        let seq = self.seq;
        self.buf.push_back(ereporter_api::Ereport {
            facts,
            class,
            time_created,
            reporter_id: self.id,
            seq,
        });
        self.seq = self.seq.next();
        slog::trace!(
            self.log,
            "recorded ereport";
            "seq" => %seq,
        );
    }
}
