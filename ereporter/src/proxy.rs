// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
#![allow(dead_code)] // TODO(eliza): i'm still working on this bit
use ereporter_api::Entry;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use std::future::Future;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

pub trait ReporterProxy {
    fn list(
        &self,
        starting_seq: Option<Generation>,
        limit: Option<usize>,
    ) -> impl Future<Output = Result<Vec<Entry>, Error>> + Send;

    fn acknowledge(
        &self,
        seq: Generation,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn recover_seq(
        &self,
        seq: Generation,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

pub(crate) struct ProxyWorker<P> {
    proxy: P,
    log: slog::Logger,
    id: Uuid,
    server: watch::Receiver<Option<crate::server::State>>,
    server_reqs: mpsc::Receiver<crate::buffer::ServerReq>,
    seqs: mpsc::Receiver<Generation>,
}

impl<P: ReporterProxy> ProxyWorker<P> {
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                seq = self.seqs.recv() => {
                    if let Some(seq) = seq {
                        if let Err(_e) = self.proxy.recover_seq(seq).await {
                            // TODO(eliza): waht do about error
                        }
                    }

                },
            }
        }
    }
}
