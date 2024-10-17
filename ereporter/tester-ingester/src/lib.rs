// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use dropshot::{
    endpoint, HttpError, HttpResponseOk, RequestContext, TypedBody,
};
use nexus_types::internal_api::params::EreporterInfo;
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::Serialize;
use std::collections::hash_map::{Entry, HashMap};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use uuid::Uuid;

/// Configuration for a standalone ereport ingester.
#[derive(Clone, Debug, Parser)]
pub struct IngesterConfig {
    /// Directory in which to store ingested ereports.
    pub data_dir: camino::Utf8PathBuf,

    /// The address for the mock Nexus server used to register.
    ///
    /// This program starts a mock version of Nexus, which is used only to
    /// register the producers and collectors. This allows them to operate
    /// as they usually would, registering each other with Nexus so that an
    /// assignment between them can be made.
    #[arg(
        long,
        default_value_t = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 12345, 0, 0)
    )]
    pub nexus: SocketAddrV6,

    /// Interval at which ereports are ingested.
    #[arg(long, default_value_t = 5)]
    pub interval_secs: usize,
}

impl IngesterConfig {
    pub async fn run(self, log: slog::Logger) -> anyhow::Result<()> {
        let (registration_tx, registrations) = mpsc::channel(128);
        let ingester = Ingester {
            reporters: HashMap::new(),
            log: log.new(slog::o!("component" => "ingester")),
            path: self.data_dir,
            registrations,
            interval: Duration::from_secs(self.interval_secs as u64),
        };
        let ingester = tokio::spawn(ingester.run());

        let _server = {
            let apictx = ServerContext { registration_tx };
            let mut api = dropshot::ApiDescription::new();
            api.register(cpapi_ereporters_post)?;
            dropshot::HttpServerStarter::new(
                &dropshot::ConfigDropshot {
                    default_handler_task_mode:
                        dropshot::HandlerTaskMode::Detached,
                    bind_address: self.nexus.into(),
                    ..Default::default()
                },
                api,
                apictx,
                &log.new(slog::o!("component" => "standalone-nexus")),
            )
            .map_err(|e| {
                anyhow::anyhow!("failed to start standalone nexus server: {e}")
            })?
            .start()
        };
        slog::info!(
            log,
            "created standalone nexus server for ereporter registration";
            "address" => %self.nexus,
        );

        ingester.await.context("ingester task panicked")??;
        Ok(())
    }
}

struct Ingester {
    reporters: HashMap<Uuid, Reporter>,
    registrations: mpsc::Receiver<Registration>,
    log: slog::Logger,
    path: Utf8PathBuf,
    interval: Duration,
}

struct Reporter {
    path: Utf8PathBuf,
    clients: HashMap<SocketAddr, ereporter_client::Client>,
}

struct Registration {
    uuid: Uuid,
    addr: SocketAddr,
    tx: oneshot::Sender<Generation>,
}

impl Ingester {
    async fn run(mut self) -> anyhow::Result<()> {
        if !self.path.exists() {
            std::fs::create_dir_all(&self.path).with_context(|| {
                format!("couldn't create directory {}", self.path)
            })?;
            slog::info!(self.log, "created data dir"; "path" => %self.path);
        }
        let mut interval = time::interval(self.interval);
        slog::info!(self.log, "ingesting ereports every {:?}", self.interval);
        loop {
            tokio::select! {
                biased;
                req = self.registrations.recv() => {
                    let Some(reg) = req else {
                        slog::warn!(self.log, "the registration-request sender has gone away?");
                        anyhow::bail!("the registration request sender went away unexpectedly");
                    };
                    self.register_client(reg).await?;
                },
                _ = interval.tick() => {
                    self.collect().await?;
                }
            }
        }
    }

    async fn collect(&mut self) -> anyhow::Result<()> {
        slog::debug!(self.log, "collecting ereports...");
        for (id, reporter) in &self.reporters {
            slog::debug!(self.log, "collecting ereports from {id}");
            for (addr, client) in &reporter.clients {
                let reports = match client.ereports_list(id, None, None).await {
                    Ok(e) => e.into_inner(),
                    Err(e) => {
                        slog::error!(self.log,
                            "error collecting ereports";
                            "reporter_id" => %id,
                            "reporter_addr" => %addr,
                            "error" => %e,
                        );
                        continue;
                    }
                };
                for ereporter_client::types::Entry {
                    seq,
                    value,
                    reporter_id,
                } in reports.items
                {
                    match value {
                        ereporter_client::types::EntryKind::Ereport(report) => {
                            slog::info!(
                                &self.log,
                                "ereport from {reporter_id}";
                                "reporter_id" => %reporter_id,
                                "seq" => %seq,
                                "report" => ?report,
                            );
                            match tokio::fs::File::create_new(
                                reporter.path.join(&format!("{seq}.json")),
                            )
                            .await
                            {
                                Err(error) => {
                                    slog::error!(self.log,
                                        "couldn't create new file for ereport, may already have been ingested!";
                                        "reporter_id" => %reporter_id,
                                        "seq" => %seq,
                                        "error" => %error,
                                    );
                                    continue;
                                }
                                Ok(mut f) => {
                                    let bytes = serde_json::to_vec_pretty(&report)
                                    .with_context(|| format!("failed to serialize ereport {seq} from {reporter_id}"))?;

                                    f
                                    .write_all(&bytes
                                    )
                                    .await
                                    .with_context(|| format!("failed to write ereport {seq} from {reporter_id}"))?;
                                }
                            }
                        }

                        ereporter_client::types::EntryKind::DataLoss {
                            dropped,
                        } => {
                            slog::warn!(self.log,
                                "reporter {reporter_id} reports data loss at seq {seq}";
                                "reporter_id" => %reporter_id,
                                "seq" => %seq,
                                "dropped" => ?dropped,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn register_client(
        &mut self,
        Registration { uuid, addr, tx }: Registration,
    ) -> anyhow::Result<()> {
        let reporter = self.reporters.entry(uuid).or_insert_with(|| {
            let path = self.path.join(uuid.to_string());
            Reporter { path, clients: HashMap::new() }
        });
        std::fs::create_dir_all(&reporter.path).with_context(|| {
            format!(
                "couldn't create reporter ereport directory {}",
                reporter.path
            )
        })?;
        let seq = recover_seq(&reporter.path).await?;
        match reporter.clients.entry(addr) {
            Entry::Occupied(_) => {
                slog::info!(
                    self.log,
                    "recovering sequence for reporter";
                    "reporter_id" => %uuid,
                    "address" => %addr,
                    "seq" => %seq,
                );
            }
            Entry::Vacant(e) => {
                slog::info!(
                    self.log,
                    "registered new endpoint for reporter";
                    "reporter_id" => %uuid,
                    "address" => %addr,
                    "seq" => %seq,
                );
                let log = self.log.new(slog::o!(
                        "reporter_id" => uuid.to_string(),
                        "reporter_addr" => addr.to_string(),
                ));
                let client = ereporter_client::Client::new(
                    &format!("http://{addr}"),
                    log,
                );
                e.insert(client);
            }
        }

        if tx.send(seq).is_err() {
            slog::warn!(
                self.log,
                "reporter gave up on registration attempt unexpectedly"
            );
        }
        Ok(())
    }
}

async fn recover_seq(path: &Utf8PathBuf) -> anyhow::Result<Generation> {
    let mut dir = tokio::fs::read_dir(path)
        .await
        .with_context(|| format!("failed to read {path}"))?;
    let mut max = 0;
    while let Some(entry) = dir
        .next_entry()
        .await
        .with_context(|| format!("failed to get next entry in {path}"))?
    {
        let path = entry.path();
        let path = Utf8Path::from_path(path.as_ref())
            .with_context(|| format!("path {} was not utf8", path.display()))?;
        if path.is_dir() {
            continue;
        }
        if let Some(file) = path.file_stem() {
            match file.parse::<u32>() {
                Ok(seq) => max = std::cmp::max(seq, max),
                Err(_) => {
                    continue;
                }
            }
        }
    }

    Ok(Generation::from_u32(max))
}

#[derive(Clone)]
struct ServerContext {
    registration_tx: mpsc::Sender<Registration>,
}

/// Register an error reporter with Nexus, returning the next sequence
/// number for an error report from that reporter.
#[endpoint {
    method = POST,
    path = "/ereport/reporters",
}]
async fn cpapi_ereporters_post(
    rqctx: RequestContext<ServerContext>,
    identity: TypedBody<EreporterInfo>,
) -> Result<HttpResponseOk<EreporterRegistered>, HttpError> {
    let ctx = rqctx.context();
    let EreporterInfo { reporter_id, address } = identity.into_inner();
    let (tx, rx) = oneshot::channel();
    ctx.registration_tx
        .send(Registration { addr: address, uuid: reporter_id, tx })
        .await
        .expect("the main task should not have gone away");
    let seq = rx.await.expect("the main task should not have given up on us");
    Ok(HttpResponseOk(EreporterRegistered { seq }))
}

/// Response to error reporter registration requests.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct EreporterRegistered {
    /// The starting sequence number of the next error report from this
    /// reporter. If the reporter has not been seen by Nexus previously, this
    /// may be 0.
    pub seq: Generation,
}
