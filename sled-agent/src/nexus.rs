// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use nexus_client::Client as NexusClient;

use internal_dns::resolver::{ResolveError, Resolver};
use internal_dns::ServiceName;
use nexus_client::types::SledAgentInfo;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use omicron_common::api::external::Generation;
use sled_hardware::HardwareManager;
use slog::Logger;
use std::future::Future;
use std::net::SocketAddrV6;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};
use uuid::Uuid;

use crate::instance_manager::InstanceManager;

/// A thin wrapper over a progenitor-generated NexusClient.
///
/// Also attaches the "DNS resolver" for historical reasons.
#[derive(Clone)]
pub struct NexusClientWithResolver {
    client: NexusClient,
    resolver: Arc<Resolver>,
}

impl NexusClientWithResolver {
    pub fn new(
        log: &Logger,
        resolver: Arc<Resolver>,
    ) -> Result<Self, ResolveError> {
        Ok(Self::new_from_resolver_with_port(
            log,
            resolver,
            NEXUS_INTERNAL_PORT,
        ))
    }

    pub fn new_from_resolver_with_port(
        log: &Logger,
        resolver: Arc<Resolver>,
        port: u16,
    ) -> Self {
        let client = reqwest::ClientBuilder::new()
            .dns_resolver(resolver.clone())
            .build()
            .expect("Failed to build client");

        let dns_name = ServiceName::Nexus.srv_name();
        Self {
            client: NexusClient::new_with_client(
                &format!("http://{dns_name}:{port}"),
                client,
                log.new(o!("component" => "NexusClient")),
            ),
            resolver,
        }
    }

    /// Access the progenitor-based Nexus Client.
    pub fn client(&self) -> &NexusClient {
        &self.client
    }

    /// Access the DNS resolver used by the Nexus Client.
    ///
    /// WARNING: If you're using this resolver to access an IP address of
    /// another service, be aware that it might change if that service moves
    /// around! Be cautious when accessing and persisting IP addresses of other
    /// services.
    pub fn resolver(&self) -> &Arc<Resolver> {
        &self.resolver
    }
}

type NexusRequestFut = dyn Future<Output = ()> + Send;
type NexusRequest = Pin<Box<NexusRequestFut>>;

/// A queue of futures which represent requests to Nexus.
pub struct NexusRequestQueue {
    tx: mpsc::UnboundedSender<NexusRequest>,
    _worker: JoinHandle<()>,
}

impl NexusRequestQueue {
    /// Creates a new request queue, along with a worker which executes
    /// any incoming tasks.
    pub fn new() -> Self {
        // TODO(https://github.com/oxidecomputer/omicron/issues/1917):
        // In the future, this should basically just be a wrapper around a
        // generation number, and we shouldn't be serializing requests to Nexus.
        //
        // In the meanwhile, we're using an unbounded_channel for simplicity, so
        // that we don't need to cope with dropped notifications /
        // retransmissions.
        let (tx, mut rx) = mpsc::unbounded_channel();

        let _worker = tokio::spawn(async move {
            while let Some(fut) = rx.recv().await {
                fut.await;
            }
        });

        Self { tx, _worker }
    }

    /// Gets access to the sending portion of the request queue.
    ///
    /// Callers can use this to add their own requests.
    pub fn sender(&self) -> &mpsc::UnboundedSender<NexusRequest> {
        &self.tx
    }
}

pub fn d2n_params(
    params: &dns_service_client::types::DnsConfigParams,
) -> nexus_client::types::DnsConfigParams {
    nexus_client::types::DnsConfigParams {
        generation: params.generation,
        time_created: params.time_created,
        zones: params.zones.iter().map(d2n_zone).collect(),
    }
}

fn d2n_zone(
    zone: &dns_service_client::types::DnsConfigZone,
) -> nexus_client::types::DnsConfigZone {
    nexus_client::types::DnsConfigZone {
        zone_name: zone.zone_name.clone(),
        records: zone
            .records
            .iter()
            .map(|(n, r)| (n.clone(), r.iter().map(d2n_record).collect()))
            .collect(),
    }
}

fn d2n_record(
    record: &dns_service_client::types::DnsRecord,
) -> nexus_client::types::DnsRecord {
    match record {
        dns_service_client::types::DnsRecord::A(addr) => {
            nexus_client::types::DnsRecord::A(*addr)
        }
        dns_service_client::types::DnsRecord::Aaaa(addr) => {
            nexus_client::types::DnsRecord::Aaaa(*addr)
        }
        dns_service_client::types::DnsRecord::Srv(srv) => {
            nexus_client::types::DnsRecord::Srv(nexus_client::types::Srv {
                port: srv.port,
                prio: srv.prio,
                target: srv.target.clone(),
                weight: srv.weight,
            })
        }
    }
}

// Although it is a bit awkward to define these conversions here, it frees us
// from depending on sled_storage/sled_hardware in the nexus_client crate.

pub(crate) trait ConvertInto<T>: Sized {
    fn convert(self) -> T;
}

impl ConvertInto<nexus_client::types::PhysicalDiskKind>
    for sled_hardware::DiskVariant
{
    fn convert(self) -> nexus_client::types::PhysicalDiskKind {
        use nexus_client::types::PhysicalDiskKind;

        match self {
            sled_hardware::DiskVariant::U2 => PhysicalDiskKind::U2,
            sled_hardware::DiskVariant::M2 => PhysicalDiskKind::M2,
        }
    }
}

impl ConvertInto<nexus_client::types::Baseboard> for sled_hardware::Baseboard {
    fn convert(self) -> nexus_client::types::Baseboard {
        nexus_client::types::Baseboard {
            serial_number: self.identifier().to_string(),
            part_number: self.model().to_string(),
            revision: self.revision(),
        }
    }
}

impl ConvertInto<nexus_client::types::DatasetKind>
    for sled_storage::dataset::DatasetKind
{
    fn convert(self) -> nexus_client::types::DatasetKind {
        use nexus_client::types::DatasetKind;
        use sled_storage::dataset::DatasetKind::*;

        match self {
            CockroachDb => DatasetKind::Cockroach,
            Crucible => DatasetKind::Crucible,
            Clickhouse => DatasetKind::Clickhouse,
            ClickhouseKeeper => DatasetKind::ClickhouseKeeper,
            ExternalDns => DatasetKind::ExternalDns,
            InternalDns => DatasetKind::InternalDns,
        }
    }
}

// Somewhat arbitrary bound size, large enough that we should never hit it.
const QUEUE_SIZE: usize = 256;

pub enum NexusNotifierMsg {
    // Inform nexus about a change to this sled-agent. This is just a
    // notification to perform a send. The request is constructed inside
    // `NexusNotifierTask`.
    NotifyNexusAboutSelf,
}

#[derive(Debug)]
pub struct NexusNotifierHandle {
    tx: mpsc::Sender<NexusNotifierMsg>,
}

// A successful reply from nexus
pub enum NexusSuccess {
    Get(SledAgentInfo),
    Put,
}

// A mechanism owned by the `NexusNotifierTask` that allows it to access
// enough information to send a `SledAgentInfo` to Nexus.
pub struct NexusNotifierInput {
    sled_id: Uuid,
    sled_address: SocketAddrV6,
    nexus_client: NexusClient,
    hardware: HardwareManager,
    instances: InstanceManager,
}

/// A mechanism for notifying nexus about this sled agent
///
/// The semantics are as follows:
///  1. At any time there is a single outstanding HTTP request to nexus
///  2. On startup, this task gets the latest sled-agent info if any
///     and saves it.
///  3. Whenever the state needs to be updated to a value different
///     from what nexus has, the generation number is bumped
///     and the new state transmitted.
///  4. If a caller requests an update to be made to nexus it gets
///     marked as pending and when the last outstanding request completes
///     a new update will be made if necessary.
pub struct NexusNotifierTask {
    input: NexusNotifierInput,
    log: Logger,
    rx: mpsc::Receiver<NexusNotifierMsg>,

    // The last known value either put or gotten from nexus
    //
    // We only send `Get` requests if we haven't learned any info yet
    nexus_known_info: Option<SledAgentInfo>,

    // The info sent in the last outstanding `Put` request
    //
    // In some cases the result of a put is indeterminate. We may get a timeout
    // back for instance. If the value of the data changes when we try another
    // put, we must bump the generation number. We therefore save any proposed
    // nexus info until we know it has been successfully put.
    proposed_info: Option<SledAgentInfo>,

    // Do we need to notify nexus about an update to our state?
    pending_notification: bool,

    // We only have one outstanding nexus request at a time.
    // We spawn a task to manage this request so we don't block our main notifier task.
    // We wait for a response on a channel.
    outstanding_request: Option<
        tokio::task::JoinHandle<
            Result<
                NexusSuccess,
                nexus_client::Error<nexus_client::types::Error>,
            >,
        >,
    >,
    // A notification sent from the outstanding task when it has completed.
    outstanding_req_ready: Arc<Notify>,
}

impl NexusNotifierTask {
    pub fn new(
        input: NexusNotifierInput,
        log: &Logger,
    ) -> (NexusNotifierTask, NexusNotifierHandle) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        (
            NexusNotifierTask {
                input,
                log: log.new(o!("component" => "NexusNotifierTask")),
                rx,
                nexus_known_info: None,
                proposed_info: None,
                // We start with pending true, because we always want to attempt
                // to retrieve the current generation number before we upsert
                // ourselves.
                pending_notification: true,
                outstanding_request: None,
                outstanding_req_ready: Arc::new(Notify::new()),
            },
            NexusNotifierHandle { tx },
        )
    }

    /// Run the main receive loop of the `NexusNotifierTask`
    ///
    /// This should be spawned into a tokio task
    pub async fn run(mut self) {
        loop {
            const RETRY_TIMEOUT: Duration = Duration::from_secs(5);
            let mut interval = interval(RETRY_TIMEOUT);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            tokio::select! {
                req = self.rx.recv() => {
                    let Some(req) = req else {
                        warn!(self.log, "All senders dropped. Exiting.");
                        break;
                    };
                    match req {
                        NexusNotifierMsg::NotifyNexusAboutSelf => {
                            // We'll contact nexus on the next timeout
                            self.pending_notification = true;
                        }
                    }
                }
                _ =  self.outstanding_req_ready.notified() => {
                    // Our request task has completed. Let's check the result.
                    self.handle_nexus_reply().await;

                }
                _ = interval.tick(), if self.pending_notification => {
                    self.contact_nexus().await;
                }
            }
        }
    }

    /// Notify nexus about self
    async fn contact_nexus(&mut self) {
        // Is there already an outstanding request to nexus?
        if self.outstanding_request.is_some() {
            return;
        }

        let client = self.input.nexus_client.clone();
        let sled_id = self.input.sled_id;

        // Have we learned about any generations stored in CRDB yet?
        if let Some(known_info) = &self.nexus_known_info {
            let role = if self.input.hardware.is_scrimlet() {
                nexus_client::types::SledRole::Scrimlet
            } else {
                nexus_client::types::SledRole::Gimlet
            };
            let mut info = SledAgentInfo {
                sa_address: self.input.sled_address.to_string(),
                role,
                baseboard: self.input.hardware.baseboard().convert(),
                usable_hardware_threads: self
                    .input
                    .hardware
                    .online_processor_count(),
                usable_physical_ram: self
                    .input
                    .hardware
                    .usable_physical_ram_bytes()
                    .into(),
                reservoir_size: self.input.instances.reservoir_size().into(),
                generation: known_info.generation,
            };
            // We don't need to send a request if the info is identical to what
            // nexus knows
            if info == *known_info {
                return;
            }

            // If we already have a proposed value and it's different from what
            // we're about to propose, we need to bump the generation number
            // greater than what we last proposed.
            if let Some(proposed_info) = &self.proposed_info {
                if *proposed_info != info {
                    info.generation = proposed_info.generation.next();
                } else {
                    // Re-try to send nexus the same info
                    info.generation = proposed_info.generation;
                }
            } else {
                // We don't have a proposed value, so bump the generation
                // of the value that nexus knows.
                info.generation = known_info.generation.next();
            }

            // Unconditionally save what we are about to propose
            self.proposed_info = Some(info.clone());

            self.outstanding_request = Some(tokio::spawn(async move {
                client
                    .sled_agent_put(&sled_id, &info)
                    .await
                    .map(|_| NexusSuccess::Put)
            }));
        } else {
            self.outstanding_request = Some(tokio::spawn(async move {
                client
                    .sled_agent_get(&sled_id)
                    .await
                    .map(|info| NexusSuccess::Get(info.into_inner()))
            }));
        }
    }

    /// Handle a reply from nexus by extracting the value from a `JoinHandle`
    async fn handle_nexus_reply(&mut self) {
        let res = match self
            .outstanding_request
            .take()
            .expect("missing JoinHandle")
            .await
        {
            Ok(res) => res,
            Err(e) => {
                error!(self.log, "Nexus request task exited prematurely: {e}");
                return;
            }
        };
        match res {
            Ok(NexusSuccess::Get(info)) => match &mut self.nexus_known_info {
                None => {
                    self.nexus_known_info = Some(info);
                }
                Some(known) => {
                    warn!(
                        self.log,
                        "Got unexpected `Get` response";
                        "known" => ?known,
                        "got" => ?info
                    );
                    if known.generation < info.generation {
                        warn!(
                            self.log,
                            "Replacing known info with unexpected info";
                            "known_generation" => %known.generation,
                            "new_generation" => %info.generation
                        );
                        *known = info;
                    } else if known.generation == info.generation {
                        if *known != info {
                            error!(
                               self.log,
                               "Different SledAgentInfo held by nexus and sled-agent for same generation";
                                "generation" => %info.generation
                            );
                        }
                    } else {
                        // This is just a stale response, although it still shouldn't occur
                        warn!(self.log, "Received stale response"; "info" => ?info);
                    }
                }
            },
            Ok(NexusSuccess::Put) => {}
            Err(e) => {}
        }
    }
}
