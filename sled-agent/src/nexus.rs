// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use nexus_client::Client as NexusClient;
use omicron_common::api::external::Generation;

use crate::vmm_reservoir::VmmReservoirManagerHandle;
use internal_dns::resolver::{ResolveError, Resolver};
use internal_dns::ServiceName;
use nexus_client::types::SledAgentInfo;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use sled_hardware::HardwareManager;
use slog::Logger;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use tokio::time::{interval, Duration, MissedTickBehavior};
use uuid::Uuid;

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

impl NexusNotifierHandle {
    pub async fn notify_nexus_about_self(&self, log: &Logger) {
        if let Err(_) =
            self.tx.send(NexusNotifierMsg::NotifyNexusAboutSelf).await
        {
            warn!(log, "Failed to send to NexusNotifierTask: did it exit?");
        }
    }
}

/// A successful reply from nexus
enum NexusSuccess {
    // Contains data returned from Nexus
    Get(SledAgentInfo),

    // Contains data that was successfully put to Nexus
    Put(SledAgentInfo),
}

/// What sled-agent has confirmed that Nexus knows about this sled-agent
enum NexusKnownInfo {
    // CRDB doesn't contain a record for this sled-agent
    NotFound,
    Found(SledAgentInfo),
}

impl NexusKnownInfo {
    fn generation(&self) -> Generation {
        match self {
            NexusKnownInfo::NotFound => Generation::new(),
            NexusKnownInfo::Found(known) => known.generation,
        }
    }
}

// A mechanism owned by the `NexusNotifierTask` that allows it to access
// enough information to send a `SledAgentInfo` to Nexus.
pub struct NexusNotifierInput {
    pub sled_id: Uuid,
    pub sled_address: SocketAddrV6,
    pub nexus_client: NexusClient,
    pub hardware: HardwareManager,
    pub vmm_reservoir_manager: VmmReservoirManagerHandle,
}

/// A mechanism for notifying nexus about this sled agent
///
/// The semantics are as follows:
///  1. At any time there is a single outstanding HTTP request to nexus
///  2. On startup, this task gets the latest sled-agent info, if any, from
///     nexus, and saves it.
///  3. Whenever the state needs to be updated to a value different
///     from what nexus has, the generation number is bumped
///     and the new state transmitted.
///  4. If a caller requests an update to be made to nexus it gets and it succeeds
///     the known value is set to what was updated.
///  5. If the request fails, we go ahead and set the known state to `None`,
///     since we are not sure if the last update succeeded. This will
///     trigger a get request to get the latest state.
pub struct NexusNotifierTask {
    input: NexusNotifierInput,
    log: Logger,
    rx: mpsc::Receiver<NexusNotifierMsg>,

    // The last known value either put or gotten from nexus
    //
    // We only send `Get` requests if we haven't learned any info yet
    nexus_known_info: Option<NexusKnownInfo>,

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

    /// If we haven't yet learned the latest info that nexus has about us
    /// then go ahead and send a get request. Otherwise, if necessaary, send
    /// a put request to nexus with the latest `SledAgentInfo`.
    ///
    /// Only one outstanding request is allowed at a time, so if there is
    /// already one outstanding then we return and will try again later.
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
                reservoir_size: self
                    .input
                    .vmm_reservoir_manager
                    .reservoir_size()
                    .into(),
                generation: known_info.generation(),
            };

            // Does CRDB actually contain an existing record for this sled?
            match known_info {
                NexusKnownInfo::NotFound => {
                    // Nothing to do. We must send the request as is.
                }
                NexusKnownInfo::Found(known) => {
                    // We don't need to send a request if the info is identical to what
                    // nexus knows
                    if info == *known {
                        self.pending_notification = false;
                        return;
                    }

                    // Bump the generation of the value that nexus knows, so
                    // that the update takes precedence.
                    info.generation = known.generation.next();
                }
            }

            self.outstanding_request = Some(tokio::spawn(async move {
                client
                    .sled_agent_put(&sled_id, &info)
                    .await
                    .map(|_| NexusSuccess::Put(info))
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

    /// Handle a reply from nexus by extracting the value from the `JoinHandle` of
    /// the last outstanding request.
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
            Ok(NexusSuccess::Get(info)) => {
                info!(
                    self.log,
                    "Retrieved SledAgentInfo from Nexus: {:?}", info
                );
                assert!(self.nexus_known_info.is_none());
                self.nexus_known_info = Some(NexusKnownInfo::Found(info));
            }
            Ok(NexusSuccess::Put(info)) => {
                // Unwrap Safety: we must have known and proposed values in
                // order to have submitted a PUT request in the first place.
                info!(
                    self.log,
                    "Successfully put SledAgentInfo to nexus";
                    "old_generation" =>
                        %self.nexus_known_info.as_ref().unwrap().generation(),
                    "new_generation" => %info.generation,
                );
                self.nexus_known_info = Some(NexusKnownInfo::Found(info));
            }
            Err(e) => {
                if e.status() == Some(http::StatusCode::NOT_FOUND) {
                    // Was this for a get request? Then it just means we haven't
                    // registered ourselves yet.
                    if self.nexus_known_info.is_none() {
                        self.nexus_known_info = Some(NexusKnownInfo::NotFound);
                        return;
                    }
                }
                warn!(self.log, "Received Error from Nexus: {:?}", e);
            }
        }
    }
}
