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
use tokio::sync::{mpsc, oneshot, Notify};
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

enum NexusNotifierMsg {
    // Inform nexus about a change to this sled-agent. This is just a
    // notification to perform a send. The request is constructed inside
    // `NexusNotifierTask`.
    NotifyNexusAboutSelf,

    // Return status of the `NexusNotifierTask`
    Status(oneshot::Sender<NexusNotifierTaskStatus>),
}

#[derive(Debug)]
pub struct NexusNotifierTaskStatus {
    pub nexus_known_info: Option<NexusKnownInfo>,
    pub has_pending_notification: bool,
    pub has_outstanding_request: bool,
    pub total_get_requests_started: u64,
    pub total_get_requests_completed: u64,
    pub total_put_requests_started: u64,
    pub total_put_requests_completed: u64,
    pub cancelled_pending_notifications: u64,
}

#[derive(Debug)]
pub struct NexusNotifierHandle {
    tx: mpsc::Sender<NexusNotifierMsg>,
}

#[derive(Debug)]
pub struct SenderOrReceiverDropped {}

impl NexusNotifierHandle {
    pub async fn notify_nexus_about_self(&self, log: &Logger) {
        if let Err(_) =
            self.tx.send(NexusNotifierMsg::NotifyNexusAboutSelf).await
        {
            warn!(log, "Failed to send to NexusNotifierTask: did it exit?");
        }
    }

    pub async fn get_status(
        &self,
    ) -> Result<NexusNotifierTaskStatus, SenderOrReceiverDropped> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NexusNotifierMsg::Status(tx))
            .await
            .map_err(|_| SenderOrReceiverDropped {})?;
        rx.await.map_err(|_| SenderOrReceiverDropped {})
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NexusKnownInfo {
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

/// Return the latest sled-agent info. Boxed for use in testing.
type GetSledAgentInfo = Box<dyn Fn(Generation) -> SledAgentInfo + Send>;

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
    sled_id: Uuid,
    nexus_client: NexusClient,
    get_sled_agent_info: GetSledAgentInfo,

    log: Logger,
    rx: mpsc::Receiver<NexusNotifierMsg>,

    // The last known value either put or gotten from nexus
    //
    // We only send `Get` requests if we haven't learned any info yet
    nexus_known_info: Option<NexusKnownInfo>,

    // Do we need to notify nexus about an update to our state?
    pending_notification: bool,

    // We only have one outstanding nexus request at a time.
    //
    // We spawn a task to manage this request so we don't block our main
    // notifier task. We wait for a notification on `outstanding_req_ready`
    // and then get the result from the `JoinHandle`.
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

    // Some stats for testing/debugging
    total_get_requests_started: u64,
    total_put_requests_started: u64,
    total_get_requests_completed: u64,
    total_put_requests_completed: u64,
    cancelled_pending_notifications: u64,
}

impl NexusNotifierTask {
    pub fn new(
        input: NexusNotifierInput,
        log: &Logger,
    ) -> (NexusNotifierTask, NexusNotifierHandle) {
        let NexusNotifierInput {
            sled_id,
            sled_address,
            nexus_client,
            hardware,
            vmm_reservoir_manager,
        } = input;

        // Box a function that can return the latest `SledAgentInfo`
        let get_sled_agent_info = Box::new(move |generation| {
            let role = if hardware.is_scrimlet() {
                nexus_client::types::SledRole::Scrimlet
            } else {
                nexus_client::types::SledRole::Gimlet
            };
            SledAgentInfo {
                sa_address: sled_address.to_string(),
                role,
                baseboard: hardware.baseboard().convert(),
                usable_hardware_threads: hardware.online_processor_count(),
                usable_physical_ram: hardware
                    .usable_physical_ram_bytes()
                    .into(),
                reservoir_size: vmm_reservoir_manager.reservoir_size().into(),
                generation,
            }
        });

        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        (
            NexusNotifierTask {
                sled_id,
                nexus_client,
                get_sled_agent_info,
                log: log.new(o!("component" => "NexusNotifierTask")),
                rx,
                nexus_known_info: None,
                // We start with pending true, because we always want to attempt
                // to retrieve the current generation number before we upsert
                // ourselves.
                pending_notification: true,
                outstanding_request: None,
                outstanding_req_ready: Arc::new(Notify::new()),
                total_get_requests_started: 0,
                total_put_requests_started: 0,
                total_get_requests_completed: 0,
                total_put_requests_completed: 0,
                cancelled_pending_notifications: 0,
            },
            NexusNotifierHandle { tx },
        )
    }

    pub fn new_for_test(
        sled_id: Uuid,
        nexus_client: NexusClient,
        get_sled_agent_info: GetSledAgentInfo,
        log: &Logger,
    ) -> (NexusNotifierTask, NexusNotifierHandle) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        (
            NexusNotifierTask {
                sled_id,
                nexus_client,
                get_sled_agent_info,
                log: log.new(o!("component" => "NexusNotifierTask")),
                rx,
                nexus_known_info: None,
                // We start with pending true, because we always want to attempt
                // to retrieve the current generation number before we upsert
                // ourselves.
                pending_notification: true,
                outstanding_request: None,
                outstanding_req_ready: Arc::new(Notify::new()),
                total_get_requests_started: 0,
                total_put_requests_started: 0,
                total_get_requests_completed: 0,
                total_put_requests_completed: 0,
                cancelled_pending_notifications: 0,
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
                        NexusNotifierMsg::Status(reply_tx) => {
                            let _ = reply_tx.send(NexusNotifierTaskStatus {
                                nexus_known_info: self.nexus_known_info.clone(),
                                has_outstanding_request: self.outstanding_request.is_some(),
                                has_pending_notification: self.pending_notification,
                                total_get_requests_started: self.total_get_requests_started,
                                total_put_requests_started: self.total_put_requests_started,
                                total_get_requests_completed: self.total_get_requests_completed,
                                total_put_requests_completed: self.total_put_requests_completed,
                                cancelled_pending_notifications: self.cancelled_pending_notifications
                            });
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

        let client = self.nexus_client.clone();
        let sled_id = self.sled_id;

        // Have we learned about any generations stored in CRDB yet?
        if let Some(known_info) = &self.nexus_known_info {
            let mut info = (self.get_sled_agent_info)(known_info.generation());

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
                        self.cancelled_pending_notifications += 1;
                        return;
                    }

                    // Bump the generation of the value that nexus knows, so
                    // that the update takes precedence.
                    info.generation = known.generation.next();
                }
            }

            let outstanding_req_ready = self.outstanding_req_ready.clone();
            self.total_put_requests_started += 1;
            self.outstanding_request = Some(tokio::spawn(async move {
                let res = client
                    .sled_agent_put(&sled_id, &info)
                    .await
                    .map(|_| NexusSuccess::Put(info));
                outstanding_req_ready.notify_one();
                res
            }));
        } else {
            let outstanding_req_ready = self.outstanding_req_ready.clone();
            self.total_get_requests_started += 1;
            self.outstanding_request = Some(tokio::spawn(async move {
                let res = client
                    .sled_agent_get(&sled_id)
                    .await
                    .map(|info| NexusSuccess::Get(info.into_inner()));
                outstanding_req_ready.notify_one();
                res
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
                self.total_get_requests_completed += 1;
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
                self.total_put_requests_completed += 1;
            }
            Err(e) => {
                if e.status() == Some(http::StatusCode::NOT_FOUND) {
                    // Was this for a get request? Then it just means we haven't
                    // registered ourselves yet.
                    //
                    // TODO: This is a bit too implicit for my liking.
                    // We should change the error type to include whether a put or get
                    // request was issued.
                    if self.nexus_known_info.is_none() {
                        self.nexus_known_info = Some(NexusKnownInfo::NotFound);
                        self.total_get_requests_completed += 1;
                        return;
                    }
                }
                warn!(self.log, "Received Error from Nexus: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::fakes::nexus::FakeNexusServer;
    use omicron_test_utils::dev::poll::{
        wait_for_condition as wait_for, CondCheckError,
    };

    use super::*;
    use omicron_common::api::external::{
        ByteCount, Error, Generation, LookupType, MessagePair, ResourceType,
    };
    use omicron_test_utils::dev::test_setup_log;
    use sled_hardware::Baseboard;

    /// Pretend to be CRDB storing info about a sled-agent
    #[derive(Default, Clone)]
    struct FakeCrdb {
        info: Arc<std::sync::Mutex<Option<SledAgentInfo>>>,
    }

    // A mechanism for injecting errors into nexus responses from the test
    enum NexusErrorInjection {
        DbConflict,
    }

    // Puts and Gets with our magical fake nexus
    struct NexusServer {
        fake_crdb: FakeCrdb,
        error: Option<NexusErrorInjection>,
    }
    impl FakeNexusServer for NexusServer {
        fn sled_agent_get(
            &self,
            sled_id: Uuid,
        ) -> Result<SledAgentInfo, Error> {
            self.fake_crdb.info.lock().unwrap().clone().ok_or(
                Error::ObjectNotFound {
                    type_name: ResourceType::Sled,
                    lookup_type: LookupType::ById(sled_id),
                },
            )
        }

        fn sled_agent_put(
            &self,
            _sled_id: Uuid,
            info: SledAgentInfo,
        ) -> Result<(), Error> {
            match self.error {
                None => {
                    let mut crdb_info = self.fake_crdb.info.lock().unwrap();
                    *crdb_info = Some(info);
                    Ok(())
                }
                Some(NexusErrorInjection::DbConflict) => Err(Error::Conflict {
                    message: MessagePair::new(
                        "I don't like the cut of your jib".into(),
                    ),
                }),
            }
        }
    }

    #[tokio::test]
    async fn nexus_self_notification_test() {
        let logctx = test_setup_log("nexus_notification_test");
        let log = &logctx.log;
        let sa_address = "::1".to_string();
        let fake_crdb = FakeCrdb::default();
        let sled_id = Uuid::new_v4();

        let nexus_server = crate::fakes::nexus::start_test_server(
            log.clone(),
            Box::new(NexusServer { fake_crdb: fake_crdb.clone(), error: None }),
        );
        let nexus_client = NexusClient::new(
            &format!("http://{}", nexus_server.local_addr()),
            log.clone(),
        );

        // Pretend we are retrieving the latest `SledAgentInfo` from hardware and
        // the VMM reservoir.
        let latest_sled_agent_info =
            Arc::new(std::sync::Mutex::new(SledAgentInfo {
                sa_address: sa_address.clone(),
                role: nexus_client::types::SledRole::Gimlet,
                baseboard: Baseboard::new_pc("test".into(), "test".into())
                    .convert(),
                usable_hardware_threads: 16,
                usable_physical_ram: ByteCount::from(1024 * 1024 * 1024u32)
                    .into(),
                reservoir_size: ByteCount::from(0u32).into(),
                generation: Generation::new(),
            }));
        let latest_sled_agent_info2 = latest_sled_agent_info.clone();

        // Return `SledAgentInfo` from our test object
        let get_sled_agent_info: GetSledAgentInfo =
            Box::new(move |generation| {
                let mut info = latest_sled_agent_info2.lock().unwrap().clone();
                info.generation = generation;
                info
            });

        let (nexus_notifier_task, handle) = NexusNotifierTask::new_for_test(
            sled_id,
            nexus_client,
            get_sled_agent_info,
            log,
        );

        tokio::spawn(async move {
            nexus_notifier_task.run().await;
        });

        // Ensure that the task will try to initially talk to Nexus and get back
        // some `SledAgentInfo` or a `NotFound`, depending upon timing.
        let status = wait_for::<_, (), _, _>(
            || async {
                let status = handle.get_status().await.unwrap();
                if status.nexus_known_info.is_some() {
                    Ok(status)
                } else {
                    Err(CondCheckError::NotYet)
                }
            },
            &Duration::from_millis(2),
            &Duration::from_secs(15),
        )
        .await
        .expect("Failed to get status from Nexus");

        if status.total_put_requests_completed == 0 {
            assert_eq!(status.nexus_known_info, Some(NexusKnownInfo::NotFound));
        }

        // Wait for a steady state, when the the latest info has been put to nexus
        let status = wait_for::<_, (), _, _>(
            || async {
                let status = handle.get_status().await.unwrap();
                if !status.has_pending_notification {
                    Ok(status)
                } else {
                    Err(CondCheckError::NotYet)
                }
            },
            &Duration::from_millis(2),
            &Duration::from_secs(15),
        )
        .await
        .expect("Failed to get status from Nexus");

        assert_eq!(status.total_get_requests_started, 1u64);
        assert_eq!(status.total_put_requests_started, 1u64);
        assert_eq!(status.total_get_requests_completed, 1u64);
        assert_eq!(status.total_put_requests_completed, 1u64);
        assert_eq!(status.has_outstanding_request, false);
        assert_eq!(status.cancelled_pending_notifications, 1);
        let expected = latest_sled_agent_info.lock().unwrap().clone();
        assert_eq!(
            status.nexus_known_info,
            Some(NexusKnownInfo::Found(expected)),
        );

        // Trigger another update notification
        //
        // We haven't changed the underlying `SledAgentInfo` so this request
        // should be cancelled without a request going to nexus.
        handle.notify_nexus_about_self(log).await;
        let status = wait_for::<_, (), _, _>(
            || async {
                let status = handle.get_status().await.unwrap();
                if status.cancelled_pending_notifications == 2 {
                    Ok(status)
                } else {
                    Err(CondCheckError::NotYet)
                }
            },
            &Duration::from_millis(2),
            &Duration::from_secs(15),
        )
        .await
        .expect("Failed to get status from Nexus");
        assert_eq!(status.total_get_requests_started, 1u64);
        assert_eq!(status.total_put_requests_started, 1u64);
        assert_eq!(status.total_get_requests_completed, 1u64);
        assert_eq!(status.total_put_requests_completed, 1u64);
        assert_eq!(status.has_outstanding_request, false);
        let expected = latest_sled_agent_info.lock().unwrap().clone();
        assert_eq!(
            status.nexus_known_info,
            Some(NexusKnownInfo::Found(expected)),
        );

        logctx.cleanup_successful();
    }
}
