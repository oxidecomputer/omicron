// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::Generation;
use omicron_common::disk::DiskVariant;
use omicron_uuid_kinds::SledUuid;

use crate::vmm_reservoir::VmmReservoirManagerHandle;
use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_client::types::SledAgentInfo;
use omicron_common::address::NEXUS_INTERNAL_PORT;
use sled_hardware::HardwareManager;
use slog::Logger;
use std::net::SocketAddrV6;
use std::sync::Arc;
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio::time::{Duration, MissedTickBehavior, interval};

// Re-export the nexus_client::Client crate. (Use a type alias to be more
// rust-analyzer friendly.)
pub(crate) type NexusClient = nexus_client::Client;

pub(crate) fn make_nexus_client(
    log: &Logger,
    resolver: Arc<Resolver>,
) -> NexusClient {
    make_nexus_client_with_port(log, resolver, NEXUS_INTERNAL_PORT)
}

pub(crate) fn make_nexus_client_with_port(
    log: &Logger,
    resolver: Arc<Resolver>,
    port: u16,
) -> NexusClient {
    let client = reqwest::ClientBuilder::new()
        .dns_resolver(resolver)
        .build()
        .expect("Failed to build client");

    let dns_name = ServiceName::Nexus.srv_name();
    NexusClient::new_with_client(
        &format!("http://{dns_name}:{port}"),
        client,
        log.new(o!("component" => "NexusClient")),
    )
}

// Although it is a bit awkward to define these conversions here, it frees us
// from depending on sled_storage/sled_hardware in the nexus_client crate.

pub(crate) trait ConvertInto<T>: Sized {
    fn convert(self) -> T;
}

impl ConvertInto<nexus_client::types::PhysicalDiskKind> for DiskVariant {
    fn convert(self) -> nexus_client::types::PhysicalDiskKind {
        use nexus_client::types::PhysicalDiskKind;

        match self {
            DiskVariant::U2 => PhysicalDiskKind::U2,
            DiskVariant::M2 => PhysicalDiskKind::M2,
        }
    }
}

impl ConvertInto<nexus_client::types::Baseboard>
    for sled_hardware_types::Baseboard
{
    fn convert(self) -> nexus_client::types::Baseboard {
        nexus_client::types::Baseboard {
            serial: self.identifier().to_string(),
            part: self.model().to_string(),
            revision: self.revision(),
        }
    }
}

impl ConvertInto<nexus_client::types::SledCpuFamily>
    for omicron_common::api::internal::shared::SledCpuFamily
{
    fn convert(self) -> nexus_client::types::SledCpuFamily {
        use omicron_common::api::internal::shared::SledCpuFamily as SharedSledCpuFamily;
        match self {
            SharedSledCpuFamily::Unknown => {
                nexus_client::types::SledCpuFamily::Unknown
            }
            SharedSledCpuFamily::AmdMilan => {
                nexus_client::types::SledCpuFamily::AmdMilan
            }
            SharedSledCpuFamily::AmdTurin => {
                nexus_client::types::SledCpuFamily::AmdTurin
            }
            SharedSledCpuFamily::AmdTurinDense => {
                nexus_client::types::SledCpuFamily::AmdTurinDense
            }
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
#[allow(unused)]
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

    #[allow(unused)]
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

// The type of operation issued to nexus
enum NexusOp {
    Get,
    Put,
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
    pub sled_id: SledUuid,
    pub sled_address: SocketAddrV6,
    pub repo_depot_port: u16,
    pub nexus_client: NexusClient,
    pub hardware: HardwareManager,
    pub vmm_reservoir_manager: VmmReservoirManagerHandle,
}

// Type returned from a join on the "outstanding request" task`
type NexusRsp = (
    NexusOp,
    Result<SledAgentInfo, nexus_client::Error<nexus_client::types::Error>>,
);

/// A mechanism for notifying nexus about this sled agent
///
/// The semantics are as follows:
///  1. At any time there is a single outstanding HTTP request to nexus
///  2. On startup, this task gets the latest sled-agent info, if any, from
///     nexus, and saves it.
///  3. Whenever the state needs to be updated to a value different
///     from what nexus has, the generation number is bumped
///     and the new state transmitted.
///  4. If a caller requests an update to be made to nexus and it succeeds
///     the known value is set to what was updated.
///  5. If the request fails, we go ahead and set the known state to `None`.
///     This will trigger a get request for the latest state. In the case of
///     a timeout we are not sure if the last update succeeded and so need to
///     learn that. In the case of an explicit rejection from nexus, we also
///     want to learn the latest state. We need to learn the latest state this
///     because an update can fail if the sled-agent's state is out of date
///     with respect to what's in CRDB. The only way to fix that is to get the
///     latest known state, and it's easiest to just do that unconditionally
///     rather than trying to reason about exactly why Nexus rejected the
///     request.
///  6. An exception to step 5 is that if the latest state returned from Nexus
///     contains `decommissioned = true`, then we stop dead in our tracks and no
///     longer send requests to nexus.
pub struct NexusNotifierTask {
    sled_id: SledUuid,
    nexus_client: NexusClient,
    get_sled_agent_info: GetSledAgentInfo,

    // Notifies when the VMM reservoir size changes.
    //
    // This is an option, since we don't use a reservoir during testing. There
    // really isn't a better place to put this in sled-agent, and this task is
    // the only one that cares about updates.
    vmm_reservoir_size_updated: Option<broadcast::Receiver<()>>,

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
    outstanding_request: Option<tokio::task::JoinHandle<NexusRsp>>,

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
            repo_depot_port,
            nexus_client,
            hardware,
            vmm_reservoir_manager,
        } = input;

        let vmm_reservoir_size_updated =
            Some(vmm_reservoir_manager.subscribe_for_size_updates());

        // Box a function that can return the latest `SledAgentInfo`
        let get_sled_agent_info = Box::new(move |generation| {
            let role = if hardware.is_scrimlet() {
                nexus_client::types::SledRole::Scrimlet
            } else {
                nexus_client::types::SledRole::Gimlet
            };
            SledAgentInfo {
                sa_address: sled_address.to_string(),
                repo_depot_port,
                role,
                baseboard: hardware.baseboard().convert(),
                usable_hardware_threads: hardware.online_processor_count(),
                usable_physical_ram: hardware
                    .usable_physical_ram_bytes()
                    .into(),
                reservoir_size: vmm_reservoir_manager.reservoir_size().into(),
                cpu_family: hardware.cpu_family().convert(),
                generation,
                decommissioned: false,
            }
        });

        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        (
            NexusNotifierTask {
                sled_id,
                nexus_client,
                get_sled_agent_info,
                vmm_reservoir_size_updated,
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

    #[cfg(test)]
    pub fn new_for_test(
        sled_id: SledUuid,
        nexus_client: NexusClient,
        get_sled_agent_info: GetSledAgentInfo,
        log: &Logger,
    ) -> (NexusNotifierTask, NexusNotifierHandle) {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);

        // During testing we don't actually have a reservoir. Just dummy it out.
        let vmm_reservoir_size_updated = None;

        (
            NexusNotifierTask {
                sled_id,
                nexus_client,
                get_sled_agent_info,
                vmm_reservoir_size_updated,
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
        let (_tx, mut vmm_size_updated) = if let Some(vmm_size_updated) =
            self.vmm_reservoir_size_updated.take()
        {
            (None, vmm_size_updated)
        } else {
            // Dummy channel for testing
            let (tx, rx) = broadcast::channel(1);
            (Some(tx), rx)
        };

        const RETRY_TIMEOUT: Duration = Duration::from_secs(2);
        let mut interval = interval(RETRY_TIMEOUT);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
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
                _ = vmm_size_updated.recv() => {
                    self.pending_notification = true;
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
                    // Don't send any more requests if this sled-agent has been decommissioned.
                    if known.decommissioned {
                        return;
                    }

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
                let res =
                    client.sled_agent_put(&sled_id, &info).await.map(|_| info);
                outstanding_req_ready.notify_one();
                (NexusOp::Put, res)
            }));
        } else {
            let outstanding_req_ready = self.outstanding_req_ready.clone();
            self.total_get_requests_started += 1;
            self.outstanding_request = Some(tokio::spawn(async move {
                let res = client
                    .sled_agent_get(&sled_id)
                    .await
                    .map(|info| info.into_inner());
                outstanding_req_ready.notify_one();
                (NexusOp::Get, res)
            }));
        }
    }

    /// Handle a reply from nexus by extracting the value from the `JoinHandle` of
    /// the last outstanding request.
    async fn handle_nexus_reply(&mut self) {
        let (op, res) = match self
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
        match (op, res) {
            (NexusOp::Get, Ok(info)) => {
                info!(
                    self.log,
                    "Retrieved SledAgentInfo from Nexus: {:?}", info
                );
                assert!(self.nexus_known_info.is_none());
                if info.decommissioned {
                    info!(self.log, "Sled Agent Decommissioned.");
                }
                self.nexus_known_info = Some(NexusKnownInfo::Found(info));
                self.total_get_requests_completed += 1;
            }
            (NexusOp::Put, Ok(info)) => {
                // Unwrap Safety: we must have a known value in order to have
                // submitted a PUT request in the first place.
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
            (NexusOp::Get, Err(e)) => {
                self.total_get_requests_completed += 1;
                if e.status() == Some(http::StatusCode::NOT_FOUND) {
                    if self.nexus_known_info.is_none() {
                        self.nexus_known_info = Some(NexusKnownInfo::NotFound);
                        return;
                    }
                    warn!(
                        self.log,
                        "Nexus doesn't have have the latest state of this\
                        sled-agent, but sled-agent thinks it should. Setting \
                        known state to `None` and trying again.",
                    );
                    self.nexus_known_info = None;
                    return;
                }
                self.nexus_known_info = None;
                warn!(
                    self.log,
                    "Received Error from Nexus for Get request: {:?}", e
                );
            }
            (NexusOp::Put, Err(e)) => {
                self.total_put_requests_completed += 1;
                self.nexus_known_info = None;
                warn!(
                    self.log,
                    "Received Error from Nexus for Put request: {:?}", e
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::fakes::nexus::FakeNexusServer;
    use omicron_test_utils::dev::poll::{
        CondCheckError, wait_for_condition as wait_for,
    };
    use omicron_uuid_kinds::GenericUuid;

    use super::*;
    use omicron_common::api::external::{
        ByteCount, Error, Generation, LookupType, MessagePair, ResourceType,
    };
    use omicron_test_utils::dev::test_setup_log;
    use sled_hardware_types::Baseboard;

    /// Pretend to be CRDB storing info about a sled-agent
    #[derive(Default, Clone)]
    struct FakeCrdb {
        info: Arc<std::sync::Mutex<Option<SledAgentInfo>>>,
    }

    // Puts and Gets with our magical fake nexus
    struct NexusServer {
        fake_crdb: FakeCrdb,
        // Injectable errors
        get_error: Arc<AtomicBool>,
        put_error: Arc<AtomicBool>,
    }
    impl FakeNexusServer for NexusServer {
        fn sled_agent_get(
            &self,
            sled_id: SledUuid,
        ) -> Result<SledAgentInfo, Error> {
            // Always disable any errors after the first time. This simplifies
            // testing due to lack of races.
            let injected_err = self.get_error.swap(false, Ordering::SeqCst);
            if injected_err {
                return Err(Error::ServiceUnavailable {
                    internal_message: "go away".into(),
                });
            }

            self.fake_crdb.info.lock().unwrap().clone().ok_or(
                Error::ObjectNotFound {
                    type_name: ResourceType::Sled,
                    lookup_type: LookupType::ById(sled_id.into_untyped_uuid()),
                },
            )
        }

        fn sled_agent_put(
            &self,
            _sled_id: SledUuid,
            info: SledAgentInfo,
        ) -> Result<(), Error> {
            // Always disable any errors after the first time. This simplifies
            // testing due to lack of races.
            let injected_err = self.put_error.swap(false, Ordering::SeqCst);
            if injected_err {
                return Err(Error::Conflict {
                    message: MessagePair::new(
                        "I don't like the cut of your jib".into(),
                    ),
                });
            }

            let mut crdb_info = self.fake_crdb.info.lock().unwrap();
            *crdb_info = Some(info);
            Ok(())
        }
    }

    #[tokio::test]
    async fn nexus_self_notification_test() {
        let logctx = test_setup_log("nexus_notification_test");
        let log = &logctx.log;
        let sa_address = "::1".to_string();
        let fake_crdb = FakeCrdb::default();
        let sled_id = SledUuid::new_v4();
        let get_error = Arc::new(AtomicBool::new(false));
        let put_error = Arc::new(AtomicBool::new(false));

        let nexus_server = crate::fakes::nexus::start_test_server(
            log.clone(),
            Box::new(NexusServer {
                fake_crdb: fake_crdb.clone(),
                get_error: get_error.clone(),
                put_error: put_error.clone(),
            }),
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
                repo_depot_port: 0,
                role: nexus_client::types::SledRole::Gimlet,
                baseboard: Baseboard::new_pc("test".into(), "test".into())
                    .convert(),
                usable_hardware_threads: 16,
                usable_physical_ram: ByteCount::from(1024 * 1024 * 1024u32)
                    .into(),
                reservoir_size: ByteCount::from(0u32).into(),
                cpu_family: nexus_client::types::SledCpuFamily::Unknown,
                generation: Generation::new(),
                decommissioned: false,
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

        // Update the VMM reservoir size and trigger a successful put to Nexus.
        {
            let mut info = latest_sled_agent_info.lock().unwrap();
            info.reservoir_size = (1024 * 1024u64).into();
            info.generation = info.generation.next();
        }

        // Wait for a steady state, when the the latest info has been put to nexus
        handle.notify_nexus_about_self(log).await;
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
        assert_eq!(status.total_get_requests_completed, 1u64);
        assert_eq!(status.total_put_requests_started, 2u64);
        assert_eq!(status.total_put_requests_completed, 2u64);
        assert_eq!(status.has_outstanding_request, false);
        assert_eq!(status.cancelled_pending_notifications, 3);
        let expected = latest_sled_agent_info.lock().unwrap().clone();
        assert_eq!(
            status.nexus_known_info,
            Some(NexusKnownInfo::Found(expected)),
        );

        // Inject a put error and trigger a put to nexus after updating the VMM
        // reservoir size. It should eventually succeed.
        put_error.store(true, Ordering::SeqCst);
        {
            let mut info = latest_sled_agent_info.lock().unwrap();
            info.reservoir_size = (2 * 1024 * 1024u64).into();
            info.generation = info.generation.next();
        }
        handle.notify_nexus_about_self(log).await;
        // Wait for a steady state, when the the latest info has been put to nexus.
        // Ensure the second get request has been sent.
        let status = wait_for::<_, (), _, _>(
            || async {
                let status = handle.get_status().await.unwrap();
                if !status.has_pending_notification
                    && status.total_get_requests_started == 2
                {
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

        // One extra get and put request because of the put error
        assert_eq!(status.total_get_requests_completed, 2u64);

        assert_eq!(status.total_put_requests_started, 4u64);
        assert_eq!(status.total_put_requests_completed, 4u64);
        assert_eq!(status.has_outstanding_request, false);
        assert_eq!(status.cancelled_pending_notifications, 4);
        let expected = latest_sled_agent_info.lock().unwrap().clone();
        assert_eq!(
            status.nexus_known_info,
            Some(NexusKnownInfo::Found(expected)),
        );

        // Inject a get error and trigger a put to nexus after updating the VMM
        // reservoir size. We shouldn't end up calling Get at all since we know
        // our state, and so we will not trigger the error here. However, later
        // on when we trigger a put error, it will perform a get and that will
        // trigger this injected get errror.
        get_error.store(true, Ordering::SeqCst);
        {
            let mut info = latest_sled_agent_info.lock().unwrap();
            info.reservoir_size = (3 * 1024 * 1024u64).into();
            info.generation = info.generation.next();
        }
        handle.notify_nexus_about_self(log).await;
        // Wait for a steady state, when the the latest info has been put to nexus.
        // Ensure the second get request has been sent.
        let status = wait_for::<_, (), _, _>(
            || async {
                let status = handle.get_status().await.unwrap();
                if !status.has_pending_notification
                    && status.total_put_requests_started == 5
                {
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

        // Get wasn't called
        assert_eq!(status.total_get_requests_started, 2u64);
        assert_eq!(status.total_get_requests_completed, 2u64);

        assert_eq!(status.total_put_requests_started, 5u64);
        assert_eq!(status.total_put_requests_completed, 5u64);
        assert_eq!(status.has_outstanding_request, false);
        assert_eq!(status.cancelled_pending_notifications, 5);
        let expected = latest_sled_agent_info.lock().unwrap().clone();
        assert_eq!(
            status.nexus_known_info,
            Some(NexusKnownInfo::Found(expected)),
        );

        // Now inject a put error. This will trigger the put error and
        // previously injected get error.
        put_error.store(true, Ordering::SeqCst);
        {
            let mut info = latest_sled_agent_info.lock().unwrap();
            info.reservoir_size = (4 * 1024 * 1024u64).into();
            info.generation = info.generation.next();
        }
        handle.notify_nexus_about_self(log).await;
        // Wait for a steady state, when the the latest info has been put to nexus.
        // Ensure the second get request has been sent.
        let status = wait_for::<_, (), _, _>(
            || async {
                let status = handle.get_status().await.unwrap();
                if !status.has_pending_notification
                    && status.total_put_requests_started > 5
                {
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

        // Get was called twice, once for error, once for success
        assert_eq!(status.total_get_requests_started, 4u64);
        assert_eq!(status.total_get_requests_completed, 4u64);

        // Put was called twice, once for error, once for success
        assert_eq!(status.total_put_requests_started, 7u64);
        assert_eq!(status.total_put_requests_completed, 7u64);

        assert_eq!(status.has_outstanding_request, false);
        assert_eq!(status.cancelled_pending_notifications, 6);
        let expected = latest_sled_agent_info.lock().unwrap().clone();
        assert_eq!(
            status.nexus_known_info,
            Some(NexusKnownInfo::Found(expected)),
        );

        logctx.cleanup_successful();
    }
}
