// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::HostFlashHashPolicy;
use crate::Responsiveness;
use crate::SimulatedSp;
use crate::config::PscConfig;
use crate::config::SpComponentConfig;
use crate::device_descriptions::DeviceDescriptions;
use crate::ereport::EreportState;
use crate::helpers::read_dummy_rot_page;
use crate::helpers::rot_boot_info;
use crate::helpers::rot_state_v2;
use crate::sensors::Sensors;
use crate::server::SimSpHandler;
use crate::server::UdpServer;
use crate::sp;
use crate::task_dumps::TaskDumps;
use crate::update::BaseboardKind;
use crate::update::SimSpUpdate;
use crate::vpd::BaseboardVpd;
use crate::vpd::ComponentVpds;
use anyhow::Result;
use async_trait::async_trait;
use gateway_messages::ComponentAction;
use gateway_messages::ComponentActionResponse;
use gateway_messages::ComponentDetails;
use gateway_messages::DiscoverResponse;
use gateway_messages::DumpSegment;
use gateway_messages::DumpTask;
use gateway_messages::HostBootfailPayloadData;
use gateway_messages::HostInfoRequest;
use gateway_messages::HostPanicPayloadData;
use gateway_messages::IgnitionState;
use gateway_messages::MgsError;
use gateway_messages::MgsRequest;
use gateway_messages::MgsResponse;
use gateway_messages::PmbusStatus;
use gateway_messages::PowerRailName;
use gateway_messages::PowerState;
use gateway_messages::PowerStateTransition;
use gateway_messages::PowerStateWithReason;
use gateway_messages::RotBootInfo;
use gateway_messages::RotRequest;
use gateway_messages::RotResponse;
use gateway_messages::SpComponent;
use gateway_messages::SpError;
use gateway_messages::SpPort;
use gateway_messages::SpStateV2;
use gateway_messages::StartupOptions;
use gateway_messages::StateChangeReason;
use gateway_messages::ignition;
use gateway_messages::ignition::LinkEvents;
use gateway_messages::sp_impl::BoundsChecked;
use gateway_messages::sp_impl::DeviceDescription;
use gateway_messages::sp_impl::Sender;
use gateway_messages::sp_impl::SpHandler;
use gateway_types::component::SpState;
use slog::Logger;
use slog::debug;
use slog::info;
use slog::warn;
use std::iter;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;
use tokio::sync::watch;

pub const SIM_PSC_BOARD: &str = "SimPscSp";

/// Baseboard model reported by simulated PSCs whose config does not set a
/// part number.
pub const FAKE_PSC_MODEL: &str = "FAKE_SIM_PSC";

pub struct Psc {
    sp: sp::Handle<Handler>,
}

#[async_trait]
impl SimulatedSp for Psc {
    async fn state(&self) -> SpState {
        SpState::from(self.sp.handler().await.unwrap().sp_state_impl())
    }

    fn local_addr(&self, port: SpPort) -> Option<SocketAddrV6> {
        self.sp.local_addr(port)
    }

    fn local_ereport_addr(&self, port: SpPort) -> Option<SocketAddrV6> {
        self.sp.local_ereport_addr(port)
    }

    async fn set_responsiveness(&self, r: Responsiveness) {
        self.sp.set_responsiveness(r).await
    }

    async fn last_sp_update_data(&self) -> Option<Box<[u8]>> {
        self.sp.handler().await?.update_state.last_sp_update_data()
    }

    async fn last_rot_update_data(&self) -> Option<Box<[u8]>> {
        self.sp.handler().await?.update_state.last_rot_update_data()
    }

    async fn host_phase1_data(&self, _slot: u16) -> Option<Vec<u8>> {
        // PSCs do not have attached hosts
        None
    }

    async fn current_update_status(&self) -> gateway_messages::UpdateStatus {
        let Some(handler) = self.sp.handler().await else {
            return gateway_messages::UpdateStatus::None;
        };

        handler.update_state.status()
    }

    fn power_state_changes(&self) -> usize {
        self.sp.power_state_changes()
    }

    fn responses_sent_count(&self) -> Option<watch::Receiver<usize>> {
        self.sp.responses_sent_count()
    }

    async fn install_udp_accept_semaphore(
        &self,
    ) -> mpsc::UnboundedSender<usize> {
        self.sp.install_udp_accept_semaphore().await
    }

    async fn ereport_restart(&self, restart: crate::config::EreportRestart) {
        self.sp.ereport_restart(restart).await
    }

    async fn ereport_append(
        &self,
        ereport: crate::config::Ereport,
    ) -> gateway_ereport_messages::Ena {
        self.sp.ereport_append(ereport).await
    }
}

impl Psc {
    pub async fn spawn(psc: &PscConfig, log: Logger) -> Result<Self> {
        info!(log, "setting up simulated PSC");

        let baseboard_vpd =
            BaseboardVpd::from_config(&psc.common, FAKE_PSC_MODEL)?;
        if let Some(network_config) = &psc.common.network_config {
            // bind to our two local "KSZ" ports
            let servers = UdpServer::bind_pair(network_config, &log).await?;

            let ereport_log = log.new(slog::o!("component" => "ereport-sim"));
            let ereport_servers = match &psc.common.ereport_network_config {
                Some(cfg) => {
                    Some(UdpServer::bind_pair(cfg, &ereport_log).await?)
                }
                None => None,
            };

            let update_state = SimSpUpdate::new(
                BaseboardKind::Psc,
                psc.common.no_stage0_caboose,
                // PSC doesn't have phase 1 flash; any policy is fine
                HostFlashHashPolicy::assume_already_hashed(),
                psc.common.cabooses.clone(),
            );

            let ereport_state = {
                let cfg = psc.common.ereport_config.clone();
                EreportState::new(
                    cfg,
                    &baseboard_vpd,
                    &update_state,
                    ereport_log,
                )
            };

            let handler = Handler::new(
                baseboard_vpd,
                psc.common.components.clone(),
                log,
                psc.common.old_rot_state,
                update_state,
            );
            let sp = sp::Handle::spawn(
                servers,
                ereport_servers,
                ereport_state,
                handler,
            );

            Ok(Self { sp })
        } else {
            Ok(Self { sp: sp::Handle::rot_only() })
        }
    }
}

struct Handler {
    log: Logger,
    device_descriptions: DeviceDescriptions,
    sensors: Sensors,
    component_vpds: ComponentVpds,

    baseboard_vpd: BaseboardVpd,
    power_state_changes: Arc<AtomicUsize>,

    update_state: SimSpUpdate,
    reset_pending: Option<SpComponent>,

    // To simulate an SP reset, we should (after doing whatever housekeeping we
    // need to track the reset) intentionally _fail_ to respond to the request,
    // simulating a `-> !` function on the SP that triggers a reset. To provide
    // this, our caller will pass us a function to call if they should ignore
    // whatever result we return and fail to respond at all.
    should_fail_to_respond_signal: Option<Box<dyn FnOnce() + Send>>,
    old_rot_state: bool,
    task_dumps: TaskDumps,
}

impl Handler {
    fn new(
        baseboard_vpd: BaseboardVpd,
        components: Vec<SpComponentConfig>,
        log: Logger,
        old_rot_state: bool,
        update_state: SimSpUpdate,
    ) -> Self {
        let device_descriptions =
            DeviceDescriptions::from_component_configs(&components);
        let sensors = Sensors::from_component_configs(&components);
        let component_vpds = ComponentVpds::from_component_configs(&components)
            .expect("component VPD configuration should be valid");

        Self {
            log,
            device_descriptions,
            sensors,
            component_vpds,
            baseboard_vpd,
            power_state_changes: Arc::new(AtomicUsize::new(0)),
            update_state,
            reset_pending: None,
            should_fail_to_respond_signal: None,
            old_rot_state,
            task_dumps: TaskDumps::default(),
        }
    }

    fn sp_state_impl(&self) -> SpStateV2 {
        SpStateV2 {
            hubris_archive_id: [0; 8],
            serial_number: self.baseboard_vpd.padded_serial_number(),
            model: self.baseboard_vpd.padded_part_number(),
            revision: 0,
            base_mac_address: [0; 6],
            // PSC is always in A2.
            power_state: PowerState::A2,
            rot: Ok(rot_state_v2(self.update_state.rot_state())),
        }
    }
}

impl SpHandler for Handler {
    type BulkIgnitionStateIter = iter::Empty<IgnitionState>;
    type BulkIgnitionLinkEventsIter = iter::Empty<LinkEvents>;
    type VLanId = SpPort;

    fn ensure_request_trusted(
        &mut self,
        kind: MgsRequest,
        _sender: Sender<Self::VLanId>,
    ) -> Result<MgsRequest, SpError> {
        Ok(kind)
    }

    fn ensure_response_trusted(
        &mut self,
        kind: MgsResponse,
        _sender: Sender<Self::VLanId>,
    ) -> Option<MgsResponse> {
        Some(kind)
    }

    fn discover(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<gateway_messages::DiscoverResponse, SpError> {
        debug!(
            &self.log,
            "received discover; sending response";
            "sender" => ?sender,
        );
        Ok(DiscoverResponse { sp_port: sender.vid })
    }

    fn num_ignition_ports(&mut self) -> Result<u32, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_state(
        &mut self,
        target: u8,
    ) -> Result<gateway_messages::IgnitionState, SpError> {
        warn!(
            &self.log,
            "received ignition state request; not supported by PSC";
            "target" => target,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_state(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionStateIter, SpError> {
        warn!(
            &self.log,
            "received bulk ignition state request; not supported by PSC";
            "offset" => offset,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_link_events(
        &mut self,
        target: u8,
    ) -> Result<LinkEvents, SpError> {
        warn!(
            &self.log,
            "received ignition link events request; not supported by PSC";
            "target" => target,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn bulk_ignition_link_events(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionLinkEventsIter, SpError> {
        warn!(
            &self.log,
            "received bulk ignition link events request; not supported by PSC";
            "offset" => offset,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    /// If `target` is `None`, clear link events for all targets.
    fn clear_ignition_link_events(
        &mut self,
        target: Option<u8>,
        transceiver_select: Option<ignition::TransceiverSelect>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received clear ignition link events request; not supported by PSC";
            "target" => ?target,
            "transceiver_select" => ?transceiver_select,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: gateway_messages::IgnitionCommand,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received ignition command; not supported by PSC";
            "target" => target,
            "command" => ?command,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_attach(
        &mut self,
        sender: Sender<Self::VLanId>,
        _component: SpComponent,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console attach; unsupported by PSC";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_write(
        &mut self,
        sender: Sender<Self::VLanId>,
        _offset: u64,
        _data: &[u8],
    ) -> Result<u64, SpError> {
        warn!(
            &self.log,
            "received serial console write; unsupported by PSC";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_keepalive(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console keepalive; unsupported by PSC";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_detach(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console detach; unsupported by PSC";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_break(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received serial console break; not supported by PSC";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn send_host_nmi(&mut self) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received host NMI request; not supported by PSC";
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn sp_state(&mut self) -> Result<SpStateV2, SpError> {
        let state = self.sp_state_impl();
        debug!(
            &self.log,
            "received state request";
            "reply-state" => ?state,
        );
        Ok(state)
    }

    fn sp_update_prepare(
        &mut self,
        update: gateway_messages::SpUpdatePrepare,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update prepare request";
            "update" => ?update,
        );
        self.update_state.sp_update_prepare(
            update.id,
            update.sp_image_size.try_into().unwrap(),
        )
    }

    fn component_update_prepare(
        &mut self,
        update: gateway_messages::ComponentUpdatePrepare,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update prepare request";
            "update" => ?update,
        );
        self.update_state.component_update_prepare(
            update.component,
            update.id,
            update.total_size.try_into().unwrap(),
            update.slot,
        )
    }

    fn update_status(
        &mut self,
        component: SpComponent,
    ) -> Result<gateway_messages::UpdateStatus, SpError> {
        debug!(
            &self.log,
            "received update status request";
            "component" => ?component,
        );
        Ok(self.update_state.status())
    }

    fn update_chunk(
        &mut self,
        chunk: gateway_messages::UpdateChunk,
        chunk_data: &[u8],
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update chunk";
            "offset" => chunk.offset,
            "length" => chunk_data.len(),
        );
        self.update_state.ingest_chunk(chunk, chunk_data)
    }

    fn update_abort(
        &mut self,
        component: SpComponent,
        update_id: gateway_messages::UpdateId,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received update abort";
            "component" => ?component,
            "id" => ?update_id,
        );
        self.update_state.abort(update_id)
    }

    fn power_state(&mut self) -> Result<PowerState, SpError> {
        // PSCs are always in A2.
        let power_state = PowerState::A2;
        debug!(
            &self.log,
            "received power state";
            "power_state" => ?power_state,
        );
        Ok(power_state)
    }

    fn power_state_with_reason(
        &mut self,
    ) -> Result<PowerStateWithReason, SpError> {
        let power_state = self.power_state()?;

        debug!(
            &self.log,
            "received power state with reason";
            "power_state" => ?power_state,
        );

        Ok(PowerStateWithReason {
            state: power_state,
            reason: StateChangeReason::InitialPowerOn,
            since: 1,
        })
    }

    fn set_power_state(
        &mut self,
        sender: Sender<Self::VLanId>,
        power_state: PowerState,
    ) -> Result<PowerStateTransition, SpError> {
        warn!(
            &self.log,
            "received set power state request; not supported by PSC";
            "power_state" => ?power_state,
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn reset_component_prepare(
        &mut self,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received reset prepare request";
            "component" => ?component,
        );
        if component == SpComponent::SP_ITSELF || component == SpComponent::ROT
        {
            self.reset_pending = Some(component);
            Ok(())
        } else {
            Err(SpError::RequestUnsupportedForComponent)
        }
    }

    fn reset_component_trigger(
        &mut self,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received reset trigger request";
            "component" => ?component,
        );
        if component == SpComponent::SP_ITSELF {
            if self.reset_pending == Some(SpComponent::SP_ITSELF) {
                self.update_state.sp_reset();
                self.reset_pending = None;
                if let Some(signal) = self.should_fail_to_respond_signal.take()
                {
                    // Instruct `server::handle_request()` to _not_ respond to
                    // this request at all, simulating an SP actually resetting.
                    signal();
                }
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else if component == SpComponent::ROT {
            if self.reset_pending == Some(SpComponent::ROT) {
                self.update_state.rot_reset();
                self.reset_pending = None;
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else {
            Err(SpError::RequestUnsupportedForComponent)
        }
    }

    fn num_devices(&mut self) -> u32 {
        self.device_descriptions.num_devices()
    }

    fn device_description(
        &mut self,
        index: BoundsChecked,
    ) -> DeviceDescription<'static> {
        self.device_descriptions.device_description(index)
    }

    fn num_component_details(
        &mut self,
        component: SpComponent,
    ) -> Result<u32, SpError> {
        let num_sensor_details =
            self.sensors.num_component_details(&component).unwrap_or(0);
        debug!(
            &self.log,
            "asked for number of component details";
            "component" => ?component,
            "num_details" => num_sensor_details
        );
        Ok(num_sensor_details)
    }

    fn component_details(
        &mut self,
        component: SpComponent,
        index: BoundsChecked,
    ) -> ComponentDetails {
        let Some(sensor_details) =
            self.sensors.component_details(&component, index)
        else {
            unreachable!("all PSC component details are sensors");
        };
        debug!(
            &self.log, "asked for component details for a sensor";
            "component" => ?component,
            "index" => index.0,
            "details" => ?sensor_details
        );
        sensor_details
    }

    fn component_clear_status(
        &mut self,
        component: SpComponent,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "asked to clear status (not supported for sim components)";
            "component" => ?component,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn component_get_active_slot(
        &mut self,
        component: SpComponent,
    ) -> Result<u16, SpError> {
        debug!(
            &self.log,
            "asked for component active slot";
            "component" => ?component,
        );
        self.update_state.component_get_active_slot(component)
    }

    fn component_set_active_slot(
        &mut self,
        component: SpComponent,
        slot: u16,
        persist: bool,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "asked to set component active slot";
            "component" => ?component,
            "slot" => slot,
            "persist" => persist,
        );
        self.update_state.component_set_active_slot(component, slot, persist)
    }

    fn component_get_persistent_slot(
        &mut self,
        component: SpComponent,
    ) -> std::result::Result<u16, SpError> {
        debug!(
            &self.log,
            "asked for component persistent slot";
            "component" => ?component,
        );
        self.update_state.component_get_persistent_slot(component)
    }

    fn component_action(
        &mut self,
        sender: Sender<Self::VLanId>,
        component: SpComponent,
        action: ComponentAction,
    ) -> Result<ComponentActionResponse, SpError> {
        warn!(
            &self.log,
            "asked to perform component action (not supported for sim components)";
            "sender" => ?sender,
            "component" => ?component,
            "action" => ?action,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn get_startup_options(&mut self) -> Result<StartupOptions, SpError> {
        warn!(
            &self.log,
            "asked for startup options (unsupported by PSC)";
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn set_startup_options(
        &mut self,
        startup_options: StartupOptions,
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "asked to set startup options (unsupported by PSC)";
            "options" => ?startup_options,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn mgs_response_error(&mut self, message_id: u32, err: MgsError) {
        warn!(
            &self.log,
            "received MGS error response";
            "message_id" => message_id,
            "err" => ?err,
        );
    }

    fn mgs_response_host_phase2_data(
        &mut self,
        sender: Sender<Self::VLanId>,
        message_id: u32,
        hash: [u8; 32],
        offset: u64,
        data: &[u8],
    ) {
        debug!(
            &self.log,
            "received host phase 2 data from MGS (not supported by PSC)";
            "sender" => ?sender,
            "message_id" => message_id,
            "hash" => ?hash,
            "offset" => offset,
            "data_len" => data.len(),
        );
    }

    fn set_ipcc_key_lookup_value(
        &mut self,
        key: u8,
        value: &[u8],
    ) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received IPCC key/value; not supported by PSC";
            "key" => key,
            "value" => ?value,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn get_component_caboose_value(
        &mut self,
        component: SpComponent,
        slot: u16,
        key: [u8; 4],
        buf: &mut [u8],
    ) -> std::result::Result<usize, SpError> {
        self.update_state.get_component_caboose_value(component, slot, key, buf)
    }

    fn component_get_vpd(
        &mut self,
        component: SpComponent,
        buf: &mut [u8],
    ) -> Result<usize, SpError> {
        self.component_vpds.component_get_vpd(&component, buf)
    }

    fn read_sensor(
        &mut self,
        request: gateway_messages::SensorRequest,
    ) -> std::result::Result<gateway_messages::SensorResponse, SpError> {
        self.sensors.read_sensor(request).map_err(SpError::Sensor)
    }

    fn current_time(&mut self) -> std::result::Result<u64, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn read_rot(
        &mut self,
        request: RotRequest,
        buf: &mut [u8],
    ) -> std::result::Result<RotResponse, SpError> {
        read_dummy_rot_page(BaseboardKind::Psc, request, buf)
    }

    fn vpd_lock_status_all(
        &mut self,
        _buf: &mut [u8],
    ) -> Result<usize, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn reset_component_trigger_with_watchdog(
        &mut self,
        component: SpComponent,
        _time_ms: u32,
    ) -> Result<(), SpError> {
        debug!(
            &self.log,
            "received reset trigger with watchdog request";
            "component" => ?component,
        );
        if component == SpComponent::SP_ITSELF {
            if self.reset_pending == Some(SpComponent::SP_ITSELF) {
                self.update_state.sp_reset();
                self.reset_pending = None;
                if let Some(signal) = self.should_fail_to_respond_signal.take()
                {
                    // Instruct `server::handle_request()` to _not_ respond to
                    // this request at all, simulating an SP actually resetting.
                    signal();
                }
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else if component == SpComponent::ROT {
            if self.reset_pending == Some(SpComponent::ROT) {
                self.update_state.rot_reset();
                self.reset_pending = None;
                Ok(())
            } else {
                Err(SpError::ResetComponentTriggerWithoutPrepare)
            }
        } else {
            Err(SpError::RequestUnsupportedForComponent)
        }
    }

    fn disable_component_watchdog(
        &mut self,
        _component: SpComponent,
    ) -> Result<(), SpError> {
        Ok(())
    }
    fn component_watchdog_supported(
        &mut self,
        _component: SpComponent,
    ) -> Result<(), SpError> {
        Ok(())
    }

    fn versioned_rot_boot_info(
        &mut self,
        version: u8,
    ) -> Result<RotBootInfo, SpError> {
        rot_boot_info(
            self.update_state.rot_state(),
            self.old_rot_state,
            version,
        )
    }

    fn get_task_dump_count(&mut self) -> Result<u32, SpError> {
        self.task_dumps.get_task_dump_count()
    }

    fn task_dump_read_start(
        &mut self,
        index: u32,
        key: [u8; 16],
    ) -> Result<DumpTask, SpError> {
        self.task_dumps.task_dump_read_start(index, key)
    }

    fn task_dump_read_continue(
        &mut self,
        key: [u8; 16],
        seq: u32,
        buf: &mut [u8],
    ) -> Result<Option<DumpSegment>, SpError> {
        self.task_dumps.task_dump_read_continue(key, seq, buf)
    }

    fn read_host_flash(
        &mut self,
        _slot: u16,
        _addr: u32,
        _buf: &mut [u8],
    ) -> Result<(), SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn start_host_flash_hash(&mut self, _slot: u16) -> Result<(), SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn get_host_flash_hash(&mut self, _slot: u16) -> Result<[u8; 32], SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn get_pmbus_status(
        &mut self,
        _rail: &PowerRailName,
    ) -> Result<PmbusStatus, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn get_host_panic_payload(
        &mut self,
        _request: Option<HostInfoRequest>,
        _len: u32,
        _trailing_tx_buf: &mut [u8],
    ) -> Result<HostPanicPayloadData, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }

    fn get_host_bootfail_payload(
        &mut self,
        _request: Option<HostInfoRequest>,
        _len: u32,
        _trailing_tx_buf: &mut [u8],
    ) -> Result<HostBootfailPayloadData, SpError> {
        Err(SpError::RequestUnsupportedForSp)
    }
}

impl SimSpHandler for Handler {
    fn set_sp_should_fail_to_respond_signal(
        &mut self,
        signal: Box<dyn FnOnce() + Send>,
    ) {
        self.should_fail_to_respond_signal = Some(signal);
    }

    fn power_state_changes(&self) -> &Arc<AtomicUsize> {
        &self.power_state_changes
    }
}
