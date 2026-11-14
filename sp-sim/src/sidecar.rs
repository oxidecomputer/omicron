// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::HostFlashHashPolicy;
use crate::Responsiveness;
use crate::SimulatedSp;
use crate::config::SidecarConfig;
use crate::config::SimulatedSpsConfig;
use crate::config::SpComponentConfig;
use crate::device_descriptions::DeviceDescriptions;
use crate::ereport::EreportState;
use crate::helpers::read_dummy_rot_page;
use crate::helpers::rot_boot_info;
use crate::helpers::rot_state_v2;
use crate::pmbus_rails::PmbusRails;
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
use gateway_messages::IgnitionCommand;
use gateway_messages::IgnitionState;
use gateway_messages::MgsError;
use gateway_messages::MgsRequest;
use gateway_messages::MgsResponse;
use gateway_messages::PmbusStatus;
use gateway_messages::PowerRailName;
use gateway_messages::PowerState;
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
use gateway_messages::ignition::IgnitionError;
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
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::watch;

pub const SIM_SIDECAR_BOARD: &str = "SimSidecarSp";

/// Baseboard model reported by simulated Sidecars whose config does not set a
/// part number.
pub const FAKE_SIDECAR_MODEL: &str = "FAKE_SIM_SIDECAR";

/// The number of ignition targets sp-sim simulates (32 sleds, 2 PSCs, and the
/// other sidecar).
///
/// Simulated SPs fill the targets in order: sidecars, then gimlets, then PSCs.
/// The total number of simulated SPs can't exceed this.
pub const NUM_IGNITION_TARGETS: usize = 35;

pub struct Sidecar {
    sp: sp::Handle<Handler>,
}

#[async_trait]
impl SimulatedSp for Sidecar {
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
        // sidecars do not have attached hosts
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

impl Sidecar {
    pub(crate) async fn spawn(
        sidecar: &SidecarConfig,
        index: usize,
        rack_ignition_power: RackIgnitionPower,
        ignition_power: watch::Receiver<ignition::SystemPowerState>,
        log: Logger,
    ) -> Result<Self> {
        info!(log, "setting up simulated sidecar");

        let baseboard_vpd =
            BaseboardVpd::from_config(&sidecar.common, FAKE_SIDECAR_MODEL)?;
        if let Some(network_config) = &sidecar.common.network_config {
            // bind to our two local "KSZ" ports
            let servers = UdpServer::bind_pair(network_config, &log).await?;

            let ereport_log = log.new(slog::o!("component" => "ereport-sim"));
            let ereport_servers = match &sidecar.common.ereport_network_config {
                Some(cfg) => {
                    Some(UdpServer::bind_pair(cfg, &ereport_log).await?)
                }
                None => None,
            };

            let update_state = SimSpUpdate::new(
                BaseboardKind::Sidecar,
                sidecar.common.no_stage0_caboose,
                // sidecar doesn't have phase 1 flash; any policy is fine
                HostFlashHashPolicy::assume_already_hashed(),
                sidecar.common.cabooses.clone(),
            );

            let ereport_state = {
                let cfg = sidecar.common.ereport_config.clone();
                EreportState::new(
                    cfg,
                    &baseboard_vpd,
                    &update_state,
                    ereport_log,
                )
            };

            let handler = Handler::new(
                baseboard_vpd,
                sidecar.common.components.clone(),
                FakeIgnition::new(index, rack_ignition_power),
                log,
                sidecar.common.old_rot_state,
                update_state,
            );
            let sp = sp::Handle::spawn(
                servers,
                ereport_servers,
                ereport_state,
                ignition_power,
                handler,
            );

            Ok(Self { sp })
        } else {
            Ok(Self { sp: sp::Handle::rot_only() })
        }
    }

    pub async fn current_ignition_state(&self) -> Vec<IgnitionState> {
        self.sp
            .handler()
            .await
            .expect("no network config provided when constructing sim sidecar")
            .ignition
            .current_state()
    }
}

struct Handler {
    log: Logger,
    device_descriptions: DeviceDescriptions,
    sensors: Sensors,
    component_vpds: ComponentVpds,
    pmbus_rails: PmbusRails,

    baseboard_vpd: BaseboardVpd,
    ignition: FakeIgnition,
    power_state: PowerState,
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
        ignition: FakeIgnition,
        log: Logger,
        old_rot_state: bool,
        update_state: SimSpUpdate,
    ) -> Self {
        let device_descriptions =
            DeviceDescriptions::from_component_configs(&components);
        let sensors = Sensors::from_component_configs(&components);
        let component_vpds = ComponentVpds::from_component_configs(&components)
            .expect("component VPD configuration should be valid");
        let pmbus_rails = match PmbusRails::from_component_configs(&components)
        {
            Ok(rails) => rails,
            Err(e) => {
                panic!("invalid PMBus rail configuration for Sidecar SP: {e}");
            }
        };

        Self {
            log,
            device_descriptions,
            sensors,
            component_vpds,
            baseboard_vpd,
            pmbus_rails,
            ignition,
            power_state: PowerState::A2,
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
            power_state: self.power_state,
            rot: Ok(rot_state_v2(self.update_state.rot_state())),
        }
    }
}

impl SpHandler for Handler {
    type BulkIgnitionStateIter = iter::Skip<std::vec::IntoIter<IgnitionState>>;
    type BulkIgnitionLinkEventsIter =
        iter::Skip<std::vec::IntoIter<LinkEvents>>;
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
        Ok(self.ignition.num_targets() as u32)
    }

    fn ignition_state(&mut self, target: u8) -> Result<IgnitionState, SpError> {
        let state = self.ignition.get_target(target)?;
        debug!(
            &self.log,
            "received ignition state request";
            "target" => target,
            "reply-state" => ?state,
        );
        Ok(state)
    }

    fn bulk_ignition_state(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionStateIter, SpError> {
        let state = self.ignition.current_state();
        debug!(
            &self.log,
            "received bulk ignition state request";
            "offset" => offset,
            "state" => ?state,
        );
        Ok(state.into_iter().skip(offset as usize))
    }

    fn ignition_link_events(
        &mut self,
        target: u8,
    ) -> Result<LinkEvents, SpError> {
        // Check validity of `target`
        _ = self.ignition.get_target(target)?;

        let events = self.ignition.link_events[usize::from(target)];

        debug!(
            &self.log,
            "received ignition link events request";
            "target" => target,
            "events" => ?events,
        );

        Ok(events)
    }

    fn bulk_ignition_link_events(
        &mut self,
        offset: u32,
    ) -> Result<Self::BulkIgnitionLinkEventsIter, SpError> {
        debug!(
            &self.log,
            "received bulk ignition link events request";
            "offset" => offset,
        );
        Ok(self.ignition.link_events.clone().into_iter().skip(offset as usize))
    }

    /// If `target` is `None`, clear link events for all targets.
    fn clear_ignition_link_events(
        &mut self,
        target: Option<u8>,
        transceiver_select: Option<ignition::TransceiverSelect>,
    ) -> Result<(), SpError> {
        let targets = match target {
            Some(t) => {
                // Check validity
                _ = self.ignition.get_target(t)?;
                usize::from(t)..usize::from(t) + 1
            }
            None => 0..self.ignition.num_targets(),
        };

        for t in targets {
            match transceiver_select {
                Some(ignition::TransceiverSelect::Controller) => {
                    self.ignition.link_events[t].controller =
                        empty_transceiver_events();
                }
                Some(ignition::TransceiverSelect::TargetLink0) => {
                    self.ignition.link_events[t].target_link0 =
                        empty_transceiver_events();
                }
                Some(ignition::TransceiverSelect::TargetLink1) => {
                    self.ignition.link_events[t].target_link1 =
                        empty_transceiver_events();
                }
                None => {
                    self.ignition.link_events[t] = empty_link_events();
                }
            }
        }

        debug!(
            &self.log,
            "cleared ignition link events";
            "target" => ?target,
            "transceiver_select" => ?transceiver_select,
        );
        Ok(())
    }

    fn ignition_command(
        &mut self,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), SpError> {
        self.ignition.command(target, command)?;
        debug!(
            &self.log,
            "received ignition command; sending ack";
            "target" => target,
            "command" => ?command,
        );
        Ok(())
    }

    fn serial_console_attach(
        &mut self,
        sender: Sender<Self::VLanId>,
        _component: SpComponent,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received serial console attach; unsupported by sidecar";
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
            &self.log, "received serial console write; unsupported by sidecar";
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
            "received serial console keepalive; unsupported by sidecar";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn serial_console_detach(
        &mut self,
        sender: Sender<Self::VLanId>,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "received serial console detach; unsupported by sidecar";
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
            "received serial console break; not supported by sidecar";
            "sender" => ?sender,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn send_host_nmi(&mut self) -> Result<(), SpError> {
        warn!(
            &self.log,
            "received host NMI request; not supported by sidecar";
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn sp_state(&mut self) -> Result<SpStateV2, SpError> {
        let state = self.sp_state_impl();
        debug!(
            &self.log, "received state request";
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
            "received update abort; not supported by simulated sidecar";
            "component" => ?component,
            "id" => ?update_id,
        );
        self.update_state.abort(update_id)
    }

    fn power_state(&mut self) -> Result<gateway_messages::PowerState, SpError> {
        debug!(
            &self.log, "received power state";
            "power_state" => ?self.power_state,
        );
        Ok(self.power_state)
    }

    fn power_state_with_reason(
        &mut self,
    ) -> Result<PowerStateWithReason, SpError> {
        let power_state = self.power_state()?;

        debug!(
            &self.log, "received power state with reason";
            "power_state" => ?power_state,
        );

        Ok(PowerStateWithReason {
            state: power_state,
            reason: StateChangeReason::Other,
            since: 1,
        })
    }

    fn set_power_state(
        &mut self,
        sender: Sender<Self::VLanId>,
        power_state: gateway_messages::PowerState,
    ) -> Result<gateway_messages::PowerStateTransition, SpError> {
        // NOTE(eliza): This is *currently* accurate to real life sidecar
        // behavior, as the sidecar sequencer does not treat `set_power_state`
        // calls with the current power state idempotently, the way the compute
        // sled sequencer does.
        // See: https://github.com/oxidecomputer/hubris/blob/13808140c49fdf8f1ce462184395d3b28212c217/task/control-plane-agent/src/mgs_sidecar.rs#L838-L840
        let transition = gateway_messages::PowerStateTransition::Changed;
        debug!(
            &self.log, "received set power state";
            "sender" => ?sender,
            "power_state" => ?power_state,
            "transition" => ?transition,
        );
        self.power_state = power_state;
        match transition {
            gateway_messages::PowerStateTransition::Changed => {
                self.power_state_changes.fetch_add(1, Ordering::Relaxed);
            }
            gateway_messages::PowerStateTransition::Unchanged => (),
        }
        Ok(transition)
    }

    fn reset_component_prepare(
        &mut self,
        component: SpComponent,
    ) -> Result<(), SpError> {
        debug!(
            &self.log, "received reset prepare request";
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
            &self.log, "received sys-reset trigger request";
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
        // TODO: here is where we might also handle port statuses, if we decide
        // to simulate that later...
        debug!(
            &self.log, "asked for number of component details";
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
            todo!("simulate port status details...");
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
            &self.log, "asked to clear status (not supported for sim components)";
            "component" => ?component,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn component_get_active_slot(
        &mut self,
        component: SpComponent,
    ) -> Result<u16, SpError> {
        warn!(
            &self.log, "asked for component active slot";
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
        warn!(
            &self.log, "asked to set component active slot";
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
            &self.log, "asked for component persistent slot";
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
            &self.log, "asked to perform component action (not supported for sim components)";
            "sender" => ?sender,
            "component" => ?component,
            "action" => ?action,
        );
        Err(SpError::RequestUnsupportedForComponent)
    }

    fn get_startup_options(&mut self) -> Result<StartupOptions, SpError> {
        warn!(
            &self.log, "asked for startup options (unsupported by sidecar)";
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn set_startup_options(
        &mut self,
        startup_options: StartupOptions,
    ) -> Result<(), SpError> {
        warn!(
            &self.log, "asked to set startup options (unsupported by sidecar)";
            "options" => ?startup_options,
        );
        Err(SpError::RequestUnsupportedForSp)
    }

    fn mgs_response_error(&mut self, message_id: u32, err: MgsError) {
        warn!(
            &self.log, "received MGS error response";
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
            &self.log, "received host phase 2 data from MGS";
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
            "received IPCC key/value; not supported by sidecar";
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
        read_dummy_rot_page(BaseboardKind::Sidecar, request, buf)
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
            &self.log, "received sys-reset trigger with wathcdog request";
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
        rail: &PowerRailName,
    ) -> Result<PmbusStatus, SpError> {
        self.pmbus_rails.pmbus_status(rail)
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

// TODO-RAINCLAUDE: the ignition power of each simulated SP's target, shared by every sidecar in the rack so both switches see the same power.
#[derive(Clone)]
pub(crate) struct RackIgnitionPower {
    num_sidecars: usize,
    targets: Arc<[RackIgnitionTarget]>,
}

struct RackIgnitionTarget {
    system_type: ignition::SystemType,
    power: watch::Sender<ignition::SystemPowerState>,
}

impl RackIgnitionPower {
    // TODO-RAINCLAUDE: the one place that numbers rack targets: sidecars, then gimlets, then PSCs, in config order; SimRack::start hands out subscribe_all()'s receivers in the same order.
    pub(crate) fn new(config: &SimulatedSpsConfig) -> Self {
        let system_types = iter::empty()
            .chain(config.sidecar.iter().map(|_| ignition::SystemType::Sidecar))
            .chain(config.gimlet.iter().map(|_| ignition::SystemType::Gimlet))
            .chain(config.psc.iter().map(|_| ignition::SystemType::Psc));
        let targets: Arc<[RackIgnitionTarget]> = system_types
            .map(|system_type| RackIgnitionTarget {
                system_type,
                power: watch::Sender::new(ignition::SystemPowerState::On),
            })
            .collect();
        assert!(
            targets.len() <= NUM_IGNITION_TARGETS,
            "too many simulated SPs"
        );
        Self { num_sidecars: config.sidecar.len(), targets }
    }

    pub(crate) fn subscribe_all(
        &self,
    ) -> impl Iterator<Item = watch::Receiver<ignition::SystemPowerState>> + '_
    {
        self.targets.iter().map(|target| target.power.subscribe())
    }
}

struct FakeIgnition {
    local_sidecar: usize,
    link_events: Vec<LinkEvents>,
    rack_power: RackIgnitionPower,
}

fn empty_transceiver_events() -> ignition::TransceiverEvents {
    ignition::TransceiverEvents {
        encoding_error: false,
        decoding_error: false,
        ordered_set_invalid: false,
        message_version_invalid: false,
        message_type_invalid: false,
        message_checksum_invalid: false,
    }
}

fn empty_link_events() -> LinkEvents {
    LinkEvents {
        controller: empty_transceiver_events(),
        target_link0: empty_transceiver_events(),
        target_link1: empty_transceiver_events(),
    }
}

fn initial_ignition_state(system_type: ignition::SystemType) -> IgnitionState {
    fn valid_receiver() -> ignition::ReceiverStatus {
        ignition::ReceiverStatus {
            aligned: true,
            locked: true,
            polarity_inverted: false,
        }
    }
    IgnitionState {
        receiver: valid_receiver(),
        target: Some(ignition::TargetState {
            system_type,
            power_state: ignition::SystemPowerState::On,
            power_reset_in_progress: false,
            faults: ignition::SystemFaults {
                power_a3: false,
                power_a2: false,
                sp: false,
                rot: false,
            },
            controller0_present: true,
            controller1_present: false,
            link0_receiver_status: valid_receiver(),
            link1_receiver_status: valid_receiver(),
        }),
    }
}

impl FakeIgnition {
    fn new(local_sidecar: usize, rack_power: RackIgnitionPower) -> Self {
        assert!(
            local_sidecar < rack_power.num_sidecars,
            "sidecar {local_sidecar} is one of the rack's {} sidecars",
            rack_power.num_sidecars,
        );
        Self {
            local_sidecar,
            link_events: vec![empty_link_events(); NUM_IGNITION_TARGETS],
            rack_power,
        }
    }

    fn num_targets(&self) -> usize {
        NUM_IGNITION_TARGETS
    }

    fn current_state(&self) -> Vec<IgnitionState> {
        (0..NUM_IGNITION_TARGETS).map(|target| self.state_of(target)).collect()
    }

    fn get_target(&self, target: u8) -> Result<IgnitionState, SpError> {
        let target = usize::from(target);
        if target < NUM_IGNITION_TARGETS {
            Ok(self.state_of(target))
        } else {
            Err(SpError::Ignition(IgnitionError::InvalidPort))
        }
    }

    // TODO-RAINCLAUDE: MGS configs number switch targets per sidecar: target 0 is the local sidecar and target 1 its peer (gateway-test-utils config.test.toml; smf/mgs/config.toml uses 35/34 the same way), so swap target 0 with this sidecar's rack index; sled and PSC targets are the same from either switch.
    fn rack_target(&self, target: usize) -> usize {
        if target == 0 {
            self.local_sidecar
        } else if target == self.local_sidecar {
            0
        } else {
            target
        }
    }

    fn rack_target_of(&self, target: usize) -> Option<&RackIgnitionTarget> {
        self.rack_power.targets.get(self.rack_target(target))
    }

    fn state_of(&self, target: usize) -> IgnitionState {
        match self.rack_target_of(target) {
            Some(rack_target) => {
                let mut state = initial_ignition_state(rack_target.system_type);
                if let Some(target_state) = state.target.as_mut() {
                    target_state.power_state = *rack_target.power.borrow();
                }
                state
            }
            None => IgnitionState {
                receiver: ignition::ReceiverStatus {
                    aligned: false,
                    locked: false,
                    polarity_inverted: false,
                },
                target: None,
            },
        }
    }

    fn command(
        &mut self,
        target: u8,
        command: IgnitionCommand,
    ) -> Result<(), SpError> {
        _ = self.get_target(target)?;
        let power = &self
            .rack_target_of(usize::from(target))
            .ok_or(SpError::Ignition(IgnitionError::NoTargetPresent))?
            .power;

        match command {
            IgnitionCommand::PowerOn | IgnitionCommand::PowerReset => {
                // TODO-RAINCLAUDE: power-on resumes the SP with the state it had before power-off (no simulated reboot), and a PowerReset of a powered target is a no-op.
                power.send_replace(ignition::SystemPowerState::On);
            }
            IgnitionCommand::PowerOff => {
                power.send_replace(ignition::SystemPowerState::Off);
            }
            IgnitionCommand::AlwaysTransmit { .. } => {
                // This is only used in manufacturing; do nothing.
            }
        }

        Ok(())
    }
}
