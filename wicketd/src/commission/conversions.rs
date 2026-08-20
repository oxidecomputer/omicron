// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Conversions between internal and commission types.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv6Addr;
use std::num::NonZeroU32;

use bootstrap_agent_lockstep_types as bootstrap;
use gateway_types::rot::RotImageError;
use iddqd::IdOrdMap;
use omicron_uuid_kinds::GenericUuid;
use sled_hardware_types::BaseboardId;
use transceiver_controller::message::ExtendedStatus;
use transceiver_controller::{
    CmisDatapath, CmisDatapathState, CmisLaneStatus, Datapath, Monitors,
    PowerMode, PowerState, ReceiverPower, Sff8636Datapath, Vendor, VendorInfo,
};
use wicket_common::inventory::{
    SpComponentCaboose, SpIgnition, SpState, SpType, Transceiver,
};
use wicketd_commission_types::inventory as ct_inv;
use wicketd_commission_types::rack_setup::{
    NewPasswordHash, RackOperation, RackOperationKind, RackOperationState,
    RackSetupStatus, RackState, RssStepInfo,
};
use wicketd_commission_types::update::StartUpdateOptions;

use crate::mgs::{
    Fetched, FetchedSpData, MgsFetchError, RotFetch, SpRecord, Stage0Fetch,
};
use crate::transceivers::{GetTransceiversResponse, SwitchTransceivers};

fn caboose_to_ct(caboose: &SpComponentCaboose) -> ct_inv::Caboose {
    let SpComponentCaboose { version, board, name, git_commit, sign, epoch } =
        caboose;
    ct_inv::Caboose {
        version: version.clone(),
        board: board.clone(),
        name: name.clone(),
        git_commit: git_commit.clone(),
        sign: sign.clone(),
        epoch: epoch.clone(),
    }
}

/// Project a wicketd fetch error onto the commission wire type.
///
/// The observation instant becomes an `age` (how long ago the error was seen),
/// computed at read time, since wicketd and its clients keep independent clocks.
pub(crate) fn fetch_error_to_ct(error: &MgsFetchError) -> ct_inv::FetchError {
    ct_inv::FetchError {
        message: error.message.clone(),
        age: error.observed_at.elapsed(),
    }
}

fn slot_caboose_to_ct(
    caboose: &Fetched<SpComponentCaboose>,
) -> ct_inv::SlotCaboose {
    match caboose {
        Fetched::NotRead => ct_inv::SlotCaboose::NotRead,
        Fetched::Error(error) => {
            ct_inv::SlotCaboose::Error { error: fetch_error_to_ct(error) }
        }
        Fetched::Read(caboose) => {
            ct_inv::SlotCaboose::Read { caboose: caboose_to_ct(caboose) }
        }
    }
}

fn stage0_caboose_to_ct(stage0: &Stage0Fetch) -> ct_inv::Stage0Caboose {
    match stage0 {
        Stage0Fetch::Unsupported => ct_inv::Stage0Caboose::Unsupported,
        Stage0Fetch::Supported(Fetched::NotRead) => {
            ct_inv::Stage0Caboose::NotRead
        }
        Stage0Fetch::Supported(Fetched::Error(error)) => {
            ct_inv::Stage0Caboose::Error { error: fetch_error_to_ct(error) }
        }
        Stage0Fetch::Supported(Fetched::Read(caboose)) => {
            ct_inv::Stage0Caboose::Read { caboose: caboose_to_ct(caboose) }
        }
    }
}

fn rot_info_to_ct(data: Option<&FetchedSpData>) -> ct_inv::RotInfo {
    // Without SP data at all, the RoT has not been read; the RoT's own read
    // state lives inside a successful SP fetch.
    let Some(data) = data else {
        return ct_inv::RotInfo::NotRead;
    };

    match &data.rot {
        // The SP itself reported it could not reach its RoT. This is the SP's
        // report (from SpState.rot), not a wicketd fetch error, so it stays
        // message-only.
        RotFetch::CommunicationFailed { message } => {
            ct_inv::RotInfo::Error { message: message.clone() }
        }
        RotFetch::Read(rot) => ct_inv::RotInfo::Read {
            active: rot.active,
            slot_a: ct_inv::RotSlotInfo {
                caboose: slot_caboose_to_ct(&rot.caboose_a),
                validity: image_validity(
                    rot.image_errors.as_ref().map(|e| e.slot_a),
                ),
            },
            slot_b: ct_inv::RotSlotInfo {
                caboose: slot_caboose_to_ct(&rot.caboose_b),
                validity: image_validity(
                    rot.image_errors.as_ref().map(|e| e.slot_b),
                ),
            },
            stage0: ct_inv::RotStage0Info {
                caboose: stage0_caboose_to_ct(&rot.stage0),
                validity: image_validity(
                    rot.image_errors.as_ref().map(|e| e.stage0),
                ),
            },
            stage0next: ct_inv::RotStage0Info {
                caboose: stage0_caboose_to_ct(&rot.stage0next),
                validity: image_validity(
                    rot.image_errors.as_ref().map(|e| e.stage0next),
                ),
            },
        },
    }
}

fn image_validity(
    error: Option<Option<RotImageError>>,
) -> ct_inv::RotImageValidity {
    match error {
        None => ct_inv::RotImageValidity::Unsupported,
        Some(None) => ct_inv::RotImageValidity::Valid,
        Some(Some(error)) => ct_inv::RotImageValidity::Invalid { error },
    }
}

fn ignition_to_ct(ignition: Option<SpIgnition>) -> ct_inv::SpIgnitionInfo {
    match ignition {
        None => ct_inv::SpIgnitionInfo::NotRead,
        Some(SpIgnition::Absent) => ct_inv::SpIgnitionInfo::Absent,
        Some(SpIgnition::Present {
            power,
            ctrl_detect_0,
            ctrl_detect_1,
            flt_a3,
            flt_a2,
            flt_rot,
            flt_sp,
            // The ignition target's id is not projected by the commission API.
            id: _,
        }) => ct_inv::SpIgnitionInfo::Present {
            power,
            faults: ct_inv::IgnitionFaults {
                a3: flt_a3,
                a2: flt_a2,
                rot: flt_rot,
                sp: flt_sp,
            },
            ctrl_detect: ct_inv::IgnitionControllerDetect {
                switch0: ctrl_detect_0,
                switch1: ctrl_detect_1,
            },
        },
    }
}

pub(crate) fn sp_info_to_ct(record: &SpRecord) -> ct_inv::SpInfo {
    let SpRecord { id, ignition, data, last_state_fetch_error } = record;

    // The SP's state, cabooses, and RoT all live inside a single successful SP
    // fetch, so they are populated together when `data` is present and unread
    // when it is absent.
    let (state, caboose_active, caboose_inactive) = match data {
        // A stale-but-real reading beats no reading, so the reading stays
        // primary. If the most recent fetch failed while this older reading
        // still stands, that concurrent failure rides alongside on
        // `refresh_error` rather than displacing the reading.
        Some(FetchedSpData {
            state,
            // Not projected by the commission API.
            components: _,
            caboose_active,
            caboose_inactive,
            mgs_received,
            // The RoT is projected separately by `rot_info_to_ct` below, off the
            // whole `data`.
            rot: _,
        }) => {
            let SpState {
                serial_number,
                model,
                power_state,
                // The remaining identity fields are not projected by the
                // commission API.
                revision: _,
                hubris_archive_id: _,
                base_mac_address: _,
                // The RoT state is projected via `FetchedSpData::rot` (which
                // is derived from this field at fetch time), not read here.
                rot: _,
            } = state;
            (
                ct_inv::SpStateInfo::Read {
                    baseboard: ct_inv::BaseboardId {
                        part_number: model.clone(),
                        serial_number: serial_number.clone(),
                    },
                    power_state: *power_state,
                    refresh_error: last_state_fetch_error
                        .as_ref()
                        .map(fetch_error_to_ct),
                    age: mgs_received.elapsed(),
                },
                slot_caboose_to_ct(caboose_active),
                slot_caboose_to_ct(caboose_inactive),
            )
        }
        None => {
            let state = match last_state_fetch_error {
                Some(error) => ct_inv::SpStateInfo::Error {
                    error: fetch_error_to_ct(error),
                },
                None => ct_inv::SpStateInfo::NotRead,
            };
            (state, ct_inv::SlotCaboose::NotRead, ct_inv::SlotCaboose::NotRead)
        }
    };

    ct_inv::SpInfo {
        id: *id,
        state,
        ignition: ignition_to_ct(ignition.clone()),
        caboose_active,
        caboose_inactive,
        rot: rot_info_to_ct(data.as_ref()),
    }
}

pub(crate) fn bootstrap_sleds_to_ct(
    records: &IdOrdMap<SpRecord>,
    ddm_discovered_sleds: &BTreeMap<BaseboardId, Ipv6Addr>,
) -> ct_inv::GetBootstrapSledsResponse {
    // Use the BaseboardId (part number + serial number) to match peers to
    // sleds.
    let mut peers = ddm_discovered_sleds.clone();
    let unidentified_peers = BTreeSet::new();

    let sleds = IdOrdMap::from_iter_unique(records.iter().filter_map(
        |record| {
            if record.id.typ != SpType::Sled {
                return None;
            }
            let state = bootstrap_sled_state_to_ct(record, &mut peers)?;
            Some(ct_inv::BootstrapSled { id: record.id, state })
        },
    ))
    .expect(
        "the manager's SP records are keyed by SpIdentifier, so the projected \
         BootstrapSleds have unique ids",
    );

    // Anything left over couldn't be matched to a sled in inventory.
    let remaining = peers.into_iter().map(|(baseboard, ip)| {
        ct_inv::UnmatchedBootstrapPeer { baseboard, ip }
    });
    let unmatched_peers = IdOrdMap::from_iter_unique(remaining).expect(
        "the remaining peers are keyed by BaseboardId, so the projected \
         UnmatchedBootstrapPeers have unique baseboards",
    );

    ct_inv::GetBootstrapSledsResponse {
        sleds,
        unmatched_peers,
        unidentified_peers,
    }
}

fn bootstrap_sled_state_to_ct(
    record: &SpRecord,
    peers: &mut BTreeMap<ct_inv::BaseboardId, Ipv6Addr>,
) -> Option<ct_inv::BootstrapSledState> {
    // A stale-but-real data read is helpful, so a record with data is projected
    // to Read even if the latest fetch failed.
    // (`SpStateInfo::Read::refresh_error` reports that failure.)
    if let Some(data) = &record.data {
        let baseboard = ct_inv::BaseboardId {
            part_number: data.state.model.clone(),
            serial_number: data.state.serial_number.clone(),
        };
        let ip = peers.remove(&baseboard);
        return Some(ct_inv::BootstrapSledState::Read { baseboard, ip });
    }

    if let Some(error) = &record.last_state_fetch_error {
        return Some(ct_inv::BootstrapSledState::Error {
            error: fetch_error_to_ct(error),
        });
    }

    // We don't have any baseboard information, so ignition is all we have.
    match &record.ignition {
        Some(SpIgnition::Present { .. }) => {
            Some(ct_inv::BootstrapSledState::NotRead)
        }
        Some(SpIgnition::Absent) | None => None,
    }
}

pub(crate) fn transceivers_to_ct(
    response: GetTransceiversResponse,
) -> IdOrdMap<ct_inv::SwitchTransceivers> {
    match response {
        GetTransceiversResponse::Unavailable => IdOrdMap::new(),
        GetTransceiversResponse::Response { transceivers } => {
            IdOrdMap::from_iter_unique(
                transceivers.into_iter().map(switch_transceivers_to_ct),
            )
            .expect(
                "the internal transceiver inventory is keyed by SwitchSlot, so \
                 the projected SwitchTransceivers have unique switch keys",
            )
        }
    }
}

fn switch_transceivers_to_ct(
    read: SwitchTransceivers,
) -> ct_inv::SwitchTransceivers {
    let SwitchTransceivers { switch, transceivers, updated_at } = read;
    let transceivers = transceivers_by_port(transceivers);
    ct_inv::SwitchTransceivers {
        switch,
        last_seen: updated_at.elapsed(),
        transceivers,
    }
}

fn transceivers_by_port(
    transceivers: Vec<Transceiver>,
) -> IdOrdMap<ct_inv::Transceiver> {
    // The incoming data is a Vec as opposed to an IdOrdMap or similar, so port
    // uniqueness is not enforced by the type. Report the duplicate port instead
    // of dropping one of the readings or panicking.
    let mut by_port: IdOrdMap<ct_inv::Transceiver> = IdOrdMap::new();
    for transceiver in transceivers {
        let projected = transceiver_to_ct(transceiver);
        let entry = if by_port.contains_key(projected.port.as_str()) {
            duplicate_port_to_ct(projected.port)
        } else {
            projected
        };
        by_port.insert_overwrite(entry);
    }
    by_port
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "the switch reported front port {port} more than once, so its readings \
     cannot be attributed to a single transceiver"
)]
struct DuplicateFrontPort {
    port: String,
}

fn duplicate_port_to_ct(port: String) -> ct_inv::Transceiver {
    let message = DuplicateFrontPort { port: port.clone() }.to_string();
    ct_inv::Transceiver {
        port,
        status: ct_inv::TransceiverStatus::Error { message: message.clone() },
        power: ct_inv::TransceiverPower::Error { message: message.clone() },
        vendor: ct_inv::TransceiverVendor::Error { message: message.clone() },
        monitors: ct_inv::TransceiverMonitors::Error {
            message: message.clone(),
        },
        datapath: ct_inv::TransceiverDatapath::Error { message },
    }
}

fn transceiver_to_ct(transceiver: Transceiver) -> ct_inv::Transceiver {
    let Transceiver { port, status, power, vendor, datapath, monitors } =
        transceiver;
    ct_inv::Transceiver {
        port,
        status: transceiver_status_to_ct(status),
        power: transceiver_power_to_ct(power),
        vendor: transceiver_vendor_to_ct(vendor),
        monitors: transceiver_monitors_to_ct(monitors),
        datapath: transceiver_datapath_to_ct(datapath),
    }
}

fn transceiver_power_to_ct(
    power: Result<PowerMode, String>,
) -> ct_inv::TransceiverPower {
    match power {
        Err(message) => ct_inv::TransceiverPower::Error { message },
        Ok(PowerMode {
            state,
            // The software-override flag is not projected by the commission API.
            software_override: _,
        }) => {
            let mode = match state {
                PowerState::Off => ct_inv::TransceiverPowerMode::Off,
                PowerState::Low => ct_inv::TransceiverPowerMode::Low,
                PowerState::High => ct_inv::TransceiverPowerMode::High,
            };
            ct_inv::TransceiverPower::Read { mode }
        }
    }
}

fn transceiver_status_to_ct(
    status: Result<ExtendedStatus, String>,
) -> ct_inv::TransceiverStatus {
    match status {
        Err(message) => ct_inv::TransceiverStatus::Error { message },
        Ok(status) => ct_inv::TransceiverStatus::Read {
            present: status.contains(ExtendedStatus::PRESENT),
            enabled: status.contains(ExtendedStatus::ENABLED),
            power_good: status.contains(ExtendedStatus::POWER_GOOD),
            fault_power_timeout: status
                .contains(ExtendedStatus::FAULT_POWER_TIMEOUT),
            fault_power_lost: status.contains(ExtendedStatus::FAULT_POWER_LOST),
            disabled_by_sp: status.contains(ExtendedStatus::DISABLED_BY_SP),
            in_reset: status.contains(ExtendedStatus::RESET),
            low_power_mode: status.contains(ExtendedStatus::LOW_POWER_MODE),
            interrupt: status.contains(ExtendedStatus::INTERRUPT),
        },
    }
}

fn transceiver_vendor_to_ct(
    vendor: Result<VendorInfo, String>,
) -> ct_inv::TransceiverVendor {
    match vendor {
        Err(message) => ct_inv::TransceiverVendor::Error { message },
        Ok(VendorInfo {
            vendor:
                Vendor {
                    name,
                    part,
                    serial,
                    // The remaining vendor fields are not projected.
                    oui: _,
                    revision: _,
                    date: _,
                },
            // The SFF-8024 identifier is not projected.
            identifier: _,
        }) => ct_inv::TransceiverVendor::Read { name, part, serial },
    }
}

fn transceiver_monitors_to_ct(
    monitors: Result<Monitors, String>,
) -> ct_inv::TransceiverMonitors {
    match monitors {
        Err(message) => ct_inv::TransceiverMonitors::Error { message },
        Ok(Monitors {
            receiver_power,
            transmitter_power,
            // The remaining monitors are not projected.
            temperature: _,
            supply_voltage: _,
            transmitter_bias_current: _,
            aux_monitors: _,
        }) => match ReportedPower::new(receiver_power, transmitter_power) {
            // If both rx and tx are missing, it's probably something like a
            // passive copper cable. Mark the monitors as unsupported.
            None => ct_inv::TransceiverMonitors::Unsupported,
            Some(power) => match lane_monitors_to_ct(power) {
                Ok(lanes) => ct_inv::TransceiverMonitors::Read { lanes },
                Err(err) => ct_inv::TransceiverMonitors::Error {
                    message: err.to_string(),
                },
            },
        },
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ReportedPower {
    Rx(Vec<ReceiverPower>),
    Tx(Vec<f32>),
    Both { rx: Vec<ReceiverPower>, tx: Vec<f32> },
}

impl ReportedPower {
    fn new(
        receiver_power: Option<Vec<ReceiverPower>>,
        transmitter_power: Option<Vec<f32>>,
    ) -> Option<Self> {
        match (receiver_power, transmitter_power) {
            (Some(rx), Some(tx)) => Some(Self::Both { rx, tx }),
            (Some(rx), None) => Some(Self::Rx(rx)),
            (None, Some(tx)) => Some(Self::Tx(tx)),
            (None, None) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
enum LaneMonitorsError {
    #[error(
        "module reported received power for {rx_lanes} lane(s) but \
         transmitted power for {tx_lanes} lane(s); the two must describe the \
         same lanes"
    )]
    MismatchedLaneCounts { rx_lanes: usize, tx_lanes: usize },

    #[error(
        "module reported optical power for {lanes} lanes, more than a lane \
         number can represent"
    )]
    TooManyLanes { lanes: usize },
}

fn lane_monitors_to_ct(
    power: ReportedPower,
) -> Result<IdOrdMap<ct_inv::LaneMonitors>, LaneMonitorsError> {
    // The SP sends a pair of vectors and not a vector of pairs, which means
    // that type system can represent the receiver and transmitter power
    // readings having different lengths. That's a nonsensical input, though, so
    // catch it.
    let (lane_count, rx, tx) = match power {
        ReportedPower::Rx(rx) => (rx.len(), rx, Vec::new()),
        ReportedPower::Tx(tx) => (tx.len(), Vec::new(), tx),
        ReportedPower::Both { rx, tx } => {
            if rx.len() != tx.len() {
                return Err(LaneMonitorsError::MismatchedLaneCounts {
                    rx_lanes: rx.len(),
                    tx_lanes: tx.len(),
                });
            }
            (rx.len(), rx, tx)
        }
    };

    let lane_count = u8::try_from(lane_count)
        .map_err(|_| LaneMonitorsError::TooManyLanes { lanes: lane_count })?;

    let lanes = IdOrdMap::from_iter_unique((0..lane_count).map(|lane| {
        let index = usize::from(lane);
        ct_inv::LaneMonitors {
            lane,
            rx_power: rx.get(index).copied().map(receiver_power_to_ct),
            tx_power_mw: tx.get(index).copied(),
        }
    }))
    .expect("lane numbers come from a range, so they are unique");
    Ok(lanes)
}

fn receiver_power_to_ct(power: ReceiverPower) -> ct_inv::ReceiverPower {
    match power {
        ReceiverPower::Average(value_mw) => {
            ct_inv::ReceiverPower::Average { value_mw }
        }
        ReceiverPower::PeakToPeak(value_mw) => {
            ct_inv::ReceiverPower::PeakToPeak { value_mw }
        }
    }
}

fn transceiver_datapath_to_ct(
    datapath: Result<Datapath, String>,
) -> ct_inv::TransceiverDatapath {
    match datapath {
        Err(message) => ct_inv::TransceiverDatapath::Error { message },
        Ok(Datapath::Sff8636 { lanes, connector: _, specification: _ }) => {
            let lanes = lanes.map(sff8636_lane_to_ct);
            ct_inv::TransceiverDatapath::Sff8636 { lanes }
        }
        Ok(Datapath::Cmis { datapaths, connector: _, supported_lanes: _ }) => {
            let datapaths = IdOrdMap::from_iter_unique(
                datapaths.into_iter().map(|(application, datapath)| {
                    cmis_datapath_to_ct(application, datapath)
                }),
            )
            .expect(
                "the internal CMIS datapaths are keyed by application selector \
                 code (a BTreeMap<u8, _>), so the projected CmisDatapaths have \
                 unique application keys",
            );
            ct_inv::TransceiverDatapath::Cmis { datapaths }
        }
    }
}

fn sff8636_lane_to_ct(lane: Sff8636Datapath) -> ct_inv::Sff8636LaneFaults {
    let Sff8636Datapath {
        rx_los,
        tx_los,
        rx_lol,
        tx_lol,
        tx_fault,
        tx_enabled: _,
        tx_adaptive_eq_fault: _,
        tx_cdr_enabled: _,
        rx_cdr_enabled: _,
    } = lane;
    ct_inv::Sff8636LaneFaults { rx_los, tx_los, rx_lol, tx_lol, tx_fault }
}

fn cmis_datapath_to_ct(
    application: u8,
    datapath: CmisDatapath,
) -> ct_inv::CmisDatapath {
    let CmisDatapath { lane_status, application: _ } = datapath;
    let lanes = IdOrdMap::from_iter_unique(
        lane_status
            .into_iter()
            .map(|(lane, status)| cmis_lane_to_ct(lane, status)),
    )
    .expect(
        "the internal CMIS lane statuses are keyed by lane number (a \
         BTreeMap<u8, _>), so the projected CmisLaneStatuses have unique lane \
         keys",
    );
    ct_inv::CmisDatapath { application, lanes }
}

fn cmis_lane_to_ct(lane: u8, status: CmisLaneStatus) -> ct_inv::CmisLaneStatus {
    let CmisLaneStatus {
        state,
        rx_los,
        tx_los,
        rx_lol,
        tx_lol,
        tx_failure,
        tx_input_polarity: _,
        tx_output_enabled: _,
        tx_auto_squelch_disable: _,
        tx_force_squelch: _,
        rx_output_polarity: _,
        rx_output_enabled: _,
        rx_auto_squelch_disable: _,
        rx_output_status: _,
        tx_output_status: _,
        tx_adaptive_eq_fail: _,
    } = status;
    ct_inv::CmisLaneStatus {
        lane,
        state: cmis_datapath_state_to_ct(state),
        rx_los: cmis_flag_to_ct(rx_los),
        tx_los: cmis_flag_to_ct(tx_los),
        rx_lol: cmis_flag_to_ct(rx_lol),
        tx_lol: cmis_flag_to_ct(tx_lol),
        tx_fault: cmis_flag_to_ct(tx_failure),
    }
}

fn cmis_flag_to_ct(flag: Option<bool>) -> ct_inv::FaultFlag {
    match flag {
        None => ct_inv::FaultFlag::Unsupported,
        Some(true) => ct_inv::FaultFlag::Asserted,
        Some(false) => ct_inv::FaultFlag::Clear,
    }
}

fn cmis_datapath_state_to_ct(
    state: CmisDatapathState,
) -> ct_inv::CmisDatapathState {
    match state {
        CmisDatapathState::Deactivated => {
            ct_inv::CmisDatapathState::Deactivated
        }
        CmisDatapathState::Init => ct_inv::CmisDatapathState::Init,
        CmisDatapathState::Deinit => ct_inv::CmisDatapathState::Deinit,
        CmisDatapathState::Activated => ct_inv::CmisDatapathState::Activated,
        CmisDatapathState::TxTurnOn => ct_inv::CmisDatapathState::TxTurnOn,
        CmisDatapathState::TxTurnOff => ct_inv::CmisDatapathState::TxTurnOff,
        CmisDatapathState::Initialized => {
            ct_inv::CmisDatapathState::Initialized
        }
    }
}

fn rss_step_to_ct(step: bootstrap::RssStep) -> RssStepInfo {
    // RssStep::index() is 0-based, so +1 makes it 1-based.
    let step_number =
        u32::try_from(step.index() + 1).expect("RSS step number fits in a u32");
    let total_steps =
        u32::try_from(step.max_step()).expect("RSS step count fits in a u32");
    RssStepInfo {
        step: NonZeroU32::new(step_number).expect("step index + 1 is nonzero"),
        total_steps: NonZeroU32::new(total_steps)
            .expect("RSS step count is nonzero"),
        description: step.description().to_string(),
    }
}

pub(crate) fn rack_setup_status_to_ct(
    status: bootstrap::RackOperationStatus,
) -> RackSetupStatus {
    use bootstrap::RackOperationStatus as B;
    match status {
        B::Uninitialized => RackSetupStatus {
            rack_state: RackState::Uninitialized,
            operation: None,
        },
        B::Initializing { id, step } => RackSetupStatus {
            rack_state: RackState::Other,
            operation: Some(RackOperation {
                kind: RackOperationKind::INITIALIZE,
                id: id.into_untyped_uuid(),
                state: RackOperationState::InProgress {
                    current_step: Some(rss_step_to_ct(step)),
                },
            }),
        },
        B::InitializationFailed { id, message } => RackSetupStatus {
            rack_state: RackState::Other,
            operation: Some(RackOperation {
                kind: RackOperationKind::INITIALIZE,
                id: id.into_untyped_uuid(),
                state: RackOperationState::Failed {
                    message,
                    failed_step: None,
                },
            }),
        },
        B::InitializationPanicked { id } => RackSetupStatus {
            rack_state: RackState::Other,
            operation: Some(RackOperation {
                kind: RackOperationKind::INITIALIZE,
                id: id.into_untyped_uuid(),
                state: RackOperationState::Panicked,
            }),
        },
        B::Initialized { id: Some(id) } => RackSetupStatus {
            rack_state: RackState::Initialized,
            operation: Some(RackOperation {
                kind: RackOperationKind::INITIALIZE,
                id: id.into_untyped_uuid(),
                state: RackOperationState::Completed,
            }),
        },
        B::Initialized { id: None } => RackSetupStatus {
            rack_state: RackState::Initialized,
            operation: None,
        },
        B::MultirackJoinInProgress { id } => RackSetupStatus {
            rack_state: RackState::Other,
            operation: Some(RackOperation {
                kind: RackOperationKind::MULTIRACK_JOIN,
                id: id.into_untyped_uuid(),
                state: RackOperationState::InProgress { current_step: None },
            }),
        },
        B::MultirackJoinCompleted { id: Some(id) } => RackSetupStatus {
            rack_state: RackState::Initialized,
            operation: Some(RackOperation {
                kind: RackOperationKind::MULTIRACK_JOIN,
                id: id.into_untyped_uuid(),
                state: RackOperationState::Completed,
            }),
        },
        B::MultirackJoinCompleted { id: None } => RackSetupStatus {
            rack_state: RackState::Initialized,
            operation: None,
        },
        B::MultirackJoinFailed { id, message } => RackSetupStatus {
            rack_state: RackState::Other,
            operation: Some(RackOperation {
                kind: RackOperationKind::MULTIRACK_JOIN,
                id: id.into_untyped_uuid(),
                state: RackOperationState::Failed {
                    message,
                    failed_step: None,
                },
            }),
        },
        B::MultirackJoinPanicked { id } => RackSetupStatus {
            rack_state: RackState::Other,
            operation: Some(RackOperation {
                kind: RackOperationKind::MULTIRACK_JOIN,
                id: id.into_untyped_uuid(),
                state: RackOperationState::Panicked,
            }),
        },
    }
}

pub(crate) fn start_update_options_to_internal(
    options: StartUpdateOptions,
) -> wicket_common::rack_update::StartUpdateOptions {
    let StartUpdateOptions {
        skip_rot_bootloader_version_check,
        skip_rot_version_check,
        skip_sp_version_check,
    } = options;
    // The commission API deliberately does not expose the test-only knobs
    // provided by the internal update API.
    wicket_common::rack_update::StartUpdateOptions {
        test_error: None,
        test_step_seconds: None,
        test_simulate_rot_bootloader_result: None,
        test_simulate_rot_result: None,
        test_simulate_sp_result: None,
        skip_rot_bootloader_version_check,
        skip_rot_version_check,
        skip_sp_version_check,
    }
}

pub(crate) fn password_hash_to_internal(
    hash: NewPasswordHash,
) -> Result<omicron_passwords::NewPasswordHash, String> {
    hash.0.parse::<omicron_passwords::NewPasswordHash>().map_err(|err| {
        format!("invalid recovery password hash (PHC string): {err}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mgs::{RotData, RotImageErrors};
    use bootstrap::RackOperationStatus as B;
    use gateway_types::component::PowerState;
    use iddqd::id_ord_map;
    use omicron_uuid_kinds::RackInitUuid;
    use sled_agent_types::early_networking::SwitchSlot;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::time::Instant;
    use transceiver_controller::{
        ApplicationDescriptor, CmisDatapathState, ConnectorType,
        HostElectricalInterfaceId, Identifier, MediaInterfaceId, MediaType,
        Oui, OutputStatus, ReceiverPower, SffComplianceCode,
    };
    use uuid::Uuid;
    use wicket_common::inventory::{
        RotSlot, RotState, SpIdentifier, SpIgnitionSystemType, SpState, SpType,
    };

    fn sled(slot: u16) -> SpIdentifier {
        SpIdentifier { typ: SpType::Sled, slot }
    }

    fn sample_caboose(version: &str) -> SpComponentCaboose {
        SpComponentCaboose {
            git_commit: "commit".to_string(),
            board: "board".to_string(),
            name: "name".to_string(),
            version: version.to_string(),
            sign: None,
            epoch: None,
        }
    }

    fn mgs_fetch_error(message: &str) -> MgsFetchError {
        MgsFetchError {
            message: message.to_string(),
            observed_at: Instant::now(),
        }
    }

    /// An `MgsFetchError` whose observation instant is `ago` in the past, so its
    /// projected `age` is at least `ago`.
    fn mgs_fetch_error_observed_ago(
        message: &str,
        ago: Duration,
    ) -> MgsFetchError {
        MgsFetchError {
            message: message.to_string(),
            observed_at: Instant::now()
                .checked_sub(ago)
                .expect("monotonic clock is far enough past its epoch"),
        }
    }

    /// An `SpState` carrying `rot`; the non-RoT fields are filler.
    fn sp_state(serial_number: &str, rot: RotState) -> SpState {
        SpState {
            serial_number: serial_number.to_string(),
            model: "model".to_string(),
            revision: 0,
            hubris_archive_id: "archive".to_string(),
            base_mac_address: [0; 6],
            power_state: PowerState::A0,
            rot,
        }
    }

    /// A V3 RoT state with an error on slot B and on stage0next, exercising the
    /// per-slot image-error projection.
    fn rot_state_v3_with_errors() -> RotState {
        RotState::V3 {
            active: RotSlot::A,
            persistent_boot_preference: RotSlot::A,
            pending_persistent_boot_preference: None,
            transient_boot_preference: None,
            slot_a_fwid: "slot-a".to_string(),
            slot_b_fwid: "slot-b".to_string(),
            stage0_fwid: "stage0".to_string(),
            stage0next_fwid: "stage0next".to_string(),
            slot_a_error: None,
            slot_b_error: Some(ct_inv::RotImageError::Signature),
            stage0_error: None,
            stage0next_error: Some(ct_inv::RotImageError::BadMagic),
        }
    }

    /// Wrap `state` and `rot` into `FetchedSpData` with the other sub-items
    /// unread.
    fn fetched_data(state: SpState, rot: RotFetch) -> FetchedSpData {
        FetchedSpData {
            state,
            components: Fetched::NotRead,
            caboose_active: Fetched::NotRead,
            caboose_inactive: Fetched::NotRead,
            rot,
            mgs_received: Instant::now(),
        }
    }

    fn sample_application_descriptor() -> ApplicationDescriptor {
        ApplicationDescriptor {
            host_id: HostElectricalInterfaceId::from(0),
            media_id: MediaInterfaceId::from_u8(MediaType::MultiModeFiber, 0)
                .expect("multi-mode fiber has a media interface id"),
            host_lane_count: 4,
            media_lane_count: 4,
            host_lane_assignment_options: 0x01,
            media_lane_assignment_options: 0x01,
        }
    }

    fn sample_vendor_info() -> VendorInfo {
        VendorInfo {
            identifier: Identifier::Qsfp28,
            vendor: Vendor {
                name: "Acme".to_string(),
                oui: Oui([0, 1, 2]),
                part: "PN-1".to_string(),
                revision: "A".to_string(),
                serial: "SN-1".to_string(),
                date: None,
            },
        }
    }

    fn sample_cmis_lane() -> CmisLaneStatus {
        CmisLaneStatus {
            state: CmisDatapathState::Activated,
            tx_input_polarity: None,
            tx_output_enabled: None,
            tx_auto_squelch_disable: None,
            tx_force_squelch: None,
            rx_output_polarity: None,
            rx_output_enabled: None,
            rx_auto_squelch_disable: None,
            rx_output_status: OutputStatus::Valid,
            tx_output_status: OutputStatus::Valid,
            tx_failure: Some(true),
            tx_los: None,
            tx_lol: Some(true),
            tx_adaptive_eq_fail: None,
            rx_los: Some(true),
            rx_lol: None,
        }
    }

    #[test]
    fn caboose_projects_every_field() {
        let full = SpComponentCaboose {
            git_commit: "abc".to_string(),
            board: "gimlet".to_string(),
            name: "sp".to_string(),
            version: "1.0.0".to_string(),
            sign: Some("deadbeef".to_string()),
            epoch: Some("3".to_string()),
        };
        assert_eq!(
            caboose_to_ct(&full),
            ct_inv::Caboose {
                version: "1.0.0".to_string(),
                board: "gimlet".to_string(),
                name: "sp".to_string(),
                git_commit: "abc".to_string(),
                sign: Some("deadbeef".to_string()),
                epoch: Some("3".to_string()),
            },
        );

        let unsigned =
            SpComponentCaboose { sign: None, epoch: None, ..full.clone() };
        let projected = caboose_to_ct(&unsigned);
        assert_eq!(projected.sign, None);
        assert_eq!(projected.epoch, None);
    }

    #[test]
    fn fetch_error_projects_message_and_age() {
        // This is the projection applied to every wicketd fetch error,
        // including the rack-wide ignition-list error surfaced on
        // `Inventory::ignition_fetch_error`.
        let error = mgs_fetch_error_observed_ago(
            "ignition list fetch failed",
            Duration::from_secs(2),
        );
        let projected = fetch_error_to_ct(&error);
        assert_eq!(projected.message, "ignition list fetch failed");
        assert!(
            projected.age >= Duration::from_secs(2),
            "age is derived from the observation instant: {:?}",
            projected.age,
        );
    }

    #[test]
    fn sp_state_error_projects_from_error_only_record() {
        // An SP we have only ever failed to reach: no data, only a recorded
        // state-fetch error. The record must still project (error-only records
        // are visible to the commission API), with the error on `state` and
        // every other sub-item unread.
        let id = sled(3);
        let record = SpRecord {
            id,
            ignition: None,
            data: None,
            last_state_fetch_error: Some(mgs_fetch_error_observed_ago(
                "mgs unreachable",
                Duration::from_secs(1),
            )),
        };

        let info = sp_info_to_ct(&record);
        assert_eq!(info.id, id);
        let ct_inv::SpStateInfo::Error { error } = &info.state else {
            panic!("expected a state error, got {:?}", info.state);
        };
        assert_eq!(error.message, "mgs unreachable");
        assert!(error.age >= Duration::from_secs(1));
        assert_eq!(info.ignition, ct_inv::SpIgnitionInfo::NotRead);
        assert_eq!(info.caboose_active, ct_inv::SlotCaboose::NotRead);
        assert_eq!(info.caboose_inactive, ct_inv::SlotCaboose::NotRead);
        assert_eq!(info.rot, ct_inv::RotInfo::NotRead);
    }

    #[test]
    fn sp_state_prefers_stale_reading_over_recorded_error() {
        // A newer fetch failed, but we still hold a prior reading: the reading
        // stays primary, and the concurrent failure rides alongside on
        // `refresh_error` rather than displacing it.
        let id = sled(1);
        let stale_reading = || {
            fetched_data(
                sp_state(
                    "SimGimlet01",
                    RotState::CommunicationFailed {
                        message: "rot down".to_string(),
                    },
                ),
                RotFetch::CommunicationFailed {
                    message: "rot down".to_string(),
                },
            )
        };

        // With a recorded error, the reading wins and `refresh_error` carries
        // the concurrent failure's message.
        let record = SpRecord {
            id,
            ignition: None,
            data: Some(stale_reading()),
            last_state_fetch_error: Some(mgs_fetch_error("mgs flaked")),
        };
        let ct_inv::SpStateInfo::Read {
            baseboard,
            power_state,
            refresh_error,
            age,
        } = sp_info_to_ct(&record).state
        else {
            panic!("a stale-but-real reading projects to Read");
        };
        assert_eq!(baseboard, baseboard_id("SimGimlet01"));
        assert_eq!(power_state, PowerState::A0);
        assert!(age < Duration::from_secs(30));
        let refresh_error = refresh_error
            .expect("the concurrent failure is surfaced on refresh_error");
        assert_eq!(refresh_error.message, "mgs flaked");

        // With no recorded error, the same reading carries no `refresh_error`.
        let record = SpRecord {
            id,
            ignition: None,
            data: Some(stale_reading()),
            last_state_fetch_error: None,
        };
        let ct_inv::SpStateInfo::Read {
            baseboard,
            power_state,
            refresh_error,
            age,
        } = sp_info_to_ct(&record).state
        else {
            panic!("a reading with no recorded error projects to Read");
        };
        assert_eq!(baseboard, baseboard_id("SimGimlet01"));
        assert_eq!(power_state, PowerState::A0);
        assert_eq!(
            refresh_error, None,
            "a reading with no recorded error carries no refresh_error",
        );
        assert!(age < Duration::from_secs(30));
    }

    #[test]
    fn sp_state_not_read_when_only_ignition_present() {
        // Ignition placed the SP, but its state has never been read and no
        // fetch has failed: the state projects to `NotRead`, not `Error`.
        let record = SpRecord {
            id: sled(2),
            ignition: Some(SpIgnition::Absent),
            data: None,
            last_state_fetch_error: None,
        };
        assert_eq!(sp_info_to_ct(&record).state, ct_inv::SpStateInfo::NotRead);
    }

    fn ignition_present() -> SpIgnition {
        SpIgnition::Present {
            id: SpIgnitionSystemType::Gimlet,
            power: true,
            ctrl_detect_0: true,
            ctrl_detect_1: true,
            flt_a3: false,
            flt_a2: false,
            flt_rot: false,
            flt_sp: false,
        }
    }

    fn read_sled_record(slot: u16, serial_number: &str) -> SpRecord {
        SpRecord {
            id: sled(slot),
            ignition: None,
            data: Some(fetched_data(
                sp_state(
                    serial_number,
                    RotState::CommunicationFailed {
                        message: "rot down".to_string(),
                    },
                ),
                RotFetch::CommunicationFailed {
                    message: "rot down".to_string(),
                },
            )),
            last_state_fetch_error: None,
        }
    }

    fn baseboard_id(serial_number: &str) -> ct_inv::BaseboardId {
        ct_inv::BaseboardId {
            part_number: "model".to_string(),
            serial_number: serial_number.to_string(),
        }
    }

    #[test]
    fn bootstrap_sleds_project_ips_unmatched_and_unidentified_peers() {
        let records = id_ord_map! {
            read_sled_record(0, "SimGimlet00"),
            read_sled_record(1, "SimGimlet01"),
            read_sled_record(2, "SimGimlet02"),
            // Nothing at all is known about this cubby, so it is omitted.
            SpRecord {
                id: sled(3),
                ignition: None,
                data: None,
                last_state_fetch_error: None,
            },
            // Ignition says this cubby is empty, so it is omitted too.
            SpRecord {
                id: sled(4),
                ignition: Some(SpIgnition::Absent),
                data: None,
                last_state_fetch_error: None,
            },
            // Ignition says a sled is present but reading it keeps failing -- it
            // must stay visible because rack setup should have been able to use
            // it.
            SpRecord {
                id: sled(5),
                ignition: Some(ignition_present()),
                data: None,
                last_state_fetch_error: Some(mgs_fetch_error_observed_ago(
                    "sp_get timed out",
                    Duration::from_secs(3),
                )),
            },
            // Ignition says a sled is present and we have not polled it yet.
            SpRecord {
                id: sled(6),
                ignition: Some(ignition_present()),
                data: None,
                last_state_fetch_error: None,
            },
            // Not a sled, so it doesn't appear in bootstrap output.
            SpRecord {
                id: SpIdentifier { typ: SpType::Switch, slot: 0 },
                ignition: Some(ignition_present()),
                data: None,
                last_state_fetch_error: None,
            },
        };

        let ip = |last: u16| -> Ipv6Addr {
            format!("fdb0::{last}").parse().expect("valid IPv6 address")
        };
        let (sled0_ip, sled1_ip, stranger_ip, _) = (ip(1), ip(2), ip(3), ip(4));

        let mut ddm = BTreeMap::new();
        ddm.insert(
            BaseboardId {
                serial_number: "SimGimlet00".to_string(),
                part_number: "model".to_string(),
            },
            sled0_ip,
        );
        // Make sled 1's peer report a different _revision_ from what MGS
        // reports. The identity is part number + serial number, so it still
        // matches.
        ddm.insert(
            BaseboardId {
                serial_number: "SimGimlet01".to_string(),
                part_number: "model".to_string(),
            },
            sled1_ip,
        );
        ddm.insert(
            BaseboardId {
                serial_number: "SimGimlet99".to_string(),
                part_number: "model".to_string(),
            },
            stranger_ip,
        );

        let response = bootstrap_sleds_to_ct(&records, &ddm);

        let reported: BTreeSet<_> =
            response.sleds.iter().map(|sled| sled.id).collect();
        assert_eq!(
            reported,
            BTreeSet::from([sled(0), sled(1), sled(2), sled(5), sled(6)]),
            "cubby 3 (nothing known) and cubby 4 (ignition says empty) are \
             omitted, the switch is not a sled, and everything else is \
             reported: {:?}",
            response.sleds,
        );

        assert_eq!(
            response.sleds.get(&sled(0)).expect("sled 0 present").state,
            ct_inv::BootstrapSledState::Read {
                baseboard: baseboard_id("SimGimlet00"),
                ip: Some(sled0_ip),
            },
            "sled 0's baseboard matches its DDM entry",
        );
        assert_eq!(
            response.sleds.get(&sled(1)).expect("sled 1 present").state,
            ct_inv::BootstrapSledState::Read {
                baseboard: baseboard_id("SimGimlet01"),
                ip: Some(sled1_ip),
            },
            "sled 1 matches despite the peer reporting a different revision",
        );
        assert_eq!(
            response.sleds.get(&sled(2)).expect("sled 2 present").state,
            ct_inv::BootstrapSledState::Read {
                baseboard: baseboard_id("SimGimlet02"),
                ip: None,
            },
            "sled 2 has no DDM entry, so no address yet",
        );

        let sled5 = response.sleds.get(&sled(5)).expect("sled 5 present");
        let ct_inv::BootstrapSledState::Error { error } = &sled5.state else {
            panic!("a sled we keep failing to read is an error: {sled5:?}");
        };
        assert_eq!(error.message, "sp_get timed out");
        assert!(
            error.age >= Duration::from_secs(3),
            "the error carries how long ago it was observed: {:?}",
            error.age,
        );

        assert_eq!(
            response.sleds.get(&sled(6)).expect("sled 6 present").state,
            ct_inv::BootstrapSledState::NotRead,
            "a sled ignition placed but that we have not polled is not_read",
        );

        let readable: Vec<_> =
            response.readable().map(|sled| sled.id).collect();
        assert_eq!(
            readable,
            vec![sled(0), sled(1), sled(2)],
            "readable() skips the unreadable and unpolled cubbies, so a \
             caller building an RSS config cannot pick one by accident",
        );

        assert_eq!(
            response.unmatched_peers,
            id_ord_map! {
                ct_inv::UnmatchedBootstrapPeer {
                    baseboard: baseboard_id("SimGimlet99"),
                    ip: stranger_ip,
                },
            },
            "the peer matching no sled in inventory is surfaced as unmatched",
        );
    }

    #[test]
    fn slot_caboose_projects_all_states() {
        assert_eq!(
            slot_caboose_to_ct(&Fetched::NotRead),
            ct_inv::SlotCaboose::NotRead,
        );

        let projected = slot_caboose_to_ct(&Fetched::Error(mgs_fetch_error(
            "caboose read failed",
        )));
        let ct_inv::SlotCaboose::Error { error } = projected else {
            panic!("expected a slot caboose error, got {projected:?}");
        };
        assert_eq!(error.message, "caboose read failed");

        let projected =
            slot_caboose_to_ct(&Fetched::Read(sample_caboose("1.0.0")));
        let ct_inv::SlotCaboose::Read { caboose } = projected else {
            panic!("expected a read slot caboose, got {projected:?}");
        };
        assert_eq!(caboose.version, "1.0.0");
    }

    #[test]
    fn stage0_caboose_projects_all_states() {
        assert_eq!(
            stage0_caboose_to_ct(&Stage0Fetch::Unsupported),
            ct_inv::Stage0Caboose::Unsupported,
        );
        assert_eq!(
            stage0_caboose_to_ct(&Stage0Fetch::Supported(Fetched::NotRead)),
            ct_inv::Stage0Caboose::NotRead,
        );

        let projected = stage0_caboose_to_ct(&Stage0Fetch::Supported(
            Fetched::Error(mgs_fetch_error("stage0 read failed")),
        ));
        let ct_inv::Stage0Caboose::Error { error } = projected else {
            panic!("expected a stage0 caboose error, got {projected:?}");
        };
        assert_eq!(error.message, "stage0 read failed");

        let projected = stage0_caboose_to_ct(&Stage0Fetch::Supported(
            Fetched::Read(sample_caboose("2.0.0")),
        ));
        let ct_inv::Stage0Caboose::Read { caboose } = projected else {
            panic!("expected a read stage0 caboose, got {projected:?}");
        };
        assert_eq!(caboose.version, "2.0.0");
    }

    #[test]
    fn rot_info_projects_not_read_and_comm_failure() {
        // No SP data at all is "not read", not an error.
        assert_eq!(rot_info_to_ct(None), ct_inv::RotInfo::NotRead);

        // The SP told us it cannot reach its RoT: message-only error.
        let data = fetched_data(
            sp_state(
                "serial",
                RotState::CommunicationFailed {
                    message: "rot unreachable".to_string(),
                },
            ),
            RotFetch::CommunicationFailed {
                message: "rot unreachable".to_string(),
            },
        );
        assert_eq!(
            rot_info_to_ct(Some(&data)),
            ct_inv::RotInfo::Error { message: "rot unreachable".to_string() },
        );
    }

    #[test]
    fn rot_info_read_projects_cabooses_and_image_errors() {
        let rot = RotFetch::Read(Box::new(RotData {
            active: RotSlot::A,
            caboose_a: Fetched::Read(sample_caboose("rot-a")),
            caboose_b: Fetched::Error(mgs_fetch_error("slot b caboose failed")),
            stage0: Stage0Fetch::Supported(Fetched::NotRead),
            stage0next: Stage0Fetch::Supported(Fetched::Read(sample_caboose(
                "stage0next",
            ))),
            image_errors: Some(RotImageErrors {
                slot_a: None,
                slot_b: Some(ct_inv::RotImageError::Signature),
                stage0: None,
                stage0next: Some(ct_inv::RotImageError::BadMagic),
            }),
        }));
        let data =
            fetched_data(sp_state("serial", rot_state_v3_with_errors()), rot);

        let ct_inv::RotInfo::Read {
            active,
            slot_a,
            slot_b,
            stage0,
            stage0next,
        } = rot_info_to_ct(Some(&data))
        else {
            panic!("expected a read RoT");
        };
        assert_eq!(active, ct_inv::RotSlot::A);

        // Slot A: caboose read, no image error.
        let ct_inv::SlotCaboose::Read { caboose } = &slot_a.caboose else {
            panic!("expected slot A caboose read, got {:?}", slot_a.caboose);
        };
        assert_eq!(caboose.version, "rot-a");
        assert_eq!(slot_a.validity, ct_inv::RotImageValidity::Valid);

        // Slot B: caboose fetch failed, and the V3 state reports an image error.
        let ct_inv::SlotCaboose::Error { error } = &slot_b.caboose else {
            panic!("expected slot B caboose error, got {:?}", slot_b.caboose);
        };
        assert_eq!(error.message, "slot b caboose failed");
        assert_eq!(
            slot_b.validity,
            ct_inv::RotImageValidity::Invalid {
                error: ct_inv::RotImageError::Signature,
            },
        );

        // stage0: supported but unread; stage0next: read, with an image error.
        assert_eq!(stage0.caboose, ct_inv::Stage0Caboose::NotRead);
        assert_eq!(stage0.validity, ct_inv::RotImageValidity::Valid);
        let ct_inv::Stage0Caboose::Read { caboose } = &stage0next.caboose
        else {
            panic!("expected stage0next read, got {:?}", stage0next.caboose);
        };
        assert_eq!(caboose.version, "stage0next");
        assert_eq!(
            stage0next.validity,
            ct_inv::RotImageValidity::Invalid {
                error: ct_inv::RotImageError::BadMagic,
            },
        );
    }

    #[test]
    fn rot_info_read_projects_unsupported_validity_for_v2() {
        let rot = RotFetch::Read(Box::new(RotData {
            active: RotSlot::A,
            caboose_a: Fetched::NotRead,
            caboose_b: Fetched::NotRead,
            stage0: Stage0Fetch::Unsupported,
            stage0next: Stage0Fetch::Unsupported,
            image_errors: None,
        }));
        let data = fetched_data(
            sp_state(
                "serial",
                RotState::V2 {
                    active: RotSlot::A,
                    persistent_boot_preference: RotSlot::A,
                    pending_persistent_boot_preference: None,
                    transient_boot_preference: None,
                    slot_a_sha3_256_digest: None,
                    slot_b_sha3_256_digest: None,
                },
            ),
            rot,
        );

        let ct_inv::RotInfo::Read {
            slot_a, slot_b, stage0, stage0next, ..
        } = rot_info_to_ct(Some(&data))
        else {
            panic!("expected a read RoT");
        };
        assert_eq!(slot_a.validity, ct_inv::RotImageValidity::Unsupported);
        assert_eq!(slot_b.validity, ct_inv::RotImageValidity::Unsupported);
        assert_eq!(stage0.validity, ct_inv::RotImageValidity::Unsupported);
        assert_eq!(stage0next.validity, ct_inv::RotImageValidity::Unsupported);
    }

    #[test]
    fn transceiver_status_projects_flags() {
        // Simulate the situation where power is enabled but never comes up.
        // The fault flags indicate why enabled is true but power_good is false.
        assert_eq!(
            transceiver_status_to_ct(Ok(ExtendedStatus::PRESENT
                | ExtendedStatus::ENABLED
                | ExtendedStatus::FAULT_POWER_TIMEOUT)),
            ct_inv::TransceiverStatus::Read {
                present: true,
                enabled: true,
                power_good: false,
                fault_power_timeout: true,
                fault_power_lost: false,
                disabled_by_sp: false,
                in_reset: false,
                low_power_mode: false,
                interrupt: false,
            },
        );

        let flags = |status| match transceiver_status_to_ct(Ok(status)) {
            ct_inv::TransceiverStatus::Read {
                present,
                enabled,
                power_good,
                fault_power_timeout,
                fault_power_lost,
                disabled_by_sp,
                in_reset,
                low_power_mode,
                interrupt,
            } => vec![
                present,
                enabled,
                power_good,
                fault_power_timeout,
                fault_power_lost,
                disabled_by_sp,
                in_reset,
                low_power_mode,
                interrupt,
            ],
            other => panic!("a read status projects to Read, got {other:?}"),
        };
        let bits = [
            ExtendedStatus::PRESENT,
            ExtendedStatus::ENABLED,
            ExtendedStatus::POWER_GOOD,
            ExtendedStatus::FAULT_POWER_TIMEOUT,
            ExtendedStatus::FAULT_POWER_LOST,
            ExtendedStatus::DISABLED_BY_SP,
            ExtendedStatus::RESET,
            ExtendedStatus::LOW_POWER_MODE,
            ExtendedStatus::INTERRUPT,
        ];
        for (index, bit) in bits.into_iter().enumerate() {
            let mut expected = vec![false; bits.len()];
            expected[index] = true;
            assert_eq!(
                flags(bit),
                expected,
                "{bit:?} projects to field {index} and nothing else",
            );
        }

        assert_eq!(
            transceiver_status_to_ct(Err("status read failed".to_string())),
            ct_inv::TransceiverStatus::Error {
                message: "status read failed".to_string(),
            },
        );
    }

    #[test]
    fn transceiver_vendor_projects_identity() {
        assert_eq!(
            transceiver_vendor_to_ct(Ok(sample_vendor_info())),
            ct_inv::TransceiverVendor::Read {
                name: "Acme".to_string(),
                part: "PN-1".to_string(),
                serial: "SN-1".to_string(),
            },
        );
        assert_eq!(
            transceiver_vendor_to_ct(Err("vendor read failed".to_string())),
            ct_inv::TransceiverVendor::Error {
                message: "vendor read failed".to_string(),
            },
        );
    }

    #[test]
    fn transceiver_monitors_projects_power() {
        let monitors = Monitors {
            receiver_power: Some(vec![
                ReceiverPower::Average(1.5),
                ReceiverPower::PeakToPeak(2.5),
            ]),
            transmitter_power: Some(vec![3.5, 4.5]),
            ..Default::default()
        };
        assert_eq!(
            transceiver_monitors_to_ct(Ok(monitors)),
            ct_inv::TransceiverMonitors::Read {
                lanes: id_ord_map! {
                    ct_inv::LaneMonitors {
                        lane: 0,
                        rx_power: Some(ct_inv::ReceiverPower::Average {
                            value_mw: 1.5,
                        }),
                        tx_power_mw: Some(3.5),
                    },
                    ct_inv::LaneMonitors {
                        lane: 1,
                        rx_power: Some(ct_inv::ReceiverPower::PeakToPeak {
                            value_mw: 2.5,
                        }),
                        tx_power_mw: Some(4.5),
                    },
                },
            },
            "average and peak-to-peak readings stay distinguishable, and each \
             stays attached to its own lane",
        );

        assert_eq!(
            transceiver_monitors_to_ct(Ok(Monitors {
                receiver_power: Some(vec![ReceiverPower::Average(1.5)]),
                transmitter_power: None,
                ..Default::default()
            })),
            ct_inv::TransceiverMonitors::Read {
                lanes: id_ord_map! {
                    ct_inv::LaneMonitors {
                        lane: 0,
                        rx_power: Some(ct_inv::ReceiverPower::Average {
                            value_mw: 1.5,
                        }),
                        tx_power_mw: None,
                    },
                },
            },
            "a module reporting received power but not transmitted power",
        );
        assert_eq!(
            transceiver_monitors_to_ct(Ok(Monitors {
                receiver_power: None,
                transmitter_power: Some(vec![3.5]),
                ..Default::default()
            })),
            ct_inv::TransceiverMonitors::Read {
                lanes: id_ord_map! {
                    ct_inv::LaneMonitors {
                        lane: 0,
                        rx_power: None,
                        tx_power_mw: Some(3.5),
                    },
                },
            },
            "a module reporting transmitted power but not received power",
        );

        assert_eq!(
            transceiver_monitors_to_ct(Err("monitors read failed".to_string())),
            ct_inv::TransceiverMonitors::Error {
                message: "monitors read failed".to_string(),
            },
        );
    }

    #[test]
    fn transceiver_monitors_distinguish_unsupported_from_no_lanes() {
        assert_eq!(
            transceiver_monitors_to_ct(Ok(Monitors::default())),
            ct_inv::TransceiverMonitors::Unsupported,
            "a module implementing neither monitor is unsupported, not an \
             empty read",
        );

        // A module that does implement a monitor but reports no lanes for it is
        // marked as Read.
        assert_eq!(
            transceiver_monitors_to_ct(Ok(Monitors {
                receiver_power: Some(Vec::new()),
                transmitter_power: None,
                ..Default::default()
            })),
            ct_inv::TransceiverMonitors::Read { lanes: IdOrdMap::new() },
            "an implemented monitor reporting zero lanes is still a read",
        );
    }

    #[test]
    fn transceiver_monitors_reject_mismatched_lane_counts() {
        let err = LaneMonitorsError::MismatchedLaneCounts {
            rx_lanes: 2,
            tx_lanes: 1,
        };
        assert_eq!(
            lane_monitors_to_ct(ReportedPower::Both {
                rx: vec![
                    ReceiverPower::Average(1.5),
                    ReceiverPower::Average(2.5),
                ],
                tx: vec![3.5],
            }),
            Err(err.clone()),
        );
        assert_eq!(
            transceiver_monitors_to_ct(Ok(Monitors {
                receiver_power: Some(vec![
                    ReceiverPower::Average(1.5),
                    ReceiverPower::Average(2.5),
                ]),
                transmitter_power: Some(vec![3.5]),
                ..Default::default()
            })),
            ct_inv::TransceiverMonitors::Error { message: err.to_string() },
            "the mismatch reaches the wire as a read error on the monitors",
        );
    }

    #[test]
    fn transceiver_monitors_reject_more_lanes_than_a_lane_number_holds() {
        let too_many = usize::from(u8::MAX) + 1;
        let err = LaneMonitorsError::TooManyLanes { lanes: too_many };
        assert_eq!(
            lane_monitors_to_ct(ReportedPower::Rx(vec![
                ReceiverPower::Average(
                    1.5
                );
                too_many
            ])),
            Err(err.clone()),
        );
        assert_eq!(
            transceiver_monitors_to_ct(Ok(Monitors {
                receiver_power: Some(vec![
                    ReceiverPower::Average(1.5);
                    too_many
                ]),
                transmitter_power: None,
                ..Default::default()
            })),
            ct_inv::TransceiverMonitors::Error { message: err.to_string() },
            "an unrepresentable lane count reaches the wire as a read error",
        );
    }

    #[test]
    fn transceiver_datapath_projects_sff8636_lanes() {
        let lanes = [
            Sff8636Datapath { rx_los: true, ..Default::default() },
            Sff8636Datapath { tx_fault: true, ..Default::default() },
            Sff8636Datapath::default(),
            Sff8636Datapath {
                tx_lol: true,
                rx_lol: true,
                ..Default::default()
            },
        ];
        let datapath = Datapath::Sff8636 {
            connector: ConnectorType::Unknown,
            specification: SffComplianceCode::new(0x04, 0),
            lanes,
        };
        let projected = transceiver_datapath_to_ct(Ok(datapath));
        let ct_inv::TransceiverDatapath::Sff8636 { lanes } = projected else {
            panic!("expected Sff8636 datapath, got {projected:?}");
        };
        assert_eq!(
            lanes,
            [
                ct_inv::Sff8636LaneFaults {
                    rx_los: true,
                    tx_los: false,
                    rx_lol: false,
                    tx_lol: false,
                    tx_fault: false,
                },
                ct_inv::Sff8636LaneFaults {
                    rx_los: false,
                    tx_los: false,
                    rx_lol: false,
                    tx_lol: false,
                    tx_fault: true,
                },
                ct_inv::Sff8636LaneFaults {
                    rx_los: false,
                    tx_los: false,
                    rx_lol: false,
                    tx_lol: false,
                    tx_fault: false,
                },
                ct_inv::Sff8636LaneFaults {
                    rx_los: false,
                    tx_los: false,
                    rx_lol: true,
                    tx_lol: true,
                    tx_fault: false,
                },
            ],
        );
    }

    #[test]
    fn cmis_lane_projects_fault_flags_and_state() {
        let lane = CmisLaneStatus {
            state: CmisDatapathState::TxTurnOn,
            tx_failure: Some(true),
            tx_los: Some(false),
            rx_los: None,
            rx_lol: Some(true),
            tx_lol: None,
            ..sample_cmis_lane()
        };
        assert_eq!(
            cmis_lane_to_ct(7, lane),
            ct_inv::CmisLaneStatus {
                lane: 7,
                state: ct_inv::CmisDatapathState::TxTurnOn,
                rx_los: ct_inv::FaultFlag::Unsupported,
                tx_los: ct_inv::FaultFlag::Clear,
                rx_lol: ct_inv::FaultFlag::Asserted,
                tx_lol: ct_inv::FaultFlag::Unsupported,
                tx_fault: ct_inv::FaultFlag::Asserted,
            },
        );
    }

    #[test]
    fn cmis_datapath_state_maps_all_variants() {
        let cases = [
            (
                CmisDatapathState::Deactivated,
                ct_inv::CmisDatapathState::Deactivated,
            ),
            (CmisDatapathState::Init, ct_inv::CmisDatapathState::Init),
            (CmisDatapathState::Deinit, ct_inv::CmisDatapathState::Deinit),
            (
                CmisDatapathState::Activated,
                ct_inv::CmisDatapathState::Activated,
            ),
            (CmisDatapathState::TxTurnOn, ct_inv::CmisDatapathState::TxTurnOn),
            (
                CmisDatapathState::TxTurnOff,
                ct_inv::CmisDatapathState::TxTurnOff,
            ),
            (
                CmisDatapathState::Initialized,
                ct_inv::CmisDatapathState::Initialized,
            ),
        ];
        for (internal, expected) in cases {
            assert_eq!(cmis_datapath_state_to_ct(internal), expected);
        }
    }

    #[test]
    fn transceiver_datapath_projects_cmis_preserving_keys() {
        let mut lane_status = BTreeMap::new();
        lane_status.insert(0, sample_cmis_lane());
        lane_status.insert(
            3,
            CmisLaneStatus { rx_los: Some(false), ..sample_cmis_lane() },
        );
        let mut datapaths = BTreeMap::new();
        datapaths.insert(
            2u8,
            CmisDatapath {
                application: sample_application_descriptor(),
                lane_status,
            },
        );
        let projected = transceiver_datapath_to_ct(Ok(Datapath::Cmis {
            connector: ConnectorType::Unknown,
            supported_lanes: 0x0f,
            datapaths,
        }));
        let ct_inv::TransceiverDatapath::Cmis { datapaths } = projected else {
            panic!("expected Cmis datapath, got {projected:?}");
        };
        let datapath =
            datapaths.get(&2).expect("application selector code 2 present");
        assert_eq!(datapath.application, 2);
        assert_eq!(datapath.lanes.len(), 2);
        let lane0 = datapath.lanes.get(&0).expect("lane 0 present");
        assert_eq!(lane0.lane, 0);
        assert_eq!(lane0.rx_los, ct_inv::FaultFlag::Asserted);
        let lane3 = datapath.lanes.get(&3).expect("lane 3 present");
        assert_eq!(lane3.lane, 3);
        assert_eq!(lane3.rx_los, ct_inv::FaultFlag::Clear);
    }

    #[test]
    fn transceiver_datapath_projects_empty_cmis_and_error() {
        let datapath = Datapath::Cmis {
            connector: ConnectorType::Unknown,
            supported_lanes: 0x0f,
            datapaths: BTreeMap::new(),
        };
        let projected = transceiver_datapath_to_ct(Ok(datapath));
        let ct_inv::TransceiverDatapath::Cmis { datapaths } = projected else {
            panic!("expected Cmis datapath, got {projected:?}");
        };
        assert!(datapaths.is_empty());
        assert_eq!(
            transceiver_datapath_to_ct(Err("datapath read failed".to_string())),
            ct_inv::TransceiverDatapath::Error {
                message: "datapath read failed".to_string(),
            },
        );
    }

    #[test]
    fn transceivers_unavailable_projects_empty() {
        assert!(
            transceivers_to_ct(GetTransceiversResponse::Unavailable).is_empty(),
        );
    }

    #[test]
    fn duplicate_front_port_projects_as_a_contradiction() {
        let readable = |port: &str| Transceiver {
            port: port.to_string(),
            status: Ok(ExtendedStatus::PRESENT),
            power: Ok(PowerMode {
                state: transceiver_controller::PowerState::High,
                software_override: None,
            }),
            vendor: Ok(sample_vendor_info()),
            datapath: Ok(Datapath::Sff8636 {
                connector: ConnectorType::Unknown,
                specification: SffComplianceCode::new(0x04, 0),
                lanes: [Sff8636Datapath::default(); 4],
            }),
            monitors: Ok(Monitors::default()),
        };

        let projected = transceivers_by_port(vec![
            readable("qsfp0"),
            readable("qsfp1"),
            readable("qsfp0"),
        ]);

        assert_eq!(projected.len(), 2, "one entry per port: {projected:?}");

        let qsfp1 = projected.get("qsfp1").expect("qsfp1 present");
        assert_eq!(
            qsfp1.status,
            ct_inv::TransceiverStatus::Read {
                present: true,
                enabled: false,
                power_good: false,
                fault_power_timeout: false,
                fault_power_lost: false,
                disabled_by_sp: false,
                in_reset: false,
                low_power_mode: false,
                interrupt: false,
            },
            "the port reported once is unaffected by its neighbor",
        );

        let message =
            DuplicateFrontPort { port: "qsfp0".to_string() }.to_string();
        assert_eq!(
            projected.get("qsfp0").expect("qsfp0 present"),
            &ct_inv::Transceiver {
                port: "qsfp0".to_string(),
                status: ct_inv::TransceiverStatus::Error {
                    message: message.clone(),
                },
                power: ct_inv::TransceiverPower::Error {
                    message: message.clone(),
                },
                vendor: ct_inv::TransceiverVendor::Error {
                    message: message.clone(),
                },
                monitors: ct_inv::TransceiverMonitors::Error {
                    message: message.clone(),
                },
                datapath: ct_inv::TransceiverDatapath::Error { message },
            },
        );
    }

    #[test]
    fn transceivers_response_projects_by_switch_and_port() {
        let transceiver = Transceiver {
            port: "qsfp0".to_string(),
            status: Ok(ExtendedStatus::PRESENT
                | ExtendedStatus::ENABLED
                | ExtendedStatus::POWER_GOOD),
            power: Err("power mode not read".to_string()),
            vendor: Ok(sample_vendor_info()),
            datapath: Ok(Datapath::Sff8636 {
                connector: ConnectorType::Unknown,
                specification: SffComplianceCode::new(0x04, 0),
                lanes: [Sff8636Datapath::default(); 4],
            }),
            monitors: Ok(Monitors::default()),
        };
        let response = GetTransceiversResponse::Response {
            transceivers: id_ord_map! {
                SwitchTransceivers {
                    switch: SwitchSlot::Switch0,
                    transceivers: vec![transceiver],
                    updated_at: tokio::time::Instant::now(),
                },
            },
        };

        let switches = transceivers_to_ct(response);

        let switch = switches
            .iter()
            .find(|s| s.switch == SwitchSlot::Switch0)
            .expect("switch 0 present");
        assert_eq!(switch.transceivers.len(), 1);
        let port = switch
            .transceivers
            .iter()
            .find(|t| t.port == "qsfp0")
            .expect("qsfp0 present");
        assert_eq!(
            port.status,
            ct_inv::TransceiverStatus::Read {
                present: true,
                enabled: true,
                power_good: true,
                fault_power_timeout: false,
                fault_power_lost: false,
                disabled_by_sp: false,
                in_reset: false,
                low_power_mode: false,
                interrupt: false,
            },
        );
        assert_eq!(
            port.vendor,
            ct_inv::TransceiverVendor::Read {
                name: "Acme".to_string(),
                part: "PN-1".to_string(),
                serial: "SN-1".to_string(),
            },
        );
        assert_eq!(
            port.power,
            ct_inv::TransceiverPower::Error {
                message: "power mode not read".to_string(),
            },
        );
    }

    #[test]
    fn transceiver_power_projects_modes() {
        assert_eq!(
            transceiver_power_to_ct(Err("power mode read failed".to_string())),
            ct_inv::TransceiverPower::Error {
                message: "power mode read failed".to_string(),
            },
        );

        let cases = [
            (
                transceiver_controller::PowerState::Off,
                ct_inv::TransceiverPowerMode::Off,
            ),
            (
                transceiver_controller::PowerState::Low,
                ct_inv::TransceiverPowerMode::Low,
            ),
            (
                transceiver_controller::PowerState::High,
                ct_inv::TransceiverPowerMode::High,
            ),
        ];
        for (state, expected) in cases {
            assert_eq!(
                transceiver_power_to_ct(Ok(PowerMode {
                    state,
                    software_override: Some(true),
                })),
                ct_inv::TransceiverPower::Read { mode: expected },
                "power state {state:?} projects to {expected:?}",
            );
        }
    }

    #[test]
    fn ignition_present_projects_ctrl_detect() {
        let projected = ignition_to_ct(Some(SpIgnition::Present {
            id: SpIgnitionSystemType::Gimlet,
            power: true,
            // Use distinct values for ctrl_detect_0 and ctrl_detect_1 to ensure
            // that the values aren't accidentally transposed during conversion.
            ctrl_detect_0: true,
            ctrl_detect_1: false,
            // Likewise, use distinct values for each fault flag, so an
            // accidental transposition among them (often) causes this test to
            // fail.
            //
            // TODO: this should probably be a PBT.
            flt_a3: true,
            flt_a2: false,
            flt_rot: true,
            flt_sp: false,
        }));
        assert_eq!(
            projected,
            ct_inv::SpIgnitionInfo::Present {
                power: true,
                faults: ct_inv::IgnitionFaults {
                    a3: true,
                    a2: false,
                    rot: true,
                    sp: false,
                },
                ctrl_detect: ct_inv::IgnitionControllerDetect {
                    switch0: true,
                    switch1: false,
                },
            },
        );

        let ct_inv::SpIgnitionInfo::Present { ctrl_detect, .. } = projected
        else {
            panic!("a present ignition target projects to Present");
        };
        assert!(
            ctrl_detect.detects(SwitchSlot::Switch0),
            "switch 0's controller detects this target",
        );
        assert!(
            !ctrl_detect.detects(SwitchSlot::Switch1),
            "switch 1's controller does not detect this target",
        );
    }

    #[test]
    fn rss_step_projects_one_based_index() {
        let nonzero = |n: u32| NonZeroU32::new(n).expect("value is nonzero");

        let first = rss_step_to_ct(bootstrap::RssStep::Requested);
        assert_eq!(
            first,
            RssStepInfo {
                step: nonzero(1),
                total_steps: nonzero(16),
                description: "Requested".to_string(),
            },
            "the first RSS step projects to a 1-based step 1 of 16",
        );

        let last = rss_step_to_ct(bootstrap::RssStep::NexusHandoff);
        assert_eq!(
            last,
            RssStepInfo {
                step: nonzero(16),
                total_steps: nonzero(16),
                description: "Handing off to Nexus".to_string(),
            },
            "the last RSS step projects to step 16 of 16",
        );
    }

    #[test]
    fn rack_setup_status_projects_every_variant() {
        let nonzero = |n: u32| NonZeroU32::new(n).expect("value is nonzero");
        let init_uuid: Uuid = "11111111-1111-4111-8111-111111111111"
            .parse()
            .expect("valid init uuid");
        let init_id = RackInitUuid::from_untyped_uuid(init_uuid);

        assert_eq!(
            rack_setup_status_to_ct(B::Uninitialized),
            RackSetupStatus {
                rack_state: RackState::Uninitialized,
                operation: None,
            },
            "uninitialized on startup carries no operation",
        );

        assert_eq!(
            rack_setup_status_to_ct(B::Initializing {
                id: init_id,
                step: bootstrap::RssStep::SledInit,
            }),
            RackSetupStatus {
                rack_state: RackState::Other,
                operation: Some(RackOperation {
                    kind: RackOperationKind::INITIALIZE,
                    id: init_uuid,
                    state: RackOperationState::InProgress {
                        current_step: Some(RssStepInfo {
                            step: nonzero(7),
                            total_steps: nonzero(16),
                            description: "Initializing sleds".to_string(),
                        }),
                    },
                }),
            },
            "a rack part-way through setup is neither initialized nor \
             uninitialized, and reports its current step",
        );

        assert_eq!(
            rack_setup_status_to_ct(B::InitializationFailed {
                id: init_id,
                message: "init boom".to_string(),
            }),
            RackSetupStatus {
                rack_state: RackState::Other,
                operation: Some(RackOperation {
                    kind: RackOperationKind::INITIALIZE,
                    id: init_uuid,
                    state: RackOperationState::Failed {
                        message: "init boom".to_string(),
                        failed_step: None,
                    },
                }),
            },
            "a setup that stopped part-way leaves the rack neither \
             initialized nor uninitialized, and carries its message",
        );

        assert_eq!(
            rack_setup_status_to_ct(B::InitializationPanicked { id: init_id }),
            RackSetupStatus {
                rack_state: RackState::Other,
                operation: Some(RackOperation {
                    kind: RackOperationKind::INITIALIZE,
                    id: init_uuid,
                    state: RackOperationState::Panicked,
                }),
            },
            "a panicked setup leaves the rack part-way through, same as a \
             failed one",
        );

        assert_eq!(
            rack_setup_status_to_ct(B::Initialized { id: Some(init_id) }),
            RackSetupStatus {
                rack_state: RackState::Initialized,
                operation: Some(RackOperation {
                    kind: RackOperationKind::INITIALIZE,
                    id: init_uuid,
                    state: RackOperationState::Completed,
                }),
            },
            "an initialized rack from a completed init carries that operation",
        );

        assert_eq!(
            rack_setup_status_to_ct(B::Initialized { id: None }),
            RackSetupStatus {
                rack_state: RackState::Initialized,
                operation: None,
            },
            "a rack initialized on startup carries no operation",
        );
    }
}
