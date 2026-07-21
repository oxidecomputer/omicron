// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Conversions between internal and commission types.

use bootstrap_agent_lockstep_types as bootstrap;
use iddqd::IdOrdMap;
use sled_agent_types::early_networking::SwitchSlot;
use transceiver_controller::message::ExtendedStatus;
use transceiver_controller::{
    CmisDatapath, CmisDatapathState, CmisLaneStatus, Datapath, Monitors,
    Sff8636Datapath, Vendor, VendorInfo,
};
use wicket_common::inventory::{
    RotInventory, SpComponentCaboose, SpIgnition, SpInventory, Transceiver,
};
use wicketd_commission_types::inventory as ct_inv;
use wicketd_commission_types::rack_setup::{
    NewPasswordHash, RackOperationStatus, RssStepInfo,
};
use wicketd_commission_types::update::StartUpdateOptions;

use crate::transceivers::GetTransceiversResponse;

fn caboose_to_ct(caboose: SpComponentCaboose) -> ct_inv::Caboose {
    let SpComponentCaboose {
        version,
        board,
        sign,
        // The remaining fields are not projected by the commission API.
        git_commit: _,
        name: _,
        epoch: _,
    } = caboose;
    ct_inv::Caboose { version, board, sign }
}

fn slot_caboose_to_ct(
    caboose: Option<SpComponentCaboose>,
) -> ct_inv::SlotCaboose {
    match caboose {
        None => ct_inv::SlotCaboose::NotRead,
        Some(caboose) => {
            ct_inv::SlotCaboose::Read { caboose: caboose_to_ct(caboose) }
        }
    }
}

fn stage0_caboose_to_ct(
    caboose: Option<Option<SpComponentCaboose>>,
) -> ct_inv::Stage0Caboose {
    match caboose {
        None => ct_inv::Stage0Caboose::Unsupported,
        Some(None) => ct_inv::Stage0Caboose::NotRead,
        Some(Some(caboose)) => {
            ct_inv::Stage0Caboose::Read { caboose: caboose_to_ct(caboose) }
        }
    }
}

fn rot_info_to_ct(rot: RotInventory) -> ct_inv::RotInfo {
    let RotInventory {
        active,
        caboose_a,
        caboose_b,
        caboose_stage0,
        caboose_stage0next,
    } = rot;
    ct_inv::RotInfo {
        active,
        caboose_a: slot_caboose_to_ct(caboose_a),
        caboose_b: slot_caboose_to_ct(caboose_b),
        caboose_stage0: stage0_caboose_to_ct(caboose_stage0),
        caboose_stage0next: stage0_caboose_to_ct(caboose_stage0next),
    }
}

fn ignition_to_ct(ignition: Option<SpIgnition>) -> ct_inv::SpIgnitionInfo {
    match ignition {
        None => ct_inv::SpIgnitionInfo::NotRead,
        Some(SpIgnition::Absent) => ct_inv::SpIgnitionInfo::Absent,
        Some(SpIgnition::Present {
            power,
            flt_a3,
            flt_a2,
            flt_rot,
            flt_sp,
            // The remaining fields are not projected by the commission API.
            id: _,
            ctrl_detect_0: _,
            ctrl_detect_1: _,
        }) => ct_inv::SpIgnitionInfo::Present {
            power,
            faults: ct_inv::IgnitionFaults {
                a3: flt_a3,
                a2: flt_a2,
                rot: flt_rot,
                sp: flt_sp,
            },
        },
    }
}

pub(crate) fn sp_info_to_ct(sp: SpInventory) -> ct_inv::SpInfo {
    let SpInventory {
        id,
        ignition,
        state,
        caboose_active,
        caboose_inactive,
        rot,
        // The raw MGS component list is not projected by the commission API.
        components: _,
    } = sp;
    ct_inv::SpInfo {
        id,
        state: state.map(|s| ct_inv::SpStateInfo {
            serial_number: s.serial_number,
            power_state: s.power_state,
        }),
        ignition: ignition_to_ct(ignition),
        caboose_active: slot_caboose_to_ct(caboose_active),
        caboose_inactive: slot_caboose_to_ct(caboose_inactive),
        rot: rot.map(rot_info_to_ct),
    }
}

pub(crate) fn transceivers_to_ct(
    response: GetTransceiversResponse,
) -> ct_inv::TransceiverInventory {
    match response {
        GetTransceiversResponse::Unavailable => {
            ct_inv::TransceiverInventory::NotRead
        }
        GetTransceiversResponse::Response {
            transceivers,
            transceivers_last_seen,
        } => {
            let switches = IdOrdMap::from_iter_unique(
                transceivers.into_iter().map(|(switch, transceivers)| {
                    switch_transceivers_to_ct(switch, transceivers)
                }),
            )
            .expect(
                "the internal transceiver inventory is keyed by SwitchSlot, so \
                 the projected SwitchTransceivers have unique switch keys",
            );
            ct_inv::TransceiverInventory::Read {
                last_seen: transceivers_last_seen,
                switches,
            }
        }
    }
}

fn switch_transceivers_to_ct(
    switch: SwitchSlot,
    transceivers: Vec<Transceiver>,
) -> ct_inv::SwitchTransceivers {
    let transceivers = IdOrdMap::from_iter_unique(
        transceivers.into_iter().map(transceiver_to_ct),
    )
    .expect(
        "a switch reports at most one transceiver per front port, so the \
         projected Transceivers have unique port keys",
    );
    ct_inv::SwitchTransceivers { switch, transceivers }
}

fn transceiver_to_ct(transceiver: Transceiver) -> ct_inv::Transceiver {
    let Transceiver {
        port,
        status,
        vendor,
        datapath,
        monitors,
        // The power mode is not projected by the commission API.
        power: _,
    } = transceiver;
    ct_inv::Transceiver {
        port,
        status: transceiver_status_to_ct(status),
        vendor: transceiver_vendor_to_ct(vendor),
        monitors: transceiver_monitors_to_ct(monitors),
        datapath: transceiver_datapath_to_ct(datapath),
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
        },
    }
}

fn transceiver_vendor_to_ct(
    vendor: Result<VendorInfo, String>,
) -> ct_inv::TransceiverVendor {
    match vendor {
        Err(message) => ct_inv::TransceiverVendor::Error { message },
        Ok(VendorInfo {
            vendor: Vendor {
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
        }) => ct_inv::TransceiverMonitors::Read {
            rx_power_mw: receiver_power.map(|powers| {
                powers.into_iter().map(|power| power.value()).collect()
            }),
            tx_power_mw: transmitter_power,
        },
    }
}

fn transceiver_datapath_to_ct(
    datapath: Result<Datapath, String>,
) -> ct_inv::TransceiverDatapath {
    match datapath {
        Err(message) => ct_inv::TransceiverDatapath::Error { message },
        Ok(Datapath::Sff8636 {
            lanes,
            connector: _,
            specification: _,
        }) => {
            let lanes = lanes.map(sff8636_lane_to_ct);
            ct_inv::TransceiverDatapath::Sff8636 { lanes }
        }
        Ok(Datapath::Cmis {
            datapaths,
            connector: _,
            supported_lanes: _,
        }) => {
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

fn cmis_lane_to_ct(
    lane: u8,
    status: CmisLaneStatus,
) -> ct_inv::CmisLaneStatus {
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
    RssStepInfo {
        // index() is 0-based, so add 1 to get the 1-based step index.
        step: step.index() as u32 + 1,
        total_steps: step.max_step() as u32,
        description: step.description().to_string(),
    }
}

pub(crate) fn rack_operation_status_to_ct(
    status: bootstrap::RackOperationStatus,
) -> RackOperationStatus {
    use bootstrap::RackOperationStatus as B;
    match status {
        B::Initializing { id, step } => {
            RackOperationStatus::Initializing { id, step: rss_step_to_ct(step) }
        }
        B::Initialized { id } => RackOperationStatus::Initialized { id },
        B::InitializationFailed { id, message } => {
            RackOperationStatus::InitializationFailed { id, message }
        }
        B::InitializationPanicked { id } => {
            RackOperationStatus::InitializationPanicked { id }
        }
        B::Resetting { id } => RackOperationStatus::Resetting { id },
        B::Uninitialized { reset_id } => {
            RackOperationStatus::Uninitialized { reset_id }
        }
        B::ResetFailed { id, message } => {
            RackOperationStatus::ResetFailed { id, message }
        }
        B::ResetPanicked { id } => RackOperationStatus::ResetPanicked { id },
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
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;
    use transceiver_controller::{
        ApplicationDescriptor, CmisDatapathState, ConnectorType,
        HostElectricalInterfaceId, Identifier, MediaInterfaceId, MediaType, Oui,
        OutputStatus, ReceiverPower, SffComplianceCode,
    };

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
    fn caboose_projects_sign() {
        let with_sign = SpComponentCaboose {
            git_commit: "abc".to_string(),
            board: "gimlet".to_string(),
            name: "sp".to_string(),
            version: "1.0.0".to_string(),
            sign: Some("deadbeef".to_string()),
            epoch: None,
        };
        assert_eq!(
            caboose_to_ct(with_sign),
            ct_inv::Caboose {
                version: "1.0.0".to_string(),
                board: "gimlet".to_string(),
                sign: Some("deadbeef".to_string()),
            },
        );

        let without_sign = SpComponentCaboose {
            git_commit: "abc".to_string(),
            board: "gimlet".to_string(),
            name: "sp".to_string(),
            version: "1.0.0".to_string(),
            sign: None,
            epoch: None,
        };
        assert_eq!(caboose_to_ct(without_sign).sign, None);
    }

    #[test]
    fn transceiver_status_projects_flags() {
        assert_eq!(
            transceiver_status_to_ct(Ok(ExtendedStatus::PRESENT
                | ExtendedStatus::POWER_GOOD)),
            ct_inv::TransceiverStatus::Read {
                present: true,
                enabled: false,
                power_good: true,
            },
        );
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
            transmitter_power: Some(vec![3.5]),
            ..Default::default()
        };
        assert_eq!(
            transceiver_monitors_to_ct(Ok(monitors)),
            ct_inv::TransceiverMonitors::Read {
                rx_power_mw: Some(vec![1.5, 2.5]),
                tx_power_mw: Some(vec![3.5]),
            },
        );

        // A module reporting no power monitoring stays None.
        assert_eq!(
            transceiver_monitors_to_ct(Ok(Monitors::default())),
            ct_inv::TransceiverMonitors::Read {
                rx_power_mw: None,
                tx_power_mw: None,
            },
        );

        assert_eq!(
            transceiver_monitors_to_ct(Err("monitors read failed".to_string())),
            ct_inv::TransceiverMonitors::Error {
                message: "monitors read failed".to_string(),
            },
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
            (CmisDatapathState::Deactivated, ct_inv::CmisDatapathState::Deactivated),
            (CmisDatapathState::Init, ct_inv::CmisDatapathState::Init),
            (CmisDatapathState::Deinit, ct_inv::CmisDatapathState::Deinit),
            (CmisDatapathState::Activated, ct_inv::CmisDatapathState::Activated),
            (CmisDatapathState::TxTurnOn, ct_inv::CmisDatapathState::TxTurnOn),
            (CmisDatapathState::TxTurnOff, ct_inv::CmisDatapathState::TxTurnOff),
            (CmisDatapathState::Initialized, ct_inv::CmisDatapathState::Initialized),
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
    fn transceivers_unavailable_is_not_read() {
        assert_eq!(
            transceivers_to_ct(GetTransceiversResponse::Unavailable),
            ct_inv::TransceiverInventory::NotRead,
        );
    }

    #[test]
    fn transceivers_response_projects_by_switch_and_port() {
        let transceiver = Transceiver {
            port: "qsfp0".to_string(),
            status: Ok(ExtendedStatus::PRESENT
                | ExtendedStatus::ENABLED
                | ExtendedStatus::POWER_GOOD),
            // The power mode is not included in the projection.
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
            transceivers: HashMap::from([(
                SwitchSlot::Switch0,
                vec![transceiver],
            )]),
            transceivers_last_seen: Duration::from_secs(3),
        };

        let projected = transceivers_to_ct(response);
        let ct_inv::TransceiverInventory::Read { last_seen, switches } =
            projected
        else {
            panic!("expected Read inventory, got {projected:?}");
        };
        assert_eq!(last_seen, Duration::from_secs(3));

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
    }
}
