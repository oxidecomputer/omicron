// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::Ipv6Addr;

use crate::v1::inventory::{
    BaseboardId, BootstrapSledState, CmisLaneStatus, FaultFlag,
    GetBootstrapSledsResponse, IgnitionControllerDetect, RotInfo,
    Sff8636LaneFaults, SlotCaboose, SpIdentifier, SpStateInfo, Stage0Caboose,
    SwitchSlot, TransceiverDatapath,
};

impl BootstrapSledState {
    /// Returns true if this sled's state was read from MGS.
    pub fn is_read(&self) -> bool {
        match self {
            BootstrapSledState::Read { .. } => true,
            BootstrapSledState::NotRead | BootstrapSledState::Error { .. } => {
                false
            }
        }
    }
}

/// A cubby from a bootstrap-sleds response whose sled was successfully read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadableBootstrapSled<'a> {
    /// The service processor for this cubby.
    pub id: SpIdentifier,
    /// The sled's baseboard identity.
    pub baseboard: &'a BaseboardId,
    /// The sled's bootstrap-network address, once it has been discovered.
    pub ip: Option<Ipv6Addr>,
}

impl GetBootstrapSledsResponse {
    /// Iterates over just the cubbies whose sled was successfully read.
    ///
    /// These are the sleds that can be commissioned. Cubbies whose sled could
    /// not be read are skipped, so a caller that only wants usable sleds does
    /// not have to remember that `sleds` also carries unreadable ones.
    pub fn readable(&self) -> impl Iterator<Item = ReadableBootstrapSled<'_>> {
        self.sleds.iter().filter_map(|sled| match &sled.state {
            BootstrapSledState::Read { baseboard, ip } => {
                Some(ReadableBootstrapSled { id: sled.id, baseboard, ip: *ip })
            }
            BootstrapSledState::NotRead | BootstrapSledState::Error { .. } => {
                None
            }
        })
    }
}

impl IgnitionControllerDetect {
    pub fn detects(&self, switch: SwitchSlot) -> bool {
        match switch {
            SwitchSlot::Switch0 => self.switch0,
            SwitchSlot::Switch1 => self.switch1,
        }
    }
}

impl SpStateInfo {
    /// Returns true if the service processor's state was read.
    pub fn is_read(&self) -> bool {
        match self {
            SpStateInfo::Read { .. } => true,
            SpStateInfo::NotRead | SpStateInfo::Error { .. } => false,
        }
    }
}

impl RotInfo {
    /// Returns true if the root of trust was read.
    pub fn is_read(&self) -> bool {
        match self {
            RotInfo::Read { .. } => true,
            RotInfo::NotRead | RotInfo::Error { .. } => false,
        }
    }
}

impl SlotCaboose {
    /// Returns true if this slot's caboose was read.
    pub fn is_read(&self) -> bool {
        match self {
            SlotCaboose::Read { .. } => true,
            SlotCaboose::NotRead | SlotCaboose::Error { .. } => false,
        }
    }
}

impl Stage0Caboose {
    /// Returns true if the stage0 caboose was read.
    pub fn is_read(&self) -> bool {
        match self {
            Stage0Caboose::Read { .. } => true,
            Stage0Caboose::Unsupported
            | Stage0Caboose::NotRead
            | Stage0Caboose::Error { .. } => false,
        }
    }
}

impl FaultFlag {
    pub fn is_asserted(&self) -> bool {
        match self {
            FaultFlag::Asserted => true,
            FaultFlag::Clear | FaultFlag::Unsupported => false,
        }
    }

    fn from_sff(flag: bool) -> Self {
        if flag { FaultFlag::Asserted } else { FaultFlag::Clear }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LaneFaultsView {
    /// The CMIS application selector code this lane belongs to.
    ///
    /// `None` for an SFF-8636 module, which has no notion of applications.
    /// CMIS lane numbers are unique only within a datapath, so this is needed
    /// alongside `lane` to identify a lane on such a module.
    pub application: Option<u8>,
    /// The lane number within its datapath.
    pub lane: u8,
    pub rx_los: FaultFlag,
    pub tx_los: FaultFlag,
    pub rx_lol: FaultFlag,
    pub tx_lol: FaultFlag,
    pub tx_fault: FaultFlag,
}

impl LaneFaultsView {
    fn from_sff8636(index: u8, lane: &Sff8636LaneFaults) -> Self {
        let Sff8636LaneFaults { rx_los, tx_los, rx_lol, tx_lol, tx_fault } =
            lane;
        LaneFaultsView {
            application: None,
            lane: index,
            rx_los: FaultFlag::from_sff(*rx_los),
            tx_los: FaultFlag::from_sff(*tx_los),
            rx_lol: FaultFlag::from_sff(*rx_lol),
            tx_lol: FaultFlag::from_sff(*tx_lol),
            tx_fault: FaultFlag::from_sff(*tx_fault),
        }
    }

    fn from_cmis(application: u8, status: &CmisLaneStatus) -> Self {
        let CmisLaneStatus {
            rx_los,
            tx_los,
            rx_lol,
            tx_lol,
            tx_fault,
            lane,
            state: _,
        } = status;
        LaneFaultsView {
            application: Some(application),
            lane: *lane,
            rx_los: *rx_los,
            tx_los: *tx_los,
            rx_lol: *rx_lol,
            tx_lol: *tx_lol,
            tx_fault: *tx_fault,
        }
    }
}

impl TransceiverDatapath {
    /// Iterates over the fault flags of every lane in this datapath.
    ///
    /// Returns an error carrying the read-failure message if the datapath
    /// state could not be read at all, so that an unreadable module is not
    /// mistaken for a fault-free one.
    pub fn iter_lane_faults(
        &self,
    ) -> Result<impl Iterator<Item = LaneFaultsView> + '_, &str> {
        let views: Vec<LaneFaultsView> = match self {
            TransceiverDatapath::Error { message } => {
                return Err(message.as_str());
            }
            // TODO-RAINCLAUDE: SFF-8636 has no lane ids, so the array index is the lane number; zipping a u8 range keeps that a conversion-free fact
            TransceiverDatapath::Sff8636 { lanes } => lanes
                .iter()
                .zip(0u8..)
                .map(|(lane, index)| LaneFaultsView::from_sff8636(index, lane))
                .collect(),
            TransceiverDatapath::Cmis { datapaths } => datapaths
                .iter()
                .flat_map(|datapath| {
                    datapath.lanes.iter().map(|status| {
                        LaneFaultsView::from_cmis(datapath.application, status)
                    })
                })
                .collect(),
        };
        Ok(views.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::inventory::{CmisDatapath, CmisDatapathState};
    use iddqd::id_ord_map;

    #[test]
    fn fault_flag_is_asserted() {
        assert!(FaultFlag::Asserted.is_asserted());
        assert!(!FaultFlag::Clear.is_asserted());
        assert!(!FaultFlag::Unsupported.is_asserted());
    }

    #[test]
    fn iter_lane_faults_error_returns_message() {
        let datapath =
            TransceiverDatapath::Error { message: "boom".to_string() };
        assert_eq!(datapath.iter_lane_faults().err(), Some("boom"));
    }

    #[test]
    fn iter_lane_faults_sff8636_lifts_bools() {
        let datapath = TransceiverDatapath::Sff8636 {
            lanes: [
                Sff8636LaneFaults {
                    rx_los: true,
                    tx_los: false,
                    rx_lol: false,
                    tx_lol: false,
                    tx_fault: false,
                },
                Sff8636LaneFaults {
                    rx_los: false,
                    tx_los: false,
                    rx_lol: false,
                    tx_lol: false,
                    tx_fault: true,
                },
                Sff8636LaneFaults {
                    rx_los: false,
                    tx_los: false,
                    rx_lol: false,
                    tx_lol: false,
                    tx_fault: false,
                },
                Sff8636LaneFaults {
                    rx_los: false,
                    tx_los: true,
                    rx_lol: false,
                    tx_lol: false,
                    tx_fault: false,
                },
            ],
        };
        let views: Vec<_> =
            datapath.iter_lane_faults().expect("datapath was read").collect();
        assert_eq!(views.len(), 4);
        // SFF-8636 lanes are identified by position and their application is
        // always None.
        let ids: Vec<_> =
            views.iter().map(|v| (v.application, v.lane)).collect();
        assert_eq!(ids, vec![(None, 0), (None, 1), (None, 2), (None, 3)]);
        assert_eq!(views[0].rx_los, FaultFlag::Asserted);
        assert_eq!(views[0].tx_los, FaultFlag::Clear);
        assert_eq!(views[1].tx_fault, FaultFlag::Asserted);
        assert_eq!(views[3].tx_los, FaultFlag::Asserted);
    }

    #[test]
    fn iter_lane_faults_cmis_passes_through_flags() {
        let lane = |lane: u8, rx_los: FaultFlag| CmisLaneStatus {
            lane,
            state: CmisDatapathState::Activated,
            rx_los,
            tx_los: FaultFlag::Clear,
            rx_lol: FaultFlag::Unsupported,
            tx_lol: FaultFlag::Clear,
            tx_fault: FaultFlag::Asserted,
        };
        let datapaths = id_ord_map! {
            CmisDatapath {
                application: 1,
                lanes: id_ord_map! {
                    lane(0, FaultFlag::Asserted),
                    lane(1, FaultFlag::Clear),
                },
            },
            CmisDatapath {
                application: 2,
                lanes: id_ord_map! { lane(0, FaultFlag::Unsupported) },
            },
        };
        let datapath = TransceiverDatapath::Cmis { datapaths };
        let views: Vec<_> =
            datapath.iter_lane_faults().expect("datapath was read").collect();
        assert_eq!(views.len(), 3);
        // Note that two of these lanes are both numbered 0, so the application
        // is part of the unique identifier.
        let rx_los: Vec<_> =
            views.iter().map(|v| (v.application, v.lane, v.rx_los)).collect();
        assert_eq!(
            rx_los,
            vec![
                (Some(1), 0, FaultFlag::Asserted),
                (Some(1), 1, FaultFlag::Clear),
                (Some(2), 0, FaultFlag::Unsupported),
            ],
            "each flag stays attached to the lane it came from",
        );
        assert!(views.iter().all(|v| v.rx_lol == FaultFlag::Unsupported));
        assert!(views.iter().all(|v| v.tx_fault == FaultFlag::Asserted));
    }
}
