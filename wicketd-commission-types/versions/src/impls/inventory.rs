// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v1::inventory::{
    CmisLaneStatus, FaultFlag, RotInfo, Sff8636LaneFaults, SlotCaboose,
    SpStateInfo, Stage0Caboose, TransceiverDatapath,
};

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
    pub rx_los: FaultFlag,
    pub tx_los: FaultFlag,
    pub rx_lol: FaultFlag,
    pub tx_lol: FaultFlag,
    pub tx_fault: FaultFlag,
}

impl LaneFaultsView {
    fn from_sff8636(lane: &Sff8636LaneFaults) -> Self {
        let Sff8636LaneFaults { rx_los, tx_los, rx_lol, tx_lol, tx_fault } =
            lane;
        LaneFaultsView {
            rx_los: FaultFlag::from_sff(*rx_los),
            tx_los: FaultFlag::from_sff(*tx_los),
            rx_lol: FaultFlag::from_sff(*rx_lol),
            tx_lol: FaultFlag::from_sff(*tx_lol),
            tx_fault: FaultFlag::from_sff(*tx_fault),
        }
    }

    fn from_cmis(lane: &CmisLaneStatus) -> Self {
        let CmisLaneStatus {
            rx_los,
            tx_los,
            rx_lol,
            tx_lol,
            tx_fault,
            lane: _,
            state: _,
        } = lane;
        LaneFaultsView {
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
            TransceiverDatapath::Sff8636 { lanes } => {
                lanes.iter().map(LaneFaultsView::from_sff8636).collect()
            }
            TransceiverDatapath::Cmis { datapaths } => datapaths
                .iter()
                .flat_map(|datapath| datapath.lanes.iter())
                .map(LaneFaultsView::from_cmis)
                .collect(),
        };
        Ok(views.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::inventory::{CmisDatapath, CmisDatapathState};
    use iddqd::IdOrdMap;

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
        let datapaths: IdOrdMap<CmisDatapath> = [
            CmisDatapath {
                application: 1,
                lanes: [
                    lane(0, FaultFlag::Asserted),
                    lane(1, FaultFlag::Clear),
                ]
                .into_iter()
                .collect(),
            },
            CmisDatapath {
                application: 2,
                lanes: [lane(0, FaultFlag::Unsupported)].into_iter().collect(),
            },
        ]
        .into_iter()
        .collect();
        let datapath = TransceiverDatapath::Cmis { datapaths };
        let views: Vec<_> =
            datapath.iter_lane_faults().expect("datapath was read").collect();
        assert_eq!(views.len(), 3);
        assert!(views.iter().all(|v| v.rx_lol == FaultFlag::Unsupported));
        assert!(views.iter().all(|v| v.tx_fault == FaultFlag::Asserted));
        let rx_los: Vec<_> = views.iter().map(|v| v.rx_los).collect();
        assert!(rx_los.contains(&FaultFlag::Asserted));
        assert!(rx_los.contains(&FaultFlag::Clear));
        assert!(rx_los.contains(&FaultFlag::Unsupported));
    }
}
