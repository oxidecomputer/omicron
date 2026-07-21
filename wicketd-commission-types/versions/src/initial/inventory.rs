// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory and discovery types for the commissioning API.
//!
//! These are deliberately minimal projections of wicketd's internal inventory
//! shapes: they carry only the fields the commissioning client needs, so that
//! the large and less stable internal inventory type graph (MGS inventory) does
//! not leak into this stable API.

use std::net::Ipv6Addr;
use std::time::Duration;

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Re-exports of pinned gateway types since they are also published by this API.
pub use gateway_types_versions::v1::component::{
    PowerState, SpIdentifier, SpType,
};
pub use gateway_types_versions::v1::rot::RotSlot;
pub use sled_agent_types_versions::v1::early_networking::SwitchSlot;

/// A minimal projection of a firmware caboose.
///
/// Only the fields the commissioning client needs are included; the remaining
/// caboose fields (git commit, name, epoch, and so on) are intentionally
/// omitted.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct Caboose {
    /// The firmware version string.
    pub version: String,
    /// The board name the firmware was built for.
    pub board: String,
    /// The signer of this firmware image, if the image is signed.
    ///
    /// For a root of trust, this is the hex-encoded hash of the public key used
    /// to sign the image; it is `None` for firmware that carries no signature
    /// (such as service-processor images). Commissioning uses it to match an
    /// active RoT slot against the corresponding signed TUF artifact.
    pub sign: Option<String>,
}

/// The stage0 (or pending stage0next) bootloader caboose for a root of trust.
///
/// The three states are kept distinct because they mean different things to a
/// caller:
///
/// * `unsupported`: this RoT version does not report a stage0 bootloader
///   caboose at all.
/// * `not_read`: the RoT version supports reporting a stage0 caboose, but it
///   has not been read yet.
/// * `read`: the stage0 caboose was read.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Stage0Caboose {
    /// This RoT version does not report a stage0 bootloader caboose.
    Unsupported,
    /// The stage0 bootloader caboose is supported but has not been read yet.
    NotRead,
    /// The stage0 bootloader caboose was read.
    Read {
        /// The caboose that was read.
        caboose: Caboose,
    },
}

/// The caboose for a single firmware slot, including whether it was read.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SlotCaboose {
    /// The caboose for this slot has not been read yet.
    NotRead,
    /// The caboose for this slot was read.
    Read {
        /// The caboose that was read.
        caboose: Caboose,
    },
}

/// Root-of-trust information for a single service processor.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct RotInfo {
    /// The currently-active RoT image slot.
    pub active: RotSlot,
    /// The caboose of the image in slot A.
    pub caboose_a: SlotCaboose,
    /// The caboose of the image in slot B.
    pub caboose_b: SlotCaboose,
    /// The caboose of the stage0 bootloader.
    pub caboose_stage0: Stage0Caboose,
    /// The caboose of the pending stage0next bootloader.
    pub caboose_stage0next: Stage0Caboose,
}

/// The service processor's state, read together from MGS.
///
/// The serial number and power state are always read together from the same
/// service-processor state, so they are presented as a single unit that is
/// either entirely present or entirely absent.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct SpStateInfo {
    /// The service processor's serial number.
    pub serial_number: String,
    /// The host power state.
    pub power_state: PowerState,
}

/// The faults ignition reports for a present service processor.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct IgnitionFaults {
    /// The A3 power fault.
    pub a3: bool,
    /// The A2 power fault.
    pub a2: bool,
    /// The root-of-trust fault.
    pub rot: bool,
    /// The service-processor fault.
    pub sp: bool,
}

/// The ignition state of a service processor.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SpIgnitionInfo {
    /// The ignition state has not been read yet.
    NotRead,
    /// Ignition reports the service processor as present.
    Present {
        /// Whether the service processor is powered on.
        power: bool,
        /// The faults ignition reports for the service processor.
        faults: IgnitionFaults,
    },
    /// Ignition reports the service processor as absent.
    Absent,
}

/// A single service processor's inventory.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct SpInfo {
    /// Identifies the service processor by type and slot.
    pub id: SpIdentifier,
    /// The service processor's state, if it has been read.
    pub state: Option<SpStateInfo>,
    /// The ignition state of this service processor.
    pub ignition: SpIgnitionInfo,
    /// The caboose of the active service-processor firmware slot.
    pub caboose_active: SlotCaboose,
    /// The caboose of the inactive service-processor firmware slot.
    pub caboose_inactive: SlotCaboose,
    /// Root-of-trust information, if it has been read.
    pub rot: Option<RotInfo>,
}

impl IdOrdItem for SpInfo {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}

/// Inventory across all service processors.
///
/// This type cannot derive `Eq` because transceiver optical-power readings are
/// floating-point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpInventory {
    /// How long it has been since wicketd last heard from MGS.
    ///
    /// A large value indicates that MGS might be down.
    pub mgs_last_seen: Duration,
    /// The inventory of each SP.
    pub sps: IdOrdMap<SpInfo>,
    /// The switch transceiver (optical module) inventory.
    pub transceivers: TransceiverInventory,
}

/// The switch transceiver (optical module) inventory.
///
/// wicketd reads transceiver state from the switches independently of MGS, so
/// it is presented as its own read/not-read unit rather than folded into the
/// per-SP inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TransceiverInventory {
    /// The transceiver inventory has not been read yet.
    NotRead,
    /// The transceiver inventory was read.
    Read {
        /// How long it has been since the transceiver inventory was last read.
        last_seen: Duration,
        /// The transceivers in each switch.
        switches: IdOrdMap<SwitchTransceivers>,
    },
}

/// The transceivers in a single switch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SwitchTransceivers {
    /// The switch these transceivers belong to.
    pub switch: SwitchSlot,
    /// The transceivers in this switch, keyed by front port.
    pub transceivers: IdOrdMap<Transceiver>,
}

impl IdOrdItem for SwitchTransceivers {
    type Key<'a> = SwitchSlot;

    fn key(&self) -> Self::Key<'_> {
        self.switch
    }

    id_upcast!();
}

/// A single transceiver (optical module) in a switch front port.
///
/// The four categories of state (`status`, `vendor`, `monitors`, and
/// `datapath`) are read independently, so each can be reported or fail on its
/// own.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Transceiver {
    /// The front port the transceiver sits in (for example, `qsfp0`).
    pub port: String,
    /// Module presence and power status.
    pub status: TransceiverStatus,
    /// Vendor identification.
    pub vendor: TransceiverVendor,
    /// Optical power monitors.
    pub monitors: TransceiverMonitors,
    /// Per-lane datapath fault state.
    pub datapath: TransceiverDatapath,
}

impl IdOrdItem for Transceiver {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        &self.port
    }

    id_upcast!();
}

/// The presence and power status of a transceiver module.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TransceiverStatus {
    /// The module status could not be read.
    Error {
        /// The error encountered while reading the status.
        message: String,
    },
    /// The module status was read.
    Read {
        /// Whether a transceiver module is present in the port.
        present: bool,
        /// Whether the module is enabled (powered on).
        enabled: bool,
        /// Whether the module's power is good.
        power_good: bool,
    },
}

/// The vendor identification of a transceiver module.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TransceiverVendor {
    /// The vendor information could not be read.
    Error {
        /// The error encountered while reading the vendor information.
        message: String,
    },
    /// The vendor information was read.
    Read {
        /// The vendor name.
        name: String,
        /// The vendor part number.
        part: String,
        /// The module serial number.
        serial: String,
    },
}

/// The optical power monitors of a transceiver module.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TransceiverMonitors {
    /// The monitoring data could not be read.
    Error {
        /// The error encountered while reading the monitoring data.
        message: String,
    },
    /// The monitoring data was read.
    Read {
        /// Per-lane received optical power, in milliwatts.
        ///
        /// `None` if the module does not report received power.
        rx_power_mw: Option<Vec<f32>>,
        /// Per-lane transmitted optical power, in milliwatts.
        ///
        /// `None` if the module does not report transmitted power.
        tx_power_mw: Option<Vec<f32>>,
    },
}

/// The per-lane datapath fault state of a transceiver module.
///
/// SFF-8636 and CMIS modules describe their datapaths very differently, so this
/// projection preserves each spec's native structure rather than flattening
/// both into a single lane list. Clients can aggregate faults across both specs
/// using the `iter_lane_faults` helper on this type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TransceiverDatapath {
    /// The datapath state could not be read.
    Error {
        /// The error encountered while reading the datapath state.
        message: String,
    },
    /// An SFF-8636 module.
    Sff8636 {
        /// The fault flags for each of the module's four lanes, in lane order.
        lanes: [Sff8636LaneFaults; 4],
    },
    /// A CMIS module.
    Cmis {
        /// The active datapaths, keyed by application selector code.
        datapaths: IdOrdMap<CmisDatapath>,
    },
}

/// Whether a lane fault flag is asserted.
///
/// SFF-8636 modules always report every flag, but a CMIS module may not support
/// reporting a given flag. `Unsupported` distinguishes "the module does not
/// report this flag" from a definite `Clear`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FaultFlag {
    /// The fault flag is asserted.
    Asserted,
    /// The fault flag is not asserted.
    Clear,
    /// The module does not support reporting this flag.
    Unsupported,
}

/// The fault flags for one SFF-8636 lane.
///
/// SFF-8636 always reports all five flags, so these are plain booleans.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct Sff8636LaneFaults {
    /// Media-side (receive) loss of signal.
    pub rx_los: bool,
    /// Host-side (transmit) loss of signal.
    pub tx_los: bool,
    /// Media-side (receive) loss of lock.
    pub rx_lol: bool,
    /// Host-side (transmit) loss of lock.
    pub tx_lol: bool,
    /// A fault in the transmitter and/or laser.
    pub tx_fault: bool,
}

/// One active CMIS datapath (application).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CmisDatapath {
    /// The application selector code identifying this datapath.
    pub application: u8,
    /// The status of each lane in this datapath, keyed by lane number.
    pub lanes: IdOrdMap<CmisLaneStatus>,
}

impl IdOrdItem for CmisDatapath {
    type Key<'a> = u8;

    fn key(&self) -> Self::Key<'_> {
        self.application
    }

    id_upcast!();
}

/// The status of one CMIS lane.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct CmisLaneStatus {
    /// The lane number within the module.
    pub lane: u8,
    /// The datapath state of this lane (CMIS 5.0 section 8.9.1).
    pub state: CmisDatapathState,
    /// Media-side (receive) loss of signal.
    pub rx_los: FaultFlag,
    /// Host-side (transmit) loss of signal.
    pub tx_los: FaultFlag,
    /// Media-side (receive) loss of lock.
    pub rx_lol: FaultFlag,
    /// Host-side (transmit) loss of lock.
    pub tx_lol: FaultFlag,
    /// A fault in the transmitter and/or laser.
    ///
    /// CMIS calls this `TxFailure`; it is named `tx_fault` here for cross-spec
    /// consistency with SFF-8636.
    pub tx_fault: FaultFlag,
}

impl IdOrdItem for CmisLaneStatus {
    type Key<'a> = u8;

    fn key(&self) -> Self::Key<'_> {
        self.lane
    }

    id_upcast!();
}

/// The datapath state of a CMIS lane.
///
/// This mirrors the CMIS datapath state machine (CMIS 5.0 section 8.9.1).
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CmisDatapathState {
    /// The datapath is deactivated.
    Deactivated,
    /// The datapath is initializing.
    Init,
    /// The datapath is deinitializing.
    Deinit,
    /// The datapath is activated.
    Activated,
    /// The transmitter is turning on.
    TxTurnOn,
    /// The transmitter is turning off.
    TxTurnOff,
    /// The datapath is initialized.
    Initialized,
}

/// The physical location of the sled wicketd is running on.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct LocationInfo {
    /// The slot of the switch this sled is cabled to.
    pub switch_slot: SwitchSlot,
    /// The serial number of that switch's service processor, if known.
    pub switch_serial: Option<String>,
    /// The serial number of the sled wicketd is running on, if known.
    pub sled_serial: Option<String>,
}

/// Parameters for the SP inventory endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpInventoryParams {
    /// Refresh the state of these service processors from MGS before returning,
    /// rather than returning their cached state. Service processors not listed
    /// here are returned from the cache.
    #[serde(default)]
    pub force_refresh: Vec<SpIdentifier>,
}

/// A sled as seen on the bootstrap network.
///
/// A sled is reported here once its service processor's state has been read
/// from MGS. (A populated cubby whose state has not yet been polled is absent
/// until it is.)
///
/// A sled's `ip` becomes `Some` once it has been discovered on the bootstrap
/// network; sleds still missing an address report `None`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct BootstrapSled {
    /// The service processor for this sled (its type and slot).
    pub id: SpIdentifier,
    /// The sled's baseboard serial number.
    pub serial_number: String,
    /// The sled's bootstrap-network address, once it has been discovered.
    pub ip: Option<Ipv6Addr>,
}

impl IdOrdItem for BootstrapSled {
    type Key<'a> = SpIdentifier;

    fn key(&self) -> Self::Key<'_> {
        self.id
    }

    id_upcast!();
}
