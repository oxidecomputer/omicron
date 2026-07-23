// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Inventory and discovery types for the commissioning API.
//!
//! These are deliberately minimal projections of wicketd's internal inventory
//! shapes: they carry only the fields the commissioning client needs, so that
//! the large and less stable internal inventory type graph (MGS inventory) does
//! not leak into this stable API.

use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use std::time::Duration;

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Re-exports of pinned gateway types since they are also published by this API.
pub use gateway_types_versions::v1::component::{
    PowerState, SpIdentifier, SpType,
};
pub use gateway_types_versions::v1::rot::{RotImageError, RotSlot};
pub use sled_agent_types_versions::v1::early_networking::SwitchSlot;

/// A minimal projection of a firmware caboose.
///
/// Only the fields the commissioning client needs are included; the remaining
/// caboose fields (git commit, name, and epoch) are intentionally omitted.
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

/// An error wicketd encountered while fetching a piece of inventory from MGS.
///
/// This is the wire projection of wicketd's internal fetch error. Each fetched
/// item that can fail on its own (the SP state, a caboose slot, the stage0
/// bootloader caboose, or the rack-wide ignition list) reports one of these
/// when its most recent fetch failed.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct FetchError {
    /// A description of what went wrong.
    pub message: String,
    /// How long ago this error was observed. A small value means the fetch is
    /// still failing now. A large value means it has not been refreshed in a
    /// while: for a top-level fetch (the SP state or the rack-wide ignition
    /// list) that the poller retries on its own schedule, the poller may be
    /// wedged; for a sub-fetch error (a caboose slot or the stage0 bootloader
    /// caboose, which only refresh on a successful `sp_get`), it instead means
    /// the enclosing SP fetch has stopped succeeding.
    pub age: Duration,
}

/// The stage0 (or pending stage0next) bootloader caboose for a root of trust.
///
/// The four states are kept distinct because they mean different things to a
/// caller:
///
/// * `unsupported`: this RoT version does not report a stage0 bootloader
///   caboose at all.
/// * `not_read`: the RoT version supports reporting a stage0 caboose, but it
///   has not been read yet.
/// * `error`: reading the stage0 caboose was attempted and failed.
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
    /// Reading the stage0 bootloader caboose failed.
    Error {
        /// What went wrong reading the caboose.
        error: FetchError,
    },
    /// The stage0 bootloader caboose was read.
    Read {
        /// The caboose that was read.
        caboose: Caboose,
    },
}

/// The caboose for a single firmware slot, including whether it was read.
///
/// A caboose that could not be read is reported as `error` rather than
/// `not_read`, so that a caller waiting for a caboose to appear can tell a
/// transient "not yet" from a read that keeps failing.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SlotCaboose {
    /// The caboose for this slot has not been read yet.
    NotRead,
    /// Reading the caboose for this slot failed.
    Error {
        /// What went wrong reading the caboose.
        error: FetchError,
    },
    /// The caboose for this slot was read.
    Read {
        /// The caboose that was read.
        caboose: Caboose,
    },
}

/// Root-of-trust information for a single service processor.
///
/// The root of trust is reached through its service processor, so it has its
/// own read state: an SP that responds may still be unable to talk to its RoT.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
// The `Read` variant legitimately carries four caboose slots, so it dwarfs the
// error and not-read variants; boxing would only complicate matching on a
// published wire type.
#[allow(clippy::large_enum_variant)]
pub enum RotInfo {
    /// The root of trust has not been read yet.
    NotRead,
    /// The service processor reported that it cannot reach its root of trust.
    ///
    /// This is the SP's own report, not a wicketd fetch error: the SP
    /// responded, but told us it could not talk to its RoT. It therefore
    /// carries only a message, with no `age`, unlike a `FetchError`.
    Error {
        /// The failure the service processor reported for its root of trust.
        message: String,
    },
    /// The root of trust was read.
    Read {
        /// The currently-active RoT image slot.
        active: RotSlot,
        /// The image in slot A.
        slot_a: RotSlotInfo,
        /// The image in slot B.
        slot_b: RotSlotInfo,
        /// The stage0 bootloader.
        stage0: RotStage0Info,
        /// The pending stage0next bootloader.
        stage0next: RotStage0Info,
    },
}

/// Whether the root of trust considers a firmware image valid.
///
/// Older RoT firmware does not report image validity at all; `Unsupported`
/// distinguishes that case from a definite `Valid`, the same way `Unsupported`
/// does for transceiver fault flags.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "validity", rename_all = "snake_case")]
pub enum RotImageValidity {
    /// The RoT reports this image as valid.
    Valid,
    /// The RoT reports this image as invalid.
    Invalid {
        /// Why the RoT considers the image invalid.
        error: RotImageError,
    },
    /// This RoT firmware version does not report image validity.
    Unsupported,
}

/// One root-of-trust firmware slot.
///
/// The caboose says what firmware is in the slot; `validity` says whether
/// the RoT considers that image usable. The two are read from different
/// sources, so a slot can have a readable caboose and still be unbootable.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct RotSlotInfo {
    /// The caboose of the image in this slot.
    pub caboose: SlotCaboose,
    /// Whether the RoT considers this image valid.
    pub validity: RotImageValidity,
}

/// The stage0 (or stage0next) bootloader slot.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct RotStage0Info {
    /// The caboose of the bootloader in this slot.
    pub caboose: Stage0Caboose,
    /// Whether the RoT considers this bootloader image valid.
    pub validity: RotImageValidity,
}

/// The service processor's state, read together from MGS.
///
/// The serial number and power state are always read together from the same
/// service-processor state, so they are reported as a single unit.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SpStateInfo {
    /// The service processor's state has not been read yet.
    NotRead,
    /// Reading the service processor's state failed.
    Error {
        /// What went wrong reading the state.
        ///
        /// The error's `message` describes the failure; its `age` says how
        /// recently the failure was last observed.
        error: FetchError,
    },
    /// The service processor's state was read.
    Read {
        /// The baseboard serial number reported by the service processor.
        serial_number: String,
        /// The host power state.
        power_state: PowerState,
        /// The error from a more recent state fetch that failed while this
        /// older reading still stands, if any.
        ///
        /// The reading remains primary: a stale-but-real reading beats no
        /// reading. `Some` means the most recent `sp_get` failed but an earlier
        /// one succeeded, so the concurrent failure is surfaced here rather than
        /// displacing the reading. `None` means the most recent fetch succeeded
        /// (or none has failed since).
        refresh_error: Option<FetchError>,
        /// How long it has been since this reading was fetched.
        ///
        /// A large value means wicketd has not successfully read this
        /// service processor recently, even if `refresh_error` is `None`.
        ///
        /// This age applies to the state reading alone. The cabooses and RoT
        /// information for this SP are re-read only when its reported state
        /// changes or when a prior read of them failed, so they may be older
        /// than this age. In particular, writing a new image to the inactive
        /// service-processor slot does not change the reported state, so
        /// `caboose_inactive` may remain stale until the service processor
        /// resets.
        age: Duration,
    },
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
///
/// This has no per-SP `Error` variant: ignition is fetched rack-wide in a
/// single request, so a fetch failure is not attributable to any one SP. A
/// failed ignition fetch is reported once on `SpInventory.ignition_fetch_error`
/// instead.
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
        /// Whether ignition controller 0 detects this target.
        ///
        /// Ignition is wired to both switches, and each switch's controller
        /// reports detection independently. `false` on one controller while
        /// the other detects the target indicates a broken ignition path (for
        /// example, a bad cable) to that controller's switch.
        ctrl_detect_0: bool,
        /// Whether ignition controller 1 detects this target.
        ///
        /// See `ctrl_detect_0`.
        ctrl_detect_1: bool,
    },
    /// Ignition reports the service processor as absent.
    Absent,
}

/// A single service processor's inventory.
///
/// The state, ignition, caboose, and root-of-trust fields are read from MGS
/// independently of one another, so each carries its own read state and they
/// may disagree about how much is known.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct SpInfo {
    /// Identifies the service processor by type and slot.
    pub id: SpIdentifier,
    /// The service processor's state.
    pub state: SpStateInfo,
    /// The ignition state of this service processor.
    pub ignition: SpIgnitionInfo,
    /// The caboose of the active service-processor firmware slot.
    pub caboose_active: SlotCaboose,
    /// The caboose of the inactive service-processor firmware slot.
    pub caboose_inactive: SlotCaboose,
    /// Root-of-trust information.
    pub rot: RotInfo,
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
    /// How long it has been since wicketd last received a successful response
    /// from MGS.
    ///
    /// A large value indicates that MGS might be down. This is a duration
    /// rather than a timestamp because wicketd and its clients keep
    /// independent clocks, and commissioning runs before time is synced.
    pub mgs_last_seen: Duration,
    /// The inventory of each SP.
    pub sps: IdOrdMap<SpInfo>,
    /// The rack-wide ignition-list fetch error, if the most recent ignition
    /// fetch failed.
    ///
    /// Ignition is fetched for the whole rack in a single request, so a failure
    /// is reported once here rather than per SP. `None` means the last ignition
    /// fetch succeeded (or none has failed yet); the per-SP `ignition` fields
    /// then reflect that most recent successful fetch.
    pub ignition_fetch_error: Option<FetchError>,
    /// The switch transceiver (optical module) inventory, keyed by switch.
    ///
    /// wicketd reads transceiver state from the switches independently of
    /// MGS. A switch that wicketd has never successfully read is absent from
    /// this map rather than present and empty, so a caller can tell "this
    /// switch reported no transceivers" from "we have never heard from this
    /// switch". An empty map means no switch has been read yet.
    pub transceivers: IdOrdMap<SwitchTransceivers>,
}

/// The transceivers in a single switch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SwitchTransceivers {
    /// The switch these transceivers belong to.
    pub switch: SwitchSlot,
    /// How long it has been since this switch's transceivers were last read
    /// successfully.
    ///
    /// This is tracked per switch because wicketd polls each switch with its
    /// own task: a large value here means this switch's transceivers have not
    /// been read recently (the switch may be unreachable, or the poller may be
    /// stuck), even if the other switch is being read normally.
    pub last_seen: Duration,
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
/// The five categories of state (`status`, `power`, `vendor`, `monitors`, and
/// `datapath`) are read independently, so each can be reported or fail on its
/// own.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Transceiver {
    /// The front port the transceiver sits in (for example, `qsfp0`).
    pub port: String,
    /// Module presence and power status.
    pub status: TransceiverStatus,
    /// Module power mode.
    pub power: TransceiverPower,
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

/// The power mode of a transceiver module, including whether it was read.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TransceiverPower {
    /// The power mode could not be read.
    Error {
        /// The error encountered while reading the power mode.
        message: String,
    },
    /// The power mode was read.
    Read {
        /// The module's power mode.
        mode: TransceiverPowerMode,
    },
}

/// The power mode of a transceiver module.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TransceiverPowerMode {
    /// The module is powered off.
    Off,
    /// The module is in low-power mode; it will not establish a link or carry
    /// traffic, but can be managed and queried.
    Low,
    /// The module is in high-power mode.
    High,
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
        /// Per-lane received optical power.
        ///
        /// `None` if the module does not report received power.
        rx_power: Option<Vec<ReceiverPower>>,
        /// Per-lane transmitted optical power, in milliwatts.
        ///
        /// `None` if the module does not report transmitted power.
        tx_power_mw: Option<Vec<f32>>,
    },
}

/// A received optical power measurement from one lane.
///
/// Modules report received power in one of two ways, and the two are not
/// directly comparable, so the kind of measurement is carried alongside the
/// value rather than being flattened away.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReceiverPower {
    /// An average optical power measurement, in milliwatts.
    Average {
        /// The measured power, in milliwatts.
        value_mw: f32,
    },
    /// A peak-to-peak optical power measurement, in milliwatts.
    PeakToPeak {
        /// The measured power, in milliwatts.
        value_mw: f32,
    },
}

/// The per-lane datapath fault state of a transceiver module.
///
/// SFF-8636 and CMIS modules describe their datapaths very differently, so this
/// projection preserves each spec's native structure rather than flattening
/// both into a single lane list. The Rust client types provide an
/// `iter_lane_faults` helper to aggregate faults across both specs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
/// This mirrors the CMIS datapath state machine; the states are those reported
/// by the Data Path State Indicator (CMIS 5.0 section 8.9.1).
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
    /// The serial number of that switch, as reported by its service
    /// processor, if known.
    ///
    /// `None` means the switch's state has not been successfully read yet;
    /// the switch's entry in the SP inventory carries the details, including
    /// any fetch error.
    pub switch_serial: Option<String>,
    /// The serial number of the sled wicketd is running on, if known.
    ///
    /// `None` means wicketd was started without baseboard information.
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
    pub force_refresh: BTreeSet<SpIdentifier>,
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

/// The response to a bootstrap-sleds request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GetBootstrapSledsResponse {
    /// The sleds visible in the MGS inventory.
    ///
    /// See `BootstrapSled` for when a sled appears here and when its `ip` is
    /// populated.
    pub sleds: IdOrdMap<BootstrapSled>,
    /// Bootstrap-network peers that could not be matched to any sled in
    /// `sleds`.
    ///
    /// Reported so that "a peer is on the bootstrap network but cannot be
    /// matched to inventory" is distinguishable from "the sled has not been
    /// discovered on the bootstrap network yet" (a `None` `BootstrapSled::ip`).
    pub unmatched_peers: Vec<UnmatchedBootstrapPeer>,
}

/// A peer discovered on the bootstrap network that could not be matched to
/// any sled reported by MGS.
///
/// Bootstrap peers are matched to sleds by the baseboard identity each peer's
/// bootstrap agent reports about itself. A peer appears here instead of
/// contributing a `BootstrapSled::ip` when that match wasn't successful: either
/// transiently, because the sled's SP has not been read from MGS yet, or
/// persistently, because the peer could not identify its own baseboard (or
/// reports an identity that disagrees with MGS). A persistent entry here means
/// the peer's address has been discovered but rack setup will not be able to
/// use it.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct UnmatchedBootstrapPeer {
    /// The baseboard identity the peer reported about itself.
    pub identity: String,
    /// The peer's bootstrap-network address.
    pub ip: Ipv6Addr,
}
