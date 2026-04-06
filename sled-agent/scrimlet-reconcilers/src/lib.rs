// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! This crate implements long-running reconciler tasks responsible for
//! configuration of services within a scrimlet's switch zone.
//!
//! These tasks _only_ talk to services on the same sled as the sled-agent
//! executing these tasks; we attempt to ensure this at runtime via types like
//! [`ThisSledSwitchZoneUnderlayIpAddr`]. A scrimlet running these tasks should
//! never attempt to talk to another scrimlet's switch zone, and a non-scrimlet
//! running these tasks should never attempt to talk to anything. (Non-scrimlet
//! sleds still run these tasks, as sled-agent can't easily tell the difference
//! between "not a scrimlet because we're in a different cubby" and "not a
//! scrimlet because we should have a switch but it isn't connected / isn't
//! powered on / etc.". The tasks remain in an inert state on non-scrimlets.)
//!
//! These tasks are responsible for applying system-level networking, including:
//!
//! * Configuration of uplink ports within `dpd`
//! * Configuration of NAT entries for system-level services (boundary NTP,
//!   Nexus, external DNS - notably _not_ instances) within `dpd`
//! * Configuration of BGP within `mgd`
//! * Configuration of BFD within `mgd`
//! * Configuration of static routes within `mgd`
//!
//! The specific configuration that should be applied comes from Nexus (or RSS,
//! at rack setup time) and is sent to `sled-agent` via the bootstore.
//!
//! In the past, responsibility for this configuration was split: sled-agent was
//! responsible for applying an initial config on sled boot (required for cold
//! boot of the rack), and Nexus was responsible for continuously keeping the
//! config in sync afterwards. This has a variety of problems; see
//! <https://github.com/oxidecomputer/omicron/issues/10167>. The split is now
//! that Nexus is responsible for maintaining what the configuration should be,
//! and each scrimlet is responsible for applying that configuration to its own
//! switch zone's services; the latter is implemented via this crate.

mod dpd_reconciler;
mod handle;
mod mgd_reconciler;
mod reconciler_task;
mod status;
mod switch_zone_slot;
mod switch_zone_underlay_ip;

pub use dpd_reconciler::DpdNatReconcilerStatusNatEntry;
pub use dpd_reconciler::DpdNatReconcilerStatusNatEntryFailure;
pub use dpd_reconciler::DpdPortOperationFailure;
pub use dpd_reconciler::DpdPortReconcilerStatus;
pub use dpd_reconciler::DpdReconcilerStatus;
pub use handle::ScrimletReconcilers;
pub use handle::ScrimletReconcilersPrereqs;
pub use mgd_reconciler::MgdReconcilerStatus;
pub use status::ReconcilerActivationReason;
pub use status::ReconcilerCurrentStatus;
pub use status::ReconcilerInertReason;
pub use status::ReconcilerRunningStatus;
pub use status::ReconcilerStatus;
pub use status::ReconciliationCompletedStatus;
pub use status::ScrimletReconcilersStatus;
pub use status::ScrimletStatus;
pub use switch_zone_underlay_ip::ThisSledSwitchZoneUnderlayIpAddr;
