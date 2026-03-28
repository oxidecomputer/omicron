// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO: crate-level docs
//!
//! For now, see <https://github.com/oxidecomputer/omicron/issues/10167>.

mod dpd_reconciler;
mod handle;
mod reconciler_task;
mod status;
mod switch_zone_slot;
mod switch_zone_underlay_ip;

pub use handle::ScrimletReconcilers;
pub use handle::ScrimletReconcilersPrereqs;
pub use status::ReconcilerActivationReason;
pub use status::ReconcilerCurrentStatus;
pub use status::ReconcilerInertReason;
pub use status::ReconcilerRunningStatus;
pub use status::ReconcilerStatus;
pub use status::ReconciliationCompletedStatus;
pub use status::ScrimletReconcilersStatus;
pub use status::ScrimletStatus;
pub use switch_zone_underlay_ip::ThisSledSwitchZoneUnderlayIpAddr;
