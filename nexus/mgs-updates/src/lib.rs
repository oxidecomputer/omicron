// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of Nexus-driven MGS-mediated updates
//!
//! This includes updates of:
//!
//! - service processor software (Hubris image)
//! - root of trust software (Hubris image)
//! - root of trust bootloader (bootleby)
//! - host phase 1 image (Helios phase 1)

mod artifacts;
mod common_sp_update;
mod host_phase1_updater;
mod mgs_clients;
mod rot_updater;
mod sp_updater;
mod tracker;

pub use artifacts::ArtifactCache;
pub use artifacts::ArtifactCacheError;
pub use common_sp_update::ReconfiguratorSpComponentUpdater;
pub use common_sp_update::SpComponentUpdateError;
pub use common_sp_update::SpComponentUpdater;
pub use host_phase1_updater::HostPhase1Updater;
pub use mgs_clients::MgsClients;
pub use rot_updater::RotUpdater;
pub use sp_updater::SpUpdater;
// XXX-dap split tracker up into ConcurrentUpdater and Tracker?
pub use tracker::ApplyUpdateError;
pub use tracker::ApplyUpdateResult;
pub use tracker::MgsUpdateDriver;
pub use tracker::apply_update;
// XXX-dap add type definitions for the map of requests?  or maybe even a
// struct to manage this?

#[derive(Debug, PartialEq, Clone)]
pub enum UpdateProgress {
    Started,
    Preparing { progress: Option<f64> },
    InProgress { progress: Option<f64> },
    Complete,
    Failed(String),
}
