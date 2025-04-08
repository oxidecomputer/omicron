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
mod driver;
mod host_phase1_updater;
mod mgs_clients;
mod rot_updater;
mod sp_updater;

pub use artifacts::ArtifactCache;
pub use artifacts::ArtifactCacheError;
pub use common_sp_update::ReconfiguratorSpComponentUpdater;
pub use common_sp_update::SpComponentUpdateError;
pub use common_sp_update::SpComponentUpdater;
pub use driver::DriverStatus;
pub use driver::MgsUpdateDriver;
pub use host_phase1_updater::HostPhase1Updater;
pub use mgs_clients::MgsClients;
pub use rot_updater::RotUpdater;
pub use sp_updater::SpUpdater;

#[derive(Debug, PartialEq, Clone)]
pub enum UpdateProgress {
    Started,
    Preparing { progress: Option<f64> },
    InProgress { progress: Option<f64> },
    Complete,
    Failed(String),
}
