// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `EXTERNAL_JUMBO_FRAMES` of the external Nexus API.
//!
//! This version adds:
//!
//! - `InstanceCreate.enable_jumbo_frames` — per-instance opt-in for jumbo
//!   frames (8500 byte MTU) on the primary OPTE interface.
//! - `InstanceUpdate.enable_jumbo_frames` — same disposition, mutable via
//!   the existing update endpoint. Changes only take effect on the next
//!   instance restart.
//! - `SystemNetworkingSettings` view/update — fleet-wide opt-in gate for
//!   the per-instance bit, controlled by fleet admins.

pub mod instance;
pub mod system_networking;
