// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::time::Duration;

pub mod example;
pub mod inventory;
pub mod preflight_check;
pub mod rack_setup;
pub mod rack_update;
pub mod update_events;

// WICKETD_TIMEOUT used to be 1 second, but that might be too short (and in
// particular might be responsible for
// https://github.com/oxidecomputer/omicron/issues/3103).
pub const WICKETD_TIMEOUT: Duration = Duration::from_secs(5);
