// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod ereports;
mod ignition;
/// Defines a small UDP client that the tests in this crate use to talk to
/// simulated SPs.
mod sp_client;

/// Path to the sp-sim configuration.
///
/// This is shared by the ereports and ignition tests here, and with the
/// gateway's integration tests.
pub(crate) const SP_SIM_CONFIG_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../gateway-test-utils/configs/sp_sim_config.test.toml"
);
