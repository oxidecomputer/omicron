// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

use omicron_gateway::Config;

#[test]
fn read_production_smf_config() {
    let production_smf_config_path = "../smf/mgs/config.toml";
    let production_smf_config =
        std::fs::read_to_string(production_smf_config_path)
            .expect("read SMF config file");

    println!("read SMF config file:\n---\n{production_smf_config}\n---");

    let config: Config =
        toml::from_str(&production_smf_config).expect("parsed config as TOML");

    println!("parsed into config: {config:?}");

    // Most of the value of this test is that the config file parsed at all, but
    // we can also spot check some fields to make sure they're not completely
    // unreasonable.
    let switch_config = &config.switch;
    assert!(switch_config.udp_listen_port > 0);
    assert!(!switch_config.local_ignition_controller_interface.is_empty());
    assert!(switch_config.rpc_retry_config.max_attempts_general > 0);
    assert!(switch_config.rpc_retry_config.max_attempts_reset > 0);
    assert!(switch_config.rpc_retry_config.per_attempt_timeout_millis > 0);
    assert!(!switch_config.location.description.is_empty());
    assert!(!switch_config.location.determination.is_empty());
    assert!(!switch_config.port.is_empty());
}
