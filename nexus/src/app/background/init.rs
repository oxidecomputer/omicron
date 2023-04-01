// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task initialization

use super::common;
use super::dns_config;
use super::dns_servers;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use slog::o;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Kick off all background tasks
///
/// Returns a `Driver` that can be used for inspecting background tasks and
/// their state
pub fn init(opctx: &OpContext, datastore: Arc<DataStore>) -> common::Driver {
    let mut driver = common::Driver::new();

    let log_dns_internal =
        opctx.log.new(o!("dns_group" => format!("{:?}", DnsGroup::Internal)));
    let log_dns_external =
        opctx.log.new(o!("dns_group" => format!("{:?}", DnsGroup::External)));
    let metadata_internal = BTreeMap::from([(
        "dns_group".to_string(),
        format!("{:?}", DnsGroup::Internal),
    )]);
    let metadata_external = BTreeMap::from([(
        "dns_group".to_string(),
        format!("{:?}", DnsGroup::External),
    )]);

    // Background task: internal DNS config watcher
    let dns_config_internal = dns_config::DnsConfigWatcher::new(
        Arc::clone(&datastore),
        DnsGroup::Internal,
    );
    let dns_config_internal_watcher = dns_config_internal.watcher();
    driver.register(
        "dns_config_internal",
        Duration::from_secs(60),
        Box::new(dns_config_internal),
        opctx.child(log_dns_internal.clone(), metadata_internal.clone()),
    );

    // Background task: internal DNS server list watcher
    let dns_servers_internal = dns_servers::DnsServersWatcher::new(
        Arc::clone(&datastore),
        DnsGroup::Internal,
    );
    let dns_servers_internal_watcher = dns_servers_internal.watcher();
    driver.register(
        "dns_servers_internal",
        Duration::from_secs(60),
        Box::new(dns_servers_internal),
        opctx.child(log_dns_internal.clone(), metadata_internal.clone()),
    );

    // Background task: external DNS config watcher
    let dns_config_external = dns_config::DnsConfigWatcher::new(
        Arc::clone(&datastore),
        DnsGroup::External,
    );
    let dns_config_external_watcher = dns_config_external.watcher();
    driver.register(
        "dns_config_external",
        Duration::from_secs(60),
        Box::new(dns_config_external),
        opctx.child(log_dns_external.clone(), metadata_external.clone()),
    );

    // Background task: external DNS server list watcher
    let dns_servers_external = dns_servers::DnsServersWatcher::new(
        Arc::clone(&datastore),
        DnsGroup::External,
    );
    let dns_servers_external_watcher = dns_servers_external.watcher();
    driver.register(
        "dns_servers_external",
        Duration::from_secs(60),
        Box::new(dns_servers_external),
        opctx.child(log_dns_external.clone(), metadata_external.clone()),
    );

    driver
}
