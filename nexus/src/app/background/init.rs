// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task initialization

use super::common;
use super::dns_config;
use super::dns_propagation;
use super::dns_servers;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::nexus_config::BackgroundTaskConfig;
use omicron_common::nexus_config::DnsTasksConfig;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Kick off all background tasks
///
/// Returns a `Driver` that can be used for inspecting background tasks and
/// their state
pub fn init(
    opctx: &OpContext,
    datastore: Arc<DataStore>,
    config: &BackgroundTaskConfig,
) -> common::Driver {
    let mut driver = common::Driver::new();

    init_dns(
        &mut driver,
        opctx,
        datastore.clone(),
        DnsGroup::Internal,
        &config.dns_internal,
    );
    init_dns(
        &mut driver,
        opctx,
        datastore,
        DnsGroup::External,
        &config.dns_external,
    );

    driver
}

fn init_dns(
    driver: &mut common::Driver,
    opctx: &OpContext,
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    config: &DnsTasksConfig,
) {
    let dns_group_name = dns_group.to_string();
    let metadata = BTreeMap::from([("dns_group".to_string(), dns_group_name)]);

    // Background task: DNS config watcher
    let dns_config =
        dns_config::DnsConfigWatcher::new(Arc::clone(&datastore), dns_group);
    let dns_config_watcher = dns_config.watcher();
    driver.register(
        format!("dns_config_{}", dns_group),
        config.period_secs_config,
        Box::new(dns_config),
        opctx.child(metadata.clone()),
        vec![],
    );

    // Background task: DNS server list watcher
    let dns_servers = dns_servers::DnsServersWatcher::new(datastore, dns_group);
    let dns_servers_watcher = dns_servers.watcher();
    driver.register(
        format!("dns_servers_{}", dns_group),
        config.period_secs_servers,
        Box::new(dns_servers),
        opctx.child(metadata.clone()),
        vec![],
    );

    // Background task: DNS propagation
    let dns_propagate = dns_propagation::DnsPropagator::new(
        dns_config_watcher.clone(),
        dns_servers_watcher.clone(),
        config.max_concurrent_server_updates,
    );
    driver.register(
        format!("dns_propagation_{}", dns_group),
        config.period_secs_propagation,
        Box::new(dns_propagate),
        opctx.child(metadata),
        vec![Box::new(dns_config_watcher), Box::new(dns_servers_watcher)],
    );
}
