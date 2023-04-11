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
/// their state, along with some well-known tasks
pub fn init(
    opctx: &OpContext,
    datastore: Arc<DataStore>,
    config: &BackgroundTaskConfig,
) -> (common::Driver, common::TaskHandle) {
    let mut driver = common::Driver::new();

    let rv = init_dns(
        &mut driver,
        opctx,
        datastore.clone(),
        DnsGroup::Internal,
        &config.dns_internal,
    );
    let _ = init_dns(
        &mut driver,
        opctx,
        datastore,
        DnsGroup::External,
        &config.dns_external,
    );

    (driver, rv)
}

fn init_dns(
    driver: &mut common::Driver,
    opctx: &OpContext,
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    config: &DnsTasksConfig,
) -> common::TaskHandle {
    let dns_group_name = dns_group.to_string();
    let metadata = BTreeMap::from([("dns_group".to_string(), dns_group_name)]);

    // Background task: DNS config watcher
    let dns_config =
        dns_config::DnsConfigWatcher::new(Arc::clone(&datastore), dns_group);
    let dns_config_watcher = dns_config.watcher();
    let rv = driver
        .register(
            format!("dns_config_{}", dns_group),
            config.period_secs_config,
            Box::new(dns_config),
            opctx.child(metadata.clone()),
            vec![],
        )
        .clone();

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

    rv
}

#[cfg(test)]
mod test {
    use crate::db::TransactionError;
    use async_bb8_diesel::AsyncConnection;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::Generation;
    use nexus_db_queries::context::OpContext;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::internal_api::params as nexus_params;
    use omicron_common::api::external::DataPageParams;
    use omicron_test_utils::dev::poll;
    use std::num::NonZeroU32;
    use std::time::Duration;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    #[nexus_test(server = crate::Server)]
    async fn test_dns_propagation_basic(cptestctx: &ControlPlaneTestContext) {
        // Nexus is supposed to automatically propagate DNS configuration to all
        // the DNS servers it knows about.  To test that, we'll first write an
        // initial DNS configuration to the datastore.  Then we'll wake up the
        // background task responsible for monitoring that.  We should shortly
        // see that DNS configuration propagated to the existing DNS server.
        let nexus = &cptestctx.server.apictx().nexus;
        let datastore = nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Verify our going-in assumption that Nexus has written the initial
        // internal DNS configuration.  This happens during rack initialization,
        // which the test runner simulates.
        let version = datastore
            .dns_group_latest_version(&opctx, DnsGroup::Internal)
            .await
            .unwrap();
        let found_version = i64::from(&version.version.0);
        assert_eq!(found_version, 1);

        // Verify that the DNS server is on version 1.  This should already be
        // the case because it was configured with version 1 when the simulated
        // sled agent started up.
        let dns_dropshot_server = &cptestctx.sled_agent.dns_dropshot_server;
        let dns_config_client = dns_service_client::Client::new(
            &format!("http://{}", dns_dropshot_server.local_addr()),
            cptestctx.logctx.log.clone(),
        );
        let config = dns_config_client
            .dns_config_get()
            .await
            .expect("failed to get initial DNS server config");
        assert_eq!(config.generation, 1);

        // We'll need the id of the internal DNS zone.
        let dns_zones = datastore
            .dns_zones_list(
                &opctx,
                DnsGroup::Internal,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::new(2).unwrap(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            dns_zones.len(),
            1,
            "expected exactly one internal DNS zone"
        );
        let internal_dns_zone_id = dns_zones[0].id;

        // Now, write version 2 of the internal DNS configuration with one
        // additional record.
        type TxnError = TransactionError<()>;
        {
            let conn = datastore.pool_for_tests().await.unwrap();
            let _: Result<(), TxnError> = conn
                .transaction_async(|conn| async move {
                    {
                        use crate::db::model::DnsVersion;
                        use crate::db::schema::dns_version::dsl;

                        diesel::insert_into(dsl::dns_version)
                            .values(DnsVersion {
                                dns_group: DnsGroup::Internal,
                                version: Generation(2.try_into().unwrap()),
                                time_created: chrono::Utc::now(),
                                creator: String::from("test suite"),
                                comment: String::from("test suite"),
                            })
                            .execute_async(&conn)
                            .await
                            .unwrap();
                    }

                    {
                        use crate::db::model::DnsName;
                        use crate::db::schema::dns_name::dsl;

                        diesel::insert_into(dsl::dns_name)
                            .values(
                                DnsName::new(
                                    internal_dns_zone_id,
                                    String::from("we-got-beets"),
                                    Generation(2.try_into().unwrap()),
                                    None,
                                    vec![nexus_params::DnsRecord::Aaaa(
                                        "fe80::3".parse().unwrap(),
                                    )],
                                )
                                .unwrap(),
                            )
                            .execute_async(&conn)
                            .await
                            .unwrap();
                    }

                    Ok(())
                })
                .await;
        }

        // Activate the internal DNS propagation pipeline.
        nexus._background_tasks.activate(&nexus._task_internal_dns_config);

        poll::wait_for_condition(
            || async {
                match dns_config_client.dns_config_get().await {
                    Err(error) => {
                        // The DNS server is already up.  This shouldn't happen.
                        Err(poll::CondCheckError::Failed(error))
                    }
                    Ok(config) => {
                        if config.generation == 2 {
                            Ok(())
                        } else {
                            Err(poll::CondCheckError::NotYet)
                        }
                    }
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(15),
        )
        .await
        .expect("DNS config not propagated in expected time");

        // XXX-dap then add a second part to the test that adds a new DNS
        // server.
    }
}
