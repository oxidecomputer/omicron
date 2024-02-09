// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task initialization

use super::bfd;
use super::blueprint_execution;
use super::blueprint_load;
use super::common;
use super::dns_config;
use super::dns_propagation;
use super::dns_servers;
use super::external_endpoints;
use super::inventory_collection;
use super::nat_cleanup;
use super::phantom_disks;
use super::region_replacement;
use super::sync_service_zone_nat::ServiceZoneNatTracker;
use crate::app::sagas::SagaRequest;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::nexus_config::BackgroundTaskConfig;
use omicron_common::nexus_config::DnsTasksConfig;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

/// Describes ongoing background tasks and provides interfaces for working with
/// them
///
/// Most interaction happens through the `driver` field.  The rest of the fields
/// are specific background tasks.
pub struct BackgroundTasks {
    /// interface for working with background tasks (activation, checking
    /// status, etc.)
    pub driver: common::Driver,

    /// task handle for the internal DNS config background task
    pub task_internal_dns_config: common::TaskHandle,
    /// task handle for the internal DNS servers background task
    pub task_internal_dns_servers: common::TaskHandle,
    /// task handle for the external DNS config background task
    pub task_external_dns_config: common::TaskHandle,
    /// task handle for the external DNS servers background task
    pub task_external_dns_servers: common::TaskHandle,

    /// task handle for the task that keeps track of external endpoints
    pub task_external_endpoints: common::TaskHandle,
    /// external endpoints read by the background task
    pub external_endpoints: tokio::sync::watch::Receiver<
        Option<external_endpoints::ExternalEndpoints>,
    >,
    /// task handle for the ipv4 nat entry garbage collector
    pub nat_cleanup: common::TaskHandle,

    /// task handle for the switch bfd manager
    pub bfd_manager: common::TaskHandle,

    /// task handle for the task that collects inventory
    pub task_inventory_collection: common::TaskHandle,

    /// task handle for the task that detects phantom disks
    pub task_phantom_disks: common::TaskHandle,

    /// task handle for blueprint target loader
    pub task_blueprint_loader: common::TaskHandle,

    /// task handle for blueprint execution background task
    pub task_blueprint_executor: common::TaskHandle,

    /// task handle for the service zone nat tracker
    pub task_service_zone_nat_tracker: common::TaskHandle,

    /// task handle for the task that detects if regions need replacement and
    /// begins the process
    pub task_region_replacement: common::TaskHandle,
}

impl BackgroundTasks {
    /// Kick off all background tasks
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        opctx: &OpContext,
        datastore: Arc<DataStore>,
        config: &BackgroundTaskConfig,
        dpd_clients: &HashMap<SwitchLocation, Arc<dpd_client::Client>>,
        mgd_clients: &HashMap<SwitchLocation, Arc<mg_admin_client::Client>>,
        nexus_id: Uuid,
        resolver: internal_dns::resolver::Resolver,
        saga_request: Sender<SagaRequest>,
    ) -> BackgroundTasks {
        let mut driver = common::Driver::new();

        let (task_internal_dns_config, task_internal_dns_servers) = init_dns(
            &mut driver,
            opctx,
            datastore.clone(),
            DnsGroup::Internal,
            &config.dns_internal,
        );
        let (task_external_dns_config, task_external_dns_servers) = init_dns(
            &mut driver,
            opctx,
            datastore.clone(),
            DnsGroup::External,
            &config.dns_external,
        );

        // Background task: External endpoints list watcher
        let (task_external_endpoints, external_endpoints) = {
            let watcher = external_endpoints::ExternalEndpointsWatcher::new(
                datastore.clone(),
            );
            let watcher_channel = watcher.watcher();
            let task = driver.register(
                String::from("external_endpoints"),
                String::from(
                    "reads config for silos and TLS certificates to determine \
                    the right set of HTTP endpoints, their HTTP server names, \
                    and which TLS certificates to use on each one",
                ),
                config.external_endpoints.period_secs,
                Box::new(watcher),
                opctx.child(BTreeMap::new()),
                vec![],
            );
            (task, watcher_channel)
        };

        let dpd_clients: Vec<_> = dpd_clients.values().cloned().collect();

        let nat_cleanup = {
            driver.register(
                "nat_v4_garbage_collector".to_string(),
                String::from(
                    "prunes soft-deleted IPV4 NAT entries from ipv4_nat_entry table \
                     based on a predetermined retention policy",
                ),
                config.nat_cleanup.period_secs,
                Box::new(nat_cleanup::Ipv4NatGarbageCollector::new(
                    datastore.clone(),
                    dpd_clients.clone(),
                )),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };

        let bfd_manager = {
            driver.register(
                "bfd_manager".to_string(),
                String::from(
                    "Manages bidirectional fowarding detection (BFD) \
                    configuration on rack switches",
                ),
                config.bfd_manager.period_secs,
                Box::new(bfd::BfdManager::new(
                    datastore.clone(),
                    mgd_clients.clone(),
                )),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };

        // Background task: inventory collector
        let task_inventory_collection = {
            let collector = inventory_collection::InventoryCollector::new(
                datastore.clone(),
                resolver,
                &nexus_id.to_string(),
                config.inventory.nkeep,
                config.inventory.disable,
            );
            let task = driver.register(
                String::from("inventory_collection"),
                String::from(
                    "collects hardware and software inventory data from the \
                    whole system",
                ),
                config.inventory.period_secs,
                Box::new(collector),
                opctx.child(BTreeMap::new()),
                vec![],
            );

            task
        };

        // Background task: phantom disk detection
        let task_phantom_disks = {
            let detector =
                phantom_disks::PhantomDiskDetector::new(datastore.clone());

            let task = driver.register(
                String::from("phantom_disks"),
                String::from("detects and un-deletes phantom disks"),
                config.phantom_disks.period_secs,
                Box::new(detector),
                opctx.child(BTreeMap::new()),
                vec![],
            );

            task
        };

        // Background task: blueprint loader
        let blueprint_loader =
            blueprint_load::TargetBlueprintLoader::new(datastore.clone());
        let rx_blueprint = blueprint_loader.watcher();
        let task_blueprint_loader = driver.register(
            String::from("blueprint_loader"),
            String::from("Loads the current target blueprint from the DB"),
            config.blueprints.period_secs_load,
            Box::new(blueprint_loader),
            opctx.child(BTreeMap::new()),
            vec![],
        );

        // Background task: blueprint executor
        let blueprint_executor = blueprint_execution::BlueprintExecutor::new(
            datastore.clone(),
            rx_blueprint.clone(),
        );
        let task_blueprint_executor = driver.register(
            String::from("blueprint_executor"),
            String::from("Executes the target blueprint"),
            config.blueprints.period_secs_execute,
            Box::new(blueprint_executor),
            opctx.child(BTreeMap::new()),
            vec![Box::new(rx_blueprint)],
        );

        let task_service_zone_nat_tracker = {
            driver.register(
                "service_zone_nat_tracker".to_string(),
                String::from(
                    "ensures service zone nat records are recorded in NAT RPW table",
                ),
                config.sync_service_zone_nat.period_secs,
                Box::new(ServiceZoneNatTracker::new(
                    datastore.clone(),
                    dpd_clients.clone(),
                )),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };

        // Background task: detect if a region needs replacement and begin the
        // process
        let task_region_replacement = {
            let detector = region_replacement::RegionReplacementDetector::new(
                datastore,
                saga_request.clone(),
            );

            let task = driver.register(
                String::from("region_replacement"),
                String::from("detects if a region requires replacing and begins the process"),
                config.region_replacement.period_secs,
                Box::new(detector),
                opctx.child(BTreeMap::new()),
                vec![],
            );

            task
        };

        BackgroundTasks {
            driver,
            task_internal_dns_config,
            task_internal_dns_servers,
            task_external_dns_config,
            task_external_dns_servers,
            task_external_endpoints,
            external_endpoints,
            nat_cleanup,
            bfd_manager,
            task_inventory_collection,
            task_phantom_disks,
            task_blueprint_loader,
            task_blueprint_executor,
            task_service_zone_nat_tracker,
            task_region_replacement,
        }
    }

    pub fn activate(&self, task: &common::TaskHandle) {
        self.driver.activate(task);
    }
}

fn init_dns(
    driver: &mut common::Driver,
    opctx: &OpContext,
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    config: &DnsTasksConfig,
) -> (common::TaskHandle, common::TaskHandle) {
    let dns_group_name = dns_group.to_string();
    let metadata = BTreeMap::from([("dns_group".to_string(), dns_group_name)]);

    // Background task: DNS config watcher
    let dns_config =
        dns_config::DnsConfigWatcher::new(Arc::clone(&datastore), dns_group);
    let dns_config_watcher = dns_config.watcher();
    let task_name_config = format!("dns_config_{}", dns_group);
    let task_config = driver.register(
        task_name_config.clone(),
        format!("watches {} DNS data stored in CockroachDB", dns_group),
        config.period_secs_config,
        Box::new(dns_config),
        opctx.child(metadata.clone()),
        vec![],
    );

    // Background task: DNS server list watcher
    let dns_servers = dns_servers::DnsServersWatcher::new(datastore, dns_group);
    let dns_servers_watcher = dns_servers.watcher();
    let task_name_servers = format!("dns_servers_{}", dns_group);
    let task_servers = driver.register(
        task_name_servers.clone(),
        format!(
            "watches list of {} DNS servers stored in CockroachDB",
            dns_group,
        ),
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
        format!(
            "propagates latest {} DNS configuration (from {:?} background \
            task) to the latest list of DNS servers (from {:?} background \
            task)",
            dns_group, task_name_config, task_name_servers,
        ),
        config.period_secs_propagation,
        Box::new(dns_propagate),
        opctx.child(metadata),
        vec![Box::new(dns_config_watcher), Box::new(dns_servers_watcher)],
    );

    (task_config, task_servers)
}

#[cfg(test)]
pub mod test {
    use async_bb8_diesel::AsyncRunQueryDsl;
    use dropshot::HandlerTaskMode;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::Generation;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::DataStore;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::internal_api::params as nexus_params;
    use nexus_types::internal_api::params::ServiceKind;
    use omicron_common::api::external::DataPageParams;
    use omicron_test_utils::dev::poll;
    use std::net::SocketAddr;
    use std::num::NonZeroU32;
    use std::time::Duration;
    use tempfile::TempDir;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    // Nexus is supposed to automatically propagate DNS configuration to all the
    // DNS servers it knows about.  We'll test two things here:
    //
    // (1) create a new DNS server and ensure that it promptly gets the
    //     existing DNS configuration
    //
    // (2) create a new configuration and ensure that both servers promptly get
    //     the new DNS configuration
    #[nexus_test(server = crate::Server)]
    async fn test_dns_propagation_basic(cptestctx: &ControlPlaneTestContext) {
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
        let initial_dns_dropshot_server =
            &cptestctx.internal_dns.dropshot_server;
        let dns_config_client = dns_service_client::Client::new(
            &format!("http://{}", initial_dns_dropshot_server.local_addr()),
            cptestctx.logctx.log.clone(),
        );
        let config = dns_config_client
            .dns_config_get()
            .await
            .expect("failed to get initial DNS server config");
        assert_eq!(config.generation, 1);

        // We'll need the id of the internal DNS zone.
        let internal_dns_zone_id =
            read_internal_dns_zone_id(&opctx, datastore).await;

        // Now spin up another DNS server, add it to the list of servers, and
        // make sure that DNS gets propagated to it.  Note that we shouldn't
        // have to explicitly activate the background task because inserting a
        // new service ought to do that for us.
        let log = &cptestctx.logctx.log;
        let storage_path =
            TempDir::new().expect("Failed to create temporary directory");
        let config_store = dns_server::storage::Config {
            keep_old_generations: 3,
            storage_path: storage_path
                .path()
                .to_string_lossy()
                .into_owned()
                .into(),
        };
        let store = dns_server::storage::Store::new(
            log.new(o!("component" => "DnsStore")),
            &config_store,
        )
        .unwrap();

        let (_, new_dns_dropshot_server) = dns_server::start_servers(
            log.clone(),
            store,
            &dns_server::dns_server::Config {
                bind_address: "[::1]:0".parse().unwrap(),
            },
            &dropshot::ConfigDropshot {
                bind_address: "[::1]:0".parse().unwrap(),
                request_body_max_bytes: 8 * 1024,
                default_handler_task_mode: HandlerTaskMode::Detached,
            },
        )
        .await
        .unwrap();

        let new_dns_addr = match new_dns_dropshot_server.local_addr() {
            SocketAddr::V4(_) => panic!("expected v6 address"),
            SocketAddr::V6(a) => a,
        };
        nexus
            .upsert_service(
                &opctx,
                Uuid::new_v4(),
                cptestctx.sled_agent.sled_agent.id,
                Some(Uuid::new_v4()),
                new_dns_addr,
                ServiceKind::InternalDns.into(),
            )
            .await
            .unwrap();

        wait_propagate_dns(
            &cptestctx.logctx.log,
            "new",
            new_dns_dropshot_server.local_addr(),
            1,
        )
        .await;

        // Now, write version 2 of the internal DNS configuration with one
        // additional record.
        write_test_dns_generation(datastore, internal_dns_zone_id).await;

        // Activate the internal DNS propagation pipeline.
        nexus
            .background_tasks
            .activate(&nexus.background_tasks.task_internal_dns_config);

        // Wait for the new generation to get propagated to both servers.
        wait_propagate_dns(
            &cptestctx.logctx.log,
            "initial",
            initial_dns_dropshot_server.local_addr(),
            2,
        )
        .await;

        wait_propagate_dns(
            &cptestctx.logctx.log,
            "new",
            new_dns_dropshot_server.local_addr(),
            2,
        )
        .await;
    }

    /// Verify that DNS gets propagated to the specified server
    async fn wait_propagate_dns(
        log: &slog::Logger,
        label: &str,
        addr: SocketAddr,
        generation: u64,
    ) {
        println!(
            "waiting for propagation of generation {} to {} DNS server ({})",
            generation, label, addr
        );

        let client = dns_service_client::Client::new(
            &format!("http://{}", addr),
            log.clone(),
        );
        poll::wait_for_condition(
            || async {
                match client.dns_config_get().await {
                    Err(error) => {
                        // The DNS server is already up.  This shouldn't
                        // happen.
                        Err(poll::CondCheckError::Failed(error))
                    }
                    Ok(config) => {
                        if config.generation == generation {
                            Ok(())
                        } else {
                            Err(poll::CondCheckError::NotYet)
                        }
                    }
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(30),
        )
        .await
        .expect("DNS config not propagated in expected time");
    }

    pub(crate) async fn write_test_dns_generation(
        datastore: &DataStore,
        internal_dns_zone_id: Uuid,
    ) {
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let _: Result<(), _> = datastore
                .transaction_retry_wrapper("write_test_dns_generation")
                .transaction(&conn, |conn| async move {
                    {
                        use nexus_db_queries::db::model::DnsVersion;
                        use nexus_db_queries::db::schema::dns_version::dsl;

                        diesel::insert_into(dsl::dns_version)
                            .values(DnsVersion {
                                dns_group: DnsGroup::Internal,
                                version: Generation(2u32.try_into().unwrap()),
                                time_created: chrono::Utc::now(),
                                creator: String::from("test suite"),
                                comment: String::from("test suite"),
                            })
                            .execute_async(&conn)
                            .await
                            .unwrap();
                    }

                    {
                        use nexus_db_queries::db::model::DnsName;
                        use nexus_db_queries::db::schema::dns_name::dsl;

                        diesel::insert_into(dsl::dns_name)
                            .values(
                                DnsName::new(
                                    internal_dns_zone_id,
                                    String::from("we-got-beets"),
                                    Generation(2u32.try_into().unwrap()),
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
    }

    pub(crate) async fn read_internal_dns_zone_id(
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Uuid {
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
        dns_zones[0].id
    }
}
