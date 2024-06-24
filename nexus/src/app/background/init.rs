// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Specific background task initialization

use super::tasks::abandoned_vmm_reaper;
use super::tasks::bfd;
use super::tasks::blueprint_execution;
use super::tasks::blueprint_load;
use super::tasks::crdb_node_id_collector;
use super::tasks::dns_config;
use super::tasks::dns_propagation;
use super::tasks::dns_servers;
use super::tasks::external_endpoints;
use super::tasks::instance_watcher;
use super::tasks::inventory_collection;
use super::tasks::metrics_producer_gc;
use super::tasks::nat_cleanup;
use super::tasks::phantom_disks;
use super::tasks::physical_disk_adoption;
use super::tasks::region_replacement;
use super::tasks::service_firewall_rules;
use super::tasks::sync_service_zone_nat::ServiceZoneNatTracker;
use super::tasks::sync_switch_configuration::SwitchPortSettingsManager;
use super::tasks::v2p_mappings::V2PManager;
use super::Driver;
use super::TaskHandle;
use crate::app::oximeter::PRODUCER_LEASE_DURATION;
use crate::app::sagas::SagaRequest;
use nexus_config::BackgroundTaskConfig;
use nexus_config::DnsTasksConfig;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use oximeter::types::ProducerRegistry;
use std::collections::BTreeMap;
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
    pub driver: Driver,

    /// task handle for the internal DNS config background task
    pub task_internal_dns_config: TaskHandle,
    /// task handle for the internal DNS servers background task
    pub task_internal_dns_servers: TaskHandle,
    /// task handle for the external DNS config background task
    pub task_external_dns_config: TaskHandle,
    /// task handle for the external DNS servers background task
    pub task_external_dns_servers: TaskHandle,

    /// task handle for pruning metrics producers with expired leases
    pub task_metrics_producer_gc: TaskHandle,

    /// task handle for the task that keeps track of external endpoints
    pub task_external_endpoints: TaskHandle,
    /// external endpoints read by the background task
    pub external_endpoints: tokio::sync::watch::Receiver<
        Option<external_endpoints::ExternalEndpoints>,
    >,
    /// task handle for the ipv4 nat entry garbage collector
    pub task_nat_cleanup: TaskHandle,

    /// task handle for the switch bfd manager
    pub task_bfd_manager: TaskHandle,

    /// task handle for the task that collects inventory
    pub task_inventory_collection: TaskHandle,

    /// task handle for the task that collects inventory
    pub task_physical_disk_adoption: TaskHandle,

    /// task handle for the task that detects phantom disks
    pub task_phantom_disks: TaskHandle,

    /// task handle for blueprint target loader
    pub task_blueprint_loader: TaskHandle,

    /// task handle for blueprint execution background task
    pub task_blueprint_executor: TaskHandle,

    /// task handle for collecting CockroachDB node IDs
    pub task_crdb_node_id_collector: TaskHandle,

    /// task handle for the service zone nat tracker
    pub task_service_zone_nat_tracker: TaskHandle,

    /// task handle for the switch port settings manager
    pub task_switch_port_settings_manager: TaskHandle,

    /// task handle for the opte v2p manager
    pub task_v2p_manager: TaskHandle,

    /// task handle for the task that detects if regions need replacement and
    /// begins the process
    pub task_region_replacement: TaskHandle,

    /// task handle for the task that polls sled agents for instance states.
    pub task_instance_watcher: TaskHandle,

    /// task handle for propagation of VPC firewall rules for Omicron services
    /// with external network connectivity,
    pub task_service_firewall_propagation: TaskHandle,

    /// task handle for deletion of database records for VMMs abandoned by their
    /// instances.
    pub task_abandoned_vmm_reaper: TaskHandle,
}

impl BackgroundTasks {
    /// Kick off all background tasks
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        opctx: &OpContext,
        datastore: Arc<DataStore>,
        config: &BackgroundTaskConfig,
        rack_id: Uuid,
        nexus_id: Uuid,
        resolver: internal_dns::resolver::Resolver,
        saga_request: Sender<SagaRequest>,
        v2p_watcher: (
            tokio::sync::watch::Sender<()>,
            tokio::sync::watch::Receiver<()>,
        ),
        producer_registry: &ProducerRegistry,
    ) -> BackgroundTasks {
        let mut driver = Driver::new();

        let (task_internal_dns_config, task_internal_dns_servers) = init_dns(
            &mut driver,
            opctx,
            datastore.clone(),
            DnsGroup::Internal,
            resolver.clone(),
            &config.dns_internal,
        );
        let (task_external_dns_config, task_external_dns_servers) = init_dns(
            &mut driver,
            opctx,
            datastore.clone(),
            DnsGroup::External,
            resolver.clone(),
            &config.dns_external,
        );

        let task_metrics_producer_gc = {
            let gc = metrics_producer_gc::MetricProducerGc::new(
                datastore.clone(),
                PRODUCER_LEASE_DURATION,
            );
            driver.register(
                String::from("metrics_producer_gc"),
                String::from(
                    "unregisters Oximeter metrics producers that have not \
                     renewed their lease",
                ),
                config.metrics_producer_gc.period_secs,
                Box::new(gc),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };

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
                     the right set of HTTP endpoints, their HTTP server \
                     names, and which TLS certificates to use on each one",
                ),
                config.external_endpoints.period_secs,
                Box::new(watcher),
                opctx.child(BTreeMap::new()),
                vec![],
            );
            (task, watcher_channel)
        };

        let task_nat_cleanup = {
            driver.register(
                "nat_v4_garbage_collector".to_string(),
                String::from(
                    "prunes soft-deleted IPV4 NAT entries from ipv4_nat_entry \
                     table based on a predetermined retention policy",
                ),
                config.nat_cleanup.period_secs,
                Box::new(nat_cleanup::Ipv4NatGarbageCollector::new(
                    datastore.clone(),
                    resolver.clone(),
                )),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };

        let task_bfd_manager = {
            driver.register(
                "bfd_manager".to_string(),
                String::from(
                    "Manages bidirectional fowarding detection (BFD) \
                     configuration on rack switches",
                ),
                config.bfd_manager.period_secs,
                Box::new(bfd::BfdManager::new(
                    datastore.clone(),
                    resolver.clone(),
                )),
                opctx.child(BTreeMap::new()),
                vec![],
            )
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
            nexus_id.to_string(),
        );
        let rx_blueprint_exec = blueprint_executor.watcher();
        let task_blueprint_executor = driver.register(
            String::from("blueprint_executor"),
            String::from("Executes the target blueprint"),
            config.blueprints.period_secs_execute,
            Box::new(blueprint_executor),
            opctx.child(BTreeMap::new()),
            vec![Box::new(rx_blueprint.clone())],
        );

        // Background task: CockroachDB node ID collector
        let crdb_node_id_collector =
            crdb_node_id_collector::CockroachNodeIdCollector::new(
                datastore.clone(),
                rx_blueprint.clone(),
            );
        let task_crdb_node_id_collector = driver.register(
            String::from("crdb_node_id_collector"),
            String::from("Collects node IDs of running CockroachDB zones"),
            config.blueprints.period_secs_collect_crdb_node_ids,
            Box::new(crdb_node_id_collector),
            opctx.child(BTreeMap::new()),
            vec![Box::new(rx_blueprint)],
        );

        // Background task: inventory collector
        //
        // This currently depends on the "output" of the blueprint executor in
        // order to automatically trigger inventory collection whenever the
        // blueprint executor runs.  In the limit, this could become a problem
        // because the blueprint executor might also depend indirectly on the
        // inventory collector.  In that case, we may need to do something more
        // complicated.  But for now, this works.
        let (task_inventory_collection, inventory_watcher) = {
            let collector = inventory_collection::InventoryCollector::new(
                datastore.clone(),
                resolver.clone(),
                &nexus_id.to_string(),
                config.inventory.nkeep,
                config.inventory.disable,
            );
            let inventory_watcher = collector.watcher();
            let task = driver.register(
                String::from("inventory_collection"),
                String::from(
                    "collects hardware and software inventory data from the \
                     whole system",
                ),
                config.inventory.period_secs,
                Box::new(collector),
                opctx.child(BTreeMap::new()),
                vec![Box::new(rx_blueprint_exec)],
            );

            (task, inventory_watcher)
        };

        let task_physical_disk_adoption = {
            driver.register(
                "physical_disk_adoption".to_string(),
                "ensure new physical disks are automatically marked in-service"
                    .to_string(),
                config.physical_disk_adoption.period_secs,
                Box::new(physical_disk_adoption::PhysicalDiskAdoption::new(
                    datastore.clone(),
                    inventory_watcher.clone(),
                    config.physical_disk_adoption.disable,
                    rack_id,
                )),
                opctx.child(BTreeMap::new()),
                vec![Box::new(inventory_watcher)],
            )
        };

        let task_service_zone_nat_tracker = {
            driver.register(
                "service_zone_nat_tracker".to_string(),
                String::from(
                    "ensures service zone nat records are recorded in NAT RPW \
                     table",
                ),
                config.sync_service_zone_nat.period_secs,
                Box::new(ServiceZoneNatTracker::new(
                    datastore.clone(),
                    resolver.clone(),
                )),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };

        let task_switch_port_settings_manager = {
            driver.register(
                "switch_port_config_manager".to_string(),
                String::from("manages switch port settings for rack switches"),
                config.switch_port_settings_manager.period_secs,
                Box::new(SwitchPortSettingsManager::new(
                    datastore.clone(),
                    resolver.clone(),
                )),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };

        let task_v2p_manager = {
            driver.register(
                "v2p_manager".to_string(),
                String::from("manages opte v2p mappings for vpc networking"),
                config.v2p_mapping_propagation.period_secs,
                Box::new(V2PManager::new(datastore.clone())),
                opctx.child(BTreeMap::new()),
                vec![Box::new(v2p_watcher.1)],
            )
        };

        // Background task: detect if a region needs replacement and begin the
        // process
        let task_region_replacement = {
            let detector = region_replacement::RegionReplacementDetector::new(
                datastore.clone(),
                saga_request.clone(),
            );

            let task = driver.register(
                String::from("region_replacement"),
                String::from(
                    "detects if a region requires replacing and begins the \
                     process",
                ),
                config.region_replacement.period_secs,
                Box::new(detector),
                opctx.child(BTreeMap::new()),
                vec![],
            );

            task
        };

        let task_instance_watcher = {
            let watcher = instance_watcher::InstanceWatcher::new(
                datastore.clone(),
                resolver.clone(),
                producer_registry,
                instance_watcher::WatcherIdentity { nexus_id, rack_id },
                v2p_watcher.0,
            );
            driver.register(
                "instance_watcher".to_string(),
                "periodically checks instance states".to_string(),
                config.instance_watcher.period_secs,
                Box::new(watcher),
                opctx.child(BTreeMap::new()),
                vec![],
            )
        };
        // Background task: service firewall rule propagation
        let task_service_firewall_propagation = driver.register(
            String::from("service_firewall_rule_propagation"),
            String::from(
                "propagates VPC firewall rules for Omicron services with \
                 external network connectivity",
            ),
            config.service_firewall_propagation.period_secs,
            Box::new(service_firewall_rules::ServiceRulePropagator::new(
                datastore.clone(),
            )),
            opctx.child(BTreeMap::new()),
            vec![],
        );

        // Background task: abandoned VMM reaping
        let task_abandoned_vmm_reaper = driver.register(
            String::from("abandoned_vmm_reaper"),
            String::from(
                "deletes sled reservations for VMMs that have been abandoned \
                 by their instances",
            ),
            config.abandoned_vmm_reaper.period_secs,
            Box::new(abandoned_vmm_reaper::AbandonedVmmReaper::new(datastore)),
            opctx.child(BTreeMap::new()),
            vec![],
        );

        BackgroundTasks {
            driver,
            task_internal_dns_config,
            task_internal_dns_servers,
            task_external_dns_config,
            task_external_dns_servers,
            task_metrics_producer_gc,
            task_external_endpoints,
            external_endpoints,
            task_nat_cleanup,
            task_bfd_manager,
            task_inventory_collection,
            task_physical_disk_adoption,
            task_phantom_disks,
            task_blueprint_loader,
            task_blueprint_executor,
            task_crdb_node_id_collector,
            task_service_zone_nat_tracker,
            task_switch_port_settings_manager,
            task_v2p_manager,
            task_region_replacement,
            task_instance_watcher,
            task_service_firewall_propagation,
            task_abandoned_vmm_reaper,
        }
    }

    /// Activate the specified background task
    ///
    /// If the task is currently running, it will be activated again when it
    /// finishes.
    pub fn activate(&self, task: &TaskHandle) {
        self.driver.activate(task);
    }
}

fn init_dns(
    driver: &mut Driver,
    opctx: &OpContext,
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    resolver: internal_dns::resolver::Resolver,
    config: &DnsTasksConfig,
) -> (TaskHandle, TaskHandle) {
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
    let dns_servers = dns_servers::DnsServersWatcher::new(dns_group, resolver);
    let dns_servers_watcher = dns_servers.watcher();
    let task_name_servers = format!("dns_servers_{}", dns_group);
    let task_servers = driver.register(
        task_name_servers.clone(),
        format!(
            "watches list of {} DNS servers stored in internal DNS",
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
    use dropshot::HandlerTaskMode;
    use nexus_db_model::DnsGroup;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
    use nexus_db_queries::db::DataStore;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::internal_api::params as nexus_params;
    use omicron_test_utils::dev::poll;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tempfile::TempDir;

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
        let nexus = &cptestctx.server.server_context().nexus;
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

        let internal_dns_srv_name =
            internal_dns::ServiceName::InternalDns.dns_name();

        let initial_srv_record = {
            let zone =
                config.zones.get(0).expect("DNS config must have a zone");
            let Some(record) = zone.records.get(&internal_dns_srv_name) else {
                panic!("zone must have a record for {internal_dns_srv_name}")
            };
            match record.get(0) {
                Some(dns_service_client::types::DnsRecord::Srv(srv)) => srv,
                record => panic!(
                    "expected a SRV record for {internal_dns_srv_name}, found \
                     {record:?}"
                ),
            }
        };

        // Now spin up another DNS server, add it to the list of servers, and
        // make sure that DNS gets propagated to it. Note that we shouldn't
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

        // In order to test that DNS gets propagated to a newly-added server, we
        // first need to update the source of truth about DNS (the database).
        // Then we need to wait for that to get propagated (by this same
        // mechanism) to the existing DNS servers.  Only then would we expect
        // the mechanism to see the new DNS server and then propagate
        // configuration to it.
        let update = {
            use nexus_params::{DnsRecord, Srv};

            let target = "my-great-dns-server.host";

            let mut update = test_dns_update_builder();
            update.remove_name(internal_dns_srv_name.clone()).unwrap();
            update
                .add_name(
                    internal_dns_srv_name,
                    vec![
                        DnsRecord::Srv(Srv {
                            prio: 0,
                            weight: 0,
                            port: new_dns_addr.port(),
                            target: format!(
                                "{target}.control-plane.oxide.internal"
                            ),
                        }),
                        DnsRecord::Srv(initial_srv_record.clone()),
                    ],
                )
                .unwrap();
            update
                .add_name(
                    target.to_string(),
                    vec![DnsRecord::Aaaa(*new_dns_addr.ip())],
                )
                .unwrap();
            update
        };
        write_dns_update(&opctx, datastore, update).await;
        info!(&cptestctx.logctx.log, "updated new dns records");

        // Activate the internal DNS propagation pipeline.
        nexus
            .background_tasks
            .activate(&nexus.background_tasks.task_internal_dns_config);

        wait_propagate_dns(
            &cptestctx.logctx.log,
            "initial",
            initial_dns_dropshot_server.local_addr(),
            2,
        )
        .await;

        // Discover the new internal DNS server from internal DNS.
        nexus
            .background_tasks
            .activate(&nexus.background_tasks.task_internal_dns_servers);

        wait_propagate_dns(
            &cptestctx.logctx.log,
            "new",
            new_dns_dropshot_server.local_addr(),
            2,
        )
        .await;

        // Now, write version 3 of the internal DNS configuration with one
        // additional record.
        write_test_dns_generation(&opctx, datastore).await;

        // Activate the internal DNS propagation pipeline.
        nexus
            .background_tasks
            .activate(&nexus.background_tasks.task_internal_dns_config);

        // Wait for the new generation to get propagated to both servers.
        wait_propagate_dns(
            &cptestctx.logctx.log,
            "initial",
            initial_dns_dropshot_server.local_addr(),
            3,
        )
        .await;

        wait_propagate_dns(
            &cptestctx.logctx.log,
            "new",
            new_dns_dropshot_server.local_addr(),
            3,
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
            "waiting for propagation of generation {generation} to {label} \
             DNS server ({addr})",
        );

        let client = dns_service_client::Client::new(
            &format!("http://{}", addr),
            log.clone(),
        );
        let poll_max = Duration::from_secs(30);
        let result = poll::wait_for_condition(
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
            &poll_max,
        )
        .await;
        if let Err(err) = result {
            panic!(
                "DNS generation {generation} not propagated to {label} DNS \
                 server ({addr}) within {poll_max:?}: {err}"
            );
        } else {
            println!(
                "DNS generation {generation} propagated to {label} DNS server \
                 ({addr}) successfully."
            );
        }
    }

    pub(crate) async fn write_dns_update(
        opctx: &OpContext,
        datastore: &DataStore,
        update: DnsVersionUpdateBuilder,
    ) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        info!(opctx.log, "writing DNS update...");
        datastore.dns_update_incremental(opctx, &conn, update).await.unwrap();
    }

    pub(crate) async fn write_test_dns_generation(
        opctx: &OpContext,
        datastore: &DataStore,
    ) {
        let mut update = test_dns_update_builder();
        update
            .add_name(
                "we-got-beets".to_string(),
                vec![nexus_params::DnsRecord::Aaaa("fe80::3".parse().unwrap())],
            )
            .unwrap();
        write_dns_update(opctx, datastore, update).await
    }

    fn test_dns_update_builder() -> DnsVersionUpdateBuilder {
        DnsVersionUpdateBuilder::new(
            DnsGroup::Internal,
            "test suite DNS update".to_string(),
            "test suite".to_string(),
        )
    }
}
