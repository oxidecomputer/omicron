// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Initialize Nexus background tasks
//!
//! This file contains entirely Nexus-specific initialization (as opposed to
//! driver.rs, which doesn't really know much about Nexus).
//!
//! The design here is oriented around being able to initialize background tasks
//! in two phases:
//!
//! 1. Phase 1 assembles a `BackgroundTasks` struct containing `Activator`
//!    objects that will be used by any part of Nexus (including background
//!    tasks) to activate any background task or read data provided by another
//!    background task.  This is the interface between this subsystem and the
//!    rest of Nexus.  At this point in startup, none of the background tasks
//!    themselves have been started yet.
//!
//! 2. Phase 2 starts all of the individual background tasks and then wires up
//!    the `Activator`s created in phase 1.
//!
//! This allows us to break what would otherwise be a circular dependency during
//! initialization.  Concretely: Nexus startup does phase 1, stores the
//! `BackgroundTasks` into the `Arc<Nexus>` to which all of Nexus has a
//! reference, and _then_ starts the background tasks.  If we didn't break it up
//! like this, then we couldn't make the `Arc<Nexus>` available to background
//! tasks during _their_ initialization (because it couldn't be constructed
//! yet), which means background tasks could not activate other background
//! tasks.  We'd also have trouble allowing background tasks to use other
//! subsystems in Nexus (e.g., sagas), especially if those subsystems wanted to
//! activate background tasks.
//!
//! Why do we do things this way?  We're trying to satisfy a few different
//! goals:
//!
//! - Background tasks should be able to activate other background tasks.
//! - Background tasks should be able to use other subsystems in Nexus (like
//!   sagas) that themselves can activate background tasks.
//! - It should be hard to mess any of this up when adding or removing
//!   background tasks.  This means:
//!     - We should be able to tell at compile-time which code activates what
//!       specific background tasks.
//!     - We should be able to tell at compile-time if code is attempting to
//!       activate a background task that doesn't exist.
//!     - It should be hard to add an `Activator` for a background task that is
//!       not wired up to that task or is wired up to a different task.
//!
//! Ultimately, tasks are activated via the `Driver` which keeps track of tasks
//! by name.  So how can we have code paths in Nexus refer to tasks in a way
//! that satisfies these goals?  A conventional approach would be to have
//! `Driver::register()` return a handle that could be used to activate the
//! task, but then we wouldn't have the handle available until the task was
//! running, which is too late -- see the note above about the circular
//! dependency during initialization.  We could make the task identifiers global
//! constants, but this is easy to mess up: someone could remove the task
//! without removing its constant.  Then code paths could appear to activate the
//! task but fail at _runtime_ (rather than compile-time) because the task
//! actually doesn't exist.
//!
//! Instead, we assemble the `BackgroundTasks` struct, whose fields correspond
//! to specific tasks.  This makes it super explicit what code paths are using
//! which tasks.  And since the `Activator`s in the struct can be created before
//! the tasks are created, we can create this whole struct and pass it to all
//! the background tasks (and anybody else that wants to activate background
//! tasks), even though the actual tasks aren't wired up yet.  Then we can wire
//! it up behind the scenes.  If someone uses the activators ahead of time,
//! they'll get the expected behavior: the task will be activated shortly.
//!
//! There remain several ways someone could get this wrong when adding or
//! reworking background tasks:
//!
//! - Forgetting to put an `Activator` for a background task into
//!   `BackgroundTasks`.  If you make this mistake, you won't get far because
//!   you won't have the argument you need for `Driver::register()`.
//! - Forgetting to wire up an `Activator` by passing it to
//!   `Driver::register()`.  We attempt to avoid this with an exhaustive match
//!   inside `BackgroundTasksInitializer::start()`.  If you forget to wire
//!   something up, rustc should report an unused variable.
//! - Wiring the `Activator` up to the wrong task (e.g., by copying and pasting
//!   a `Driver::register()` call and forgetting to update the activator
//!   argument).  If this happens, it's likely that either one `Activator` gets
//!   used more than once (which is caught with a panic only at runtime, but
//!   it _is_ during Nexus initialization, so it should definitely be caught in
//!   testing) or else some `Activator` is unused (see the previous bullet).
//!
//! It's not foolproof but hopefully these mechanisms will catch the easy
//! mistakes.

use super::Driver;
use super::driver::TaskDefinition;
use super::tasks::abandoned_vmm_reaper;
use super::tasks::alert_dispatcher::AlertDispatcher;
use super::tasks::bfd;
use super::tasks::blueprint_execution;
use super::tasks::blueprint_load;
use super::tasks::blueprint_rendezvous;
use super::tasks::crdb_node_id_collector;
use super::tasks::decommissioned_disk_cleaner;
use super::tasks::dns_config;
use super::tasks::dns_propagation;
use super::tasks::dns_servers;
use super::tasks::external_endpoints;
use super::tasks::instance_reincarnation;
use super::tasks::instance_updater;
use super::tasks::instance_watcher;
use super::tasks::inventory_collection;
use super::tasks::lookup_region_port;
use super::tasks::metrics_producer_gc;
use super::tasks::nat_cleanup;
use super::tasks::phantom_disks;
use super::tasks::physical_disk_adoption;
use super::tasks::read_only_region_replacement_start::*;
use super::tasks::region_replacement;
use super::tasks::region_replacement_driver;
use super::tasks::region_snapshot_replacement_finish::*;
use super::tasks::region_snapshot_replacement_garbage_collect::*;
use super::tasks::region_snapshot_replacement_start::*;
use super::tasks::region_snapshot_replacement_step::*;
use super::tasks::saga_recovery;
use super::tasks::service_firewall_rules;
use super::tasks::support_bundle_collector;
use super::tasks::sync_service_zone_nat::ServiceZoneNatTracker;
use super::tasks::sync_switch_configuration::SwitchPortSettingsManager;
use super::tasks::tuf_artifact_replication;
use super::tasks::v2p_mappings::V2PManager;
use super::tasks::vpc_routes;
use super::tasks::webhook_deliverator;
use crate::Nexus;
use crate::app::oximeter::PRODUCER_LEASE_DURATION;
use crate::app::saga::StartSaga;
use nexus_background_task_interface::Activator;
use nexus_background_task_interface::BackgroundTasks;
use nexus_config::BackgroundTaskConfig;
use nexus_config::DnsTasksConfig;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::PendingMgsUpdates;
use omicron_uuid_kinds::OmicronZoneUuid;
use oximeter::types::ProducerRegistry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use update_common::artifacts::ArtifactsWithPlan;
use uuid::Uuid;

/// Internal state for communication between Nexus and background tasks.
///
/// This is not part of the larger `BackgroundTask` type because it contains
/// references to internal types.
pub(crate) struct BackgroundTasksInternal {
    pub(crate) external_endpoints:
        watch::Receiver<Option<external_endpoints::ExternalEndpoints>>,
}

/// Initializes the background task subsystem
///
/// See the module-level documentation for more on the two-phase initialization
/// of this subsystem.
// See the definition of `Activator` for more design notes about this interface.
pub struct BackgroundTasksInitializer {
    driver: Driver,
    external_endpoints_tx:
        watch::Sender<Option<external_endpoints::ExternalEndpoints>>,
}

impl BackgroundTasksInitializer {
    /// Begin initializing the Nexus background task subsystem
    ///
    /// This step does not start any background tasks.  It just returns:
    ///
    /// * a short-lived `BackgroundTasksInitializer` object, on which you can
    ///   call `start()` to actually start the tasks
    /// * a long-lived `BackgroundTasks` object that you can use to activate any
    ///   of the tasks that will be started and read data that they provide
    pub fn new()
    -> (BackgroundTasksInitializer, BackgroundTasks, BackgroundTasksInternal)
    {
        let (external_endpoints_tx, external_endpoints_rx) =
            watch::channel(None);

        let initializer = BackgroundTasksInitializer {
            driver: Driver::new(),
            external_endpoints_tx,
        };

        let background_tasks = BackgroundTasks {
            task_internal_dns_config: Activator::new(),
            task_internal_dns_servers: Activator::new(),
            task_external_dns_config: Activator::new(),
            task_external_dns_servers: Activator::new(),
            task_metrics_producer_gc: Activator::new(),
            task_external_endpoints: Activator::new(),
            task_nat_cleanup: Activator::new(),
            task_bfd_manager: Activator::new(),
            task_inventory_collection: Activator::new(),
            task_support_bundle_collector: Activator::new(),
            task_physical_disk_adoption: Activator::new(),
            task_decommissioned_disk_cleaner: Activator::new(),
            task_phantom_disks: Activator::new(),
            task_blueprint_loader: Activator::new(),
            task_blueprint_executor: Activator::new(),
            task_blueprint_rendezvous: Activator::new(),
            task_crdb_node_id_collector: Activator::new(),
            task_service_zone_nat_tracker: Activator::new(),
            task_switch_port_settings_manager: Activator::new(),
            task_v2p_manager: Activator::new(),
            task_region_replacement: Activator::new(),
            task_region_replacement_driver: Activator::new(),
            task_instance_watcher: Activator::new(),
            task_instance_updater: Activator::new(),
            task_instance_reincarnation: Activator::new(),
            task_service_firewall_propagation: Activator::new(),
            task_abandoned_vmm_reaper: Activator::new(),
            task_vpc_route_manager: Activator::new(),
            task_saga_recovery: Activator::new(),
            task_lookup_region_port: Activator::new(),
            task_region_snapshot_replacement_start: Activator::new(),
            task_region_snapshot_replacement_garbage_collection: Activator::new(
            ),
            task_region_snapshot_replacement_step: Activator::new(),
            task_region_snapshot_replacement_finish: Activator::new(),
            task_tuf_artifact_replication: Activator::new(),
            task_read_only_region_replacement_start: Activator::new(),
            task_alert_dispatcher: Activator::new(),
            task_webhook_deliverator: Activator::new(),

            task_internal_dns_propagation: Activator::new(),
            task_external_dns_propagation: Activator::new(),
        };

        let internal = BackgroundTasksInternal {
            external_endpoints: external_endpoints_rx,
        };

        (initializer, background_tasks, internal)
    }

    /// Starts all the Nexus background tasks
    ///
    /// This function will wire up the `Activator`s in `background_tasks` to the
    /// corresponding tasks once they've been started.
    pub fn start(
        self,
        background_tasks: &'_ BackgroundTasks,
        args: BackgroundTasksData,
    ) -> Driver {
        let mut driver = self.driver;
        let opctx = &args.opctx;
        let datastore = args.datastore;
        let config = args.config;
        let rack_id = args.rack_id;
        let nexus_id = args.nexus_id;
        let resolver = args.resolver;
        let sagas = args.saga_starter;
        let producer_registry = &args.producer_registry;

        // This "let" construction helps catch mistakes where someone forgets to
        // wire up an activator to its corresponding background task.
        let BackgroundTasks {
            task_internal_dns_config,
            task_internal_dns_servers,
            task_internal_dns_propagation,
            task_external_dns_config,
            task_external_dns_servers,
            task_external_dns_propagation,
            task_metrics_producer_gc,
            task_external_endpoints,
            task_nat_cleanup,
            task_bfd_manager,
            task_inventory_collection,
            task_support_bundle_collector,
            task_physical_disk_adoption,
            task_decommissioned_disk_cleaner,
            task_phantom_disks,
            task_blueprint_loader,
            task_blueprint_executor,
            task_blueprint_rendezvous,
            task_crdb_node_id_collector,
            task_service_zone_nat_tracker,
            task_switch_port_settings_manager,
            task_v2p_manager,
            task_region_replacement,
            task_region_replacement_driver,
            task_instance_watcher,
            task_instance_updater,
            task_instance_reincarnation,
            task_service_firewall_propagation,
            task_abandoned_vmm_reaper,
            task_vpc_route_manager,
            task_saga_recovery,
            task_lookup_region_port,
            task_region_snapshot_replacement_start,
            task_region_snapshot_replacement_garbage_collection,
            task_region_snapshot_replacement_step,
            task_region_snapshot_replacement_finish,
            task_tuf_artifact_replication,
            task_read_only_region_replacement_start,
            task_alert_dispatcher,
            task_webhook_deliverator,
            // Add new background tasks here.  Be sure to use this binding in a
            // call to `Driver::register()` below.  That's what actually wires
            // up the Activator to the corresponding background task.

            // Do NOT add a `..` catch-all here!  See above.
        } = &background_tasks;

        init_dns(
            &mut driver,
            opctx,
            datastore.clone(),
            DnsGroup::Internal,
            resolver.clone(),
            &config.dns_internal,
            task_internal_dns_config,
            task_internal_dns_servers,
            task_internal_dns_propagation,
        );

        init_dns(
            &mut driver,
            opctx,
            datastore.clone(),
            DnsGroup::External,
            resolver.clone(),
            &config.dns_external,
            task_external_dns_config,
            task_external_dns_servers,
            task_external_dns_propagation,
        );

        {
            let gc = metrics_producer_gc::MetricProducerGc::new(
                datastore.clone(),
                PRODUCER_LEASE_DURATION,
            );

            driver.register(TaskDefinition {
                name: "metrics_producer_gc",
                description:
                    "unregisters Oximeter metrics producers that have not \
                     renewed their lease",
                period: config.metrics_producer_gc.period_secs,
                task_impl: Box::new(gc),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_metrics_producer_gc,
            })
        };

        // Background task: External endpoints list watcher
        {
            let watcher = external_endpoints::ExternalEndpointsWatcher::new(
                datastore.clone(),
                self.external_endpoints_tx,
            );
            driver.register(TaskDefinition {
                name: "external_endpoints",
                description:
                    "reads config for silos and TLS certificates to determine \
                     the right set of HTTP endpoints, their HTTP server \
                     names, and which TLS certificates to use on each one",
                period: config.external_endpoints.period_secs,
                task_impl: Box::new(watcher),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_external_endpoints,
            });
        }

        driver.register(TaskDefinition {
            name: "nat_v4_garbage_collector",
            description:
                "prunes soft-deleted IPV4 NAT entries from ipv4_nat_entry \
                 table based on a predetermined retention policy",
            period: config.nat_cleanup.period_secs,
            task_impl: Box::new(nat_cleanup::Ipv4NatGarbageCollector::new(
                datastore.clone(),
                resolver.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_nat_cleanup,
        });

        driver.register(TaskDefinition {
            name: "bfd_manager",
            description: "Manages bidirectional fowarding detection (BFD) \
                 configuration on rack switches",
            period: config.bfd_manager.period_secs,
            task_impl: Box::new(bfd::BfdManager::new(
                datastore.clone(),
                resolver.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_bfd_manager,
        });

        // Background task: phantom disk detection
        {
            let detector =
                phantom_disks::PhantomDiskDetector::new(datastore.clone());
            driver.register(TaskDefinition {
                name: "phantom_disks",
                description: "detects and un-deletes phantom disks",
                period: config.phantom_disks.period_secs,
                task_impl: Box::new(detector),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_phantom_disks,
            });
        };

        // Background task: blueprint loader
        let blueprint_loader =
            blueprint_load::TargetBlueprintLoader::new(datastore.clone());
        let rx_blueprint = blueprint_loader.watcher();
        driver.register(TaskDefinition {
            name: "blueprint_loader",
            description: "Loads the current target blueprint from the DB",
            period: config.blueprints.period_secs_load,
            task_impl: Box::new(blueprint_loader),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_blueprint_loader,
        });

        // Background task: blueprint executor
        let blueprint_executor = blueprint_execution::BlueprintExecutor::new(
            datastore.clone(),
            resolver.clone(),
            rx_blueprint.clone(),
            nexus_id,
            task_saga_recovery.clone(),
            args.mgs_updates_tx,
        );
        let rx_blueprint_exec = blueprint_executor.watcher();
        driver.register(TaskDefinition {
            name: "blueprint_executor",
            description: "Executes the target blueprint",
            period: config.blueprints.period_secs_execute,
            task_impl: Box::new(blueprint_executor),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![Box::new(rx_blueprint.clone())],
            activator: task_blueprint_executor,
        });

        // Background task: CockroachDB node ID collector
        let crdb_node_id_collector =
            crdb_node_id_collector::CockroachNodeIdCollector::new(
                datastore.clone(),
                rx_blueprint.clone(),
            );
        driver.register(TaskDefinition {
            name: "crdb_node_id_collector",
            description: "Collects node IDs of running CockroachDB zones",
            period: config.blueprints.period_secs_collect_crdb_node_ids,
            task_impl: Box::new(crdb_node_id_collector),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![Box::new(rx_blueprint.clone())],
            activator: task_crdb_node_id_collector,
        });

        // Background task: inventory collector
        //
        // This depends on the "output" of the blueprint executor in
        // order to automatically trigger inventory collection whenever the
        // blueprint executor runs.
        let inventory_watcher = {
            let collector = inventory_collection::InventoryCollector::new(
                datastore.clone(),
                resolver.clone(),
                &nexus_id.to_string(),
                config.inventory.nkeep,
                config.inventory.disable,
            );
            let inventory_watcher = collector.watcher();
            driver.register(TaskDefinition {
                name: "inventory_collection",
                description:
                    "collects hardware and software inventory data from the \
                     whole system",
                period: config.inventory.period_secs,
                task_impl: Box::new(collector),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![Box::new(rx_blueprint_exec.clone())],
                activator: task_inventory_collection,
            });

            inventory_watcher
        };

        // Cleans up and collects support bundles.
        //
        // This task is triggered by blueprint execution, since blueprint
        // execution may cause bundles to start failing and need garbage
        // collection.
        driver.register(TaskDefinition {
            name: "support_bundle_collector",
            description: "Manage support bundle collection and cleanup",
            period: config.support_bundle_collector.period_secs,
            task_impl: Box::new(
                support_bundle_collector::SupportBundleCollector::new(
                    datastore.clone(),
                    config.support_bundle_collector.disable,
                    nexus_id,
                ),
            ),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![Box::new(rx_blueprint_exec)],
            activator: task_support_bundle_collector,
        });

        driver.register(TaskDefinition {
            name: "physical_disk_adoption",
            description:
                "ensure new physical disks are automatically marked in-service",
            period: config.physical_disk_adoption.period_secs,
            task_impl: Box::new(
                physical_disk_adoption::PhysicalDiskAdoption::new(
                    datastore.clone(),
                    inventory_watcher.clone(),
                    config.physical_disk_adoption.disable,
                    rack_id,
                ),
            ),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![Box::new(inventory_watcher.clone())],
            activator: task_physical_disk_adoption,
        });

        driver.register(TaskDefinition {
            name: "blueprint_rendezvous",
            description:
                "reconciles blueprints and inventory collection, updating \
                 Reconfigurator-owned rendezvous tables that other subsystems \
                 consume",
            period: config.blueprints.period_secs_rendezvous,
            task_impl: Box::new(
                blueprint_rendezvous::BlueprintRendezvous::new(
                    datastore.clone(),
                    rx_blueprint.clone(),
                ),
            ),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![Box::new(inventory_watcher.clone())],
            activator: task_blueprint_rendezvous,
        });

        driver.register(TaskDefinition {
            name: "decommissioned_disk_cleaner",
            description:
                "deletes DB records for decommissioned disks, after regions \
                 and region snapshots have been replaced",
            period: config.decommissioned_disk_cleaner.period_secs,
            task_impl: Box::new(
                decommissioned_disk_cleaner::DecommissionedDiskCleaner::new(
                    datastore.clone(),
                    config.decommissioned_disk_cleaner.disable,
                ),
            ),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_decommissioned_disk_cleaner,
        });

        driver.register(TaskDefinition {
            name: "service_zone_nat_tracker",
            description:
                "ensures service zone nat records are recorded in NAT RPW \
                 table",
            period: config.sync_service_zone_nat.period_secs,
            task_impl: Box::new(ServiceZoneNatTracker::new(
                datastore.clone(),
                resolver.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_service_zone_nat_tracker,
        });

        driver.register(TaskDefinition {
            name: "switch_port_config_manager",
            description: "manages switch port settings for rack switches",
            period: config.switch_port_settings_manager.period_secs,
            task_impl: Box::new(SwitchPortSettingsManager::new(
                datastore.clone(),
                resolver.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_switch_port_settings_manager,
        });

        driver.register(TaskDefinition {
            name: "v2p_manager",
            description: "manages opte v2p mappings for vpc networking",
            period: config.v2p_mapping_propagation.period_secs,
            task_impl: Box::new(V2PManager::new(datastore.clone())),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_v2p_manager,
        });

        // Background task: detect if a region needs replacement and begin the
        // process
        {
            let detector = region_replacement::RegionReplacementDetector::new(
                datastore.clone(),
                sagas.clone(),
            );

            driver.register(TaskDefinition {
                name: "region_replacement",
                description:
                    "detects if a region requires replacing and begins the \
                     process",
                period: config.region_replacement.period_secs,
                task_impl: Box::new(detector),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_region_replacement,
            });
        };

        // Background task: drive region replacements forward to completion
        {
            let detector =
                region_replacement_driver::RegionReplacementDriver::new(
                    datastore.clone(),
                    sagas.clone(),
                );

            driver.register(TaskDefinition {
                name: "region_replacement_driver",
                description: "drive region replacements forward to completion",
                period: config.region_replacement_driver.period_secs,
                task_impl: Box::new(detector),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_region_replacement_driver,
            });
        };

        {
            let watcher = instance_watcher::InstanceWatcher::new(
                datastore.clone(),
                sagas.clone(),
                producer_registry,
                instance_watcher::WatcherIdentity { nexus_id, rack_id },
            );
            driver.register(TaskDefinition {
                name: "instance_watcher",
                description: "periodically checks instance states",
                period: config.instance_watcher.period_secs,
                task_impl: Box::new(watcher),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_instance_watcher,
            })
        };

        // Background task: schedule update sagas for instances in need of
        // state updates.
        {
            let updater = instance_updater::InstanceUpdater::new(
                datastore.clone(),
                sagas.clone(),
                config.instance_updater.disable,
            );
            driver.register( TaskDefinition {
                name: "instance_updater",
                description: "detects if instances require update sagas and schedules them",
                period: config.instance_watcher.period_secs,
                task_impl: Box::new(updater),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_instance_updater,
            });
        }

        // Background task: schedule restart sagas for failed instances that can
        // be automatically restarted.
        {
            let reincarnator =
                instance_reincarnation::InstanceReincarnation::new(
                    datastore.clone(),
                    sagas.clone(),
                    config.instance_reincarnation.disable,
                );
            driver.register(TaskDefinition {
                name: "instance_reincarnation",
                description: "schedules start sagas for failed instances that \
                    can be automatically restarted",
                period: config.instance_reincarnation.period_secs,
                task_impl: Box::new(reincarnator),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_instance_reincarnation,
            });
        }

        // Background task: service firewall rule propagation
        driver.register(TaskDefinition {
            name: "service_firewall_rule_propagation",
            description:
                "propagates VPC firewall rules for Omicron services with \
                 external network connectivity",
            period: config.service_firewall_propagation.period_secs,
            task_impl: Box::new(
                service_firewall_rules::ServiceRulePropagator::new(
                    datastore.clone(),
                ),
            ),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_service_firewall_propagation,
        });

        // Background task: OPTE port route propagation
        {
            let watcher = vpc_routes::VpcRouteManager::new(datastore.clone());
            driver.register(TaskDefinition {
                name: "vpc_route_manager",
                description: "propagates updated VPC routes to all OPTE ports",
                period: config.switch_port_settings_manager.period_secs,
                task_impl: Box::new(watcher),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_vpc_route_manager,
            })
        };

        // Background task: abandoned VMM reaping
        driver.register(TaskDefinition {
            name: "abandoned_vmm_reaper",
            description:
                "deletes sled reservations for VMMs that have been abandoned \
                 by their instances",
            period: config.abandoned_vmm_reaper.period_secs,
            task_impl: Box::new(abandoned_vmm_reaper::AbandonedVmmReaper::new(
                datastore.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_abandoned_vmm_reaper,
        });

        // Background task: saga recovery
        {
            let task_impl = Box::new(saga_recovery::SagaRecovery::new(
                datastore.clone(),
                nexus_db_model::SecId::from(args.nexus_id),
                args.saga_recovery,
            ));

            driver.register(TaskDefinition {
                name: "saga_recovery",
                description: "recovers sagas assigned to this Nexus",
                period: config.saga_recovery.period_secs,
                task_impl,
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_saga_recovery,
            });
        }

        driver.register(TaskDefinition {
            name: "lookup_region_port",
            description: "fill in missing ports for region records",
            period: config.lookup_region_port.period_secs,
            task_impl: Box::new(lookup_region_port::LookupRegionPort::new(
                datastore.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_lookup_region_port,
        });

        driver.register(TaskDefinition {
            name: "region_snapshot_replacement_start",
            description:
                "detect if region snapshots need replacement and begin the \
                process",
            period: config.region_snapshot_replacement_start.period_secs,
            task_impl: Box::new(RegionSnapshotReplacementDetector::new(
                datastore.clone(),
                sagas.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_region_snapshot_replacement_start,
        });

        driver.register(TaskDefinition {
            name: "region_snapshot_replacement_garbage_collection",
            description:
                "clean up all region snapshot replacement step volumes",
            period: config
                .region_snapshot_replacement_garbage_collection
                .period_secs,
            task_impl: Box::new(RegionSnapshotReplacementGarbageCollect::new(
                datastore.clone(),
                sagas.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_region_snapshot_replacement_garbage_collection,
        });

        driver.register(TaskDefinition {
            name: "region_snapshot_replacement_step",
            description:
                "detect what volumes were affected by a region snapshot \
                replacement, and run the step saga for them",
            period: config.region_snapshot_replacement_step.period_secs,
            task_impl: Box::new(RegionSnapshotReplacementFindAffected::new(
                datastore.clone(),
                sagas.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_region_snapshot_replacement_step,
        });

        driver.register(TaskDefinition {
            name: "region_snapshot_replacement_finish",
            description:
                "complete a region snapshot replacement if all the steps are \
                done",
            period: config.region_snapshot_replacement_finish.period_secs,
            task_impl: Box::new(RegionSnapshotReplacementFinishDetector::new(
                datastore.clone(),
                sagas,
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_region_snapshot_replacement_finish,
        });

        driver.register(TaskDefinition {
            name: "tuf_artifact_replication",
            description: "replicate update repo artifacts across sleds",
            period: config.tuf_artifact_replication.period_secs,
            task_impl: Box::new(
                tuf_artifact_replication::ArtifactReplication::new(
                    datastore.clone(),
                    args.tuf_artifact_replication_rx,
                    config.tuf_artifact_replication.min_sled_replication,
                ),
            ),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_tuf_artifact_replication,
        });

        driver.register(TaskDefinition {
            name: "read_only_region_replacement_start",
            description:
                "detect if read-only regions need replacement and begin the \
                process",
            period: config.read_only_region_replacement_start.period_secs,
            task_impl: Box::new(ReadOnlyRegionReplacementDetector::new(
                datastore.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_read_only_region_replacement_start,
        });

        driver.register(TaskDefinition {
            name: "alert_dispatcher",
            description: "dispatches queued alerts to receivers",
            period: config.alert_dispatcher.period_secs,
            task_impl: Box::new(AlertDispatcher::new(
                datastore.clone(),
                task_webhook_deliverator.clone(),
            )),
            opctx: opctx.child(BTreeMap::new()),
            watchers: vec![],
            activator: task_alert_dispatcher,
        });

        driver.register({
            let nexus_config::WebhookDeliveratorConfig {
                lease_timeout_secs,
                period_secs,
                first_retry_backoff_secs,
                second_retry_backoff_secs,
            } = config.webhook_deliverator;
            let cfg = webhook_deliverator::DeliveryConfig {
                lease_timeout: chrono::TimeDelta::seconds(
                    lease_timeout_secs.try_into().expect(
                        "invalid webhook_deliverator.lease_timeout_secs",
                    ),
                ),
                first_retry_backoff: chrono::TimeDelta::seconds(
                    first_retry_backoff_secs.try_into().expect(
                        "invalid webhook_deliverator.first_retry_backoff_secs",
                    ),
                ),
                second_retry_backoff: chrono::TimeDelta::seconds(
                    second_retry_backoff_secs.try_into().expect(
                        "invalid webhook_deliverator.first_retry_backoff_secs",
                    ),
                ),
            };
            TaskDefinition {
                name: "webhook_deliverator",
                description: "sends webhook delivery requests",
                period: period_secs,
                task_impl: Box::new(
                    webhook_deliverator::WebhookDeliverator::new(
                        datastore,
                        cfg,
                        nexus_id,
                        args.webhook_delivery_client,
                    ),
                ),
                opctx: opctx.child(BTreeMap::new()),
                watchers: vec![],
                activator: task_webhook_deliverator,
            }
        });

        driver
    }
}

pub struct BackgroundTasksData {
    /// root `OpContext` used for background tasks
    pub opctx: OpContext,
    /// handle to `DataStore`, provided directly to many background tasks
    pub datastore: Arc<DataStore>,
    /// background task configuration
    pub config: BackgroundTaskConfig,
    /// rack identifier
    pub rack_id: Uuid,
    /// nexus identifier
    pub nexus_id: OmicronZoneUuid,
    /// internal DNS DNS resolver, used when tasks need to contact other
    /// internal services
    pub resolver: internal_dns_resolver::Resolver,
    /// handle to saga subsystem for starting sagas
    pub saga_starter: Arc<dyn StartSaga>,
    /// Oximeter producer registry (for metrics)
    pub producer_registry: ProducerRegistry,
    /// Helpers for saga recovery
    pub saga_recovery: saga_recovery::SagaRecoveryHelpers<Arc<Nexus>>,
    /// Channel for TUF repository artifacts to be replicated out to sleds
    pub tuf_artifact_replication_rx: mpsc::Receiver<ArtifactsWithPlan>,
    /// `reqwest::Client` for webhook delivery requests.
    ///
    /// This is shared with the external API as it's also used when sending
    /// webhook liveness probe requests from the API.
    pub webhook_delivery_client: reqwest::Client,
    /// Channel for configuring pending MGS updates
    pub mgs_updates_tx: watch::Sender<PendingMgsUpdates>,
}

/// Starts the three DNS-propagation-related background tasks for either
/// internal or external DNS (depending on the arguments)
#[allow(clippy::too_many_arguments)]
fn init_dns(
    driver: &mut Driver,
    opctx: &OpContext,
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
    resolver: internal_dns_resolver::Resolver,
    config: &DnsTasksConfig,
    task_config: &Activator,
    task_servers: &Activator,
    task_propagation: &Activator,
) {
    let dns_group_name = dns_group.to_string();
    let metadata = BTreeMap::from([("dns_group".to_string(), dns_group_name)]);

    // Background task: DNS config watcher
    let dns_config =
        dns_config::DnsConfigWatcher::new(Arc::clone(&datastore), dns_group);
    let dns_config_watcher = dns_config.watcher();
    let task_name_config = format!("dns_config_{}", dns_group);
    driver.register(TaskDefinition {
        name: task_name_config.clone(),
        description: format!(
            "watches {} DNS data stored in CockroachDB",
            dns_group
        ),
        period: config.period_secs_config,
        task_impl: Box::new(dns_config),
        opctx: opctx.child(metadata.clone()),
        watchers: vec![],
        activator: task_config,
    });

    // Background task: DNS server list watcher
    let dns_servers = dns_servers::DnsServersWatcher::new(dns_group, resolver);
    let dns_servers_watcher = dns_servers.watcher();
    let task_name_servers = format!("dns_servers_{}", dns_group);
    driver.register(TaskDefinition {
        name: task_name_servers.clone(),
        description: format!(
            "watches list of {} DNS servers stored in internal DNS",
            dns_group,
        ),
        period: config.period_secs_servers,
        task_impl: Box::new(dns_servers),
        opctx: opctx.child(metadata.clone()),
        watchers: vec![],
        activator: task_servers,
    });

    // Background task: DNS propagation
    let dns_propagate = dns_propagation::DnsPropagator::new(
        dns_config_watcher.clone(),
        dns_servers_watcher.clone(),
        config.max_concurrent_server_updates,
    );
    driver.register(TaskDefinition {
        name: format!("dns_propagation_{}", dns_group),
        description: format!(
            "propagates latest {} DNS configuration (from {:?} background \
             task) to the latest list of DNS servers (from {:?} background \
             task)",
            dns_group, task_name_config, task_name_servers,
        ),
        period: config.period_secs_propagation,
        task_impl: Box::new(dns_propagate),
        opctx: opctx.child(metadata),
        watchers: vec![
            Box::new(dns_config_watcher),
            Box::new(dns_servers_watcher),
        ],
        activator: task_propagation,
    });
}

#[cfg(test)]
pub mod test {
    use crate::app::saga::SagaCompletionFuture;
    use crate::app::saga::StartSaga;
    use dropshot::HandlerTaskMode;
    use futures::FutureExt;
    use internal_dns_types::names::ServiceName;
    use nexus_db_model::DnsGroup;
    use nexus_db_queries::context::OpContext;
    use nexus_db_queries::db::DataStore;
    use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::internal_api::params as nexus_params;
    use nexus_types::internal_api::params::DnsRecord;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::poll;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tempfile::TempDir;
    use uuid::Uuid;

    /// Used by various tests of tasks that kick off sagas
    pub(crate) struct NoopStartSaga {
        count: AtomicU64,
    }

    impl NoopStartSaga {
        pub(crate) fn new() -> Self {
            Self { count: AtomicU64::new(0) }
        }

        pub(crate) fn count_reset(&self) -> u64 {
            self.count.swap(0, Ordering::SeqCst)
        }
    }

    impl StartSaga for NoopStartSaga {
        fn saga_start(
            &self,
            _: steno::SagaDag,
        ) -> futures::prelude::future::BoxFuture<'_, Result<steno::SagaId, Error>>
        {
            let _ = self.count.fetch_add(1, Ordering::SeqCst);
            async {
                // We've not actually started a real saga, so just make
                // something up.
                Ok(steno::SagaId(Uuid::new_v4()))
            }
            .boxed()
        }

        fn saga_run(
            &self,
            _: steno::SagaDag,
        ) -> futures::prelude::future::BoxFuture<
            '_,
            Result<(steno::SagaId, SagaCompletionFuture), Error>,
        > {
            let _ = self.count.fetch_add(1, Ordering::SeqCst);
            async {
                let id = steno::SagaId(Uuid::new_v4());
                // No-op sagas complete immediately.
                let completed = async { Ok(()) }.boxed();
                Ok((id, completed))
            }
            .boxed()
        }
    }

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
        assert_eq!(config.generation, Generation::from_u32(1));

        let internal_dns_srv_name = ServiceName::InternalDns.dns_name();

        let initial_srv_record = {
            let zone =
                config.zones.get(0).expect("DNS config must have a zone");
            let Some(record) = zone.names.get(&internal_dns_srv_name) else {
                panic!("zone must have a record for {internal_dns_srv_name}")
            };
            match record.get(0) {
                Some(DnsRecord::Srv(srv)) => srv,
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
                default_request_body_max_bytes: 8 * 1024,
                default_handler_task_mode: HandlerTaskMode::Detached,
                log_headers: vec![],
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
            Generation::from_u32(2),
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
            Generation::from_u32(2),
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
            Generation::from_u32(3),
        )
        .await;

        wait_propagate_dns(
            &cptestctx.logctx.log,
            "new",
            new_dns_dropshot_server.local_addr(),
            Generation::from_u32(3),
        )
        .await;
    }

    /// Verify that DNS gets propagated to the specified server
    async fn wait_propagate_dns(
        log: &slog::Logger,
        label: &str,
        addr: SocketAddr,
        generation: Generation,
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
