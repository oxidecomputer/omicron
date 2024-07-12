// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus, the service that operates much of the control plane in an Oxide fleet

use self::external_endpoints::NexusCertResolver;
use self::saga::SagaExecutor;
use crate::app::background::BackgroundTasksData;
use crate::app::oximeter::LazyTimeseriesClient;
use crate::populate::populate_start;
use crate::populate::PopulateArgs;
use crate::populate::PopulateStatus;
use crate::DropshotServer;
use ::oximeter::types::ProducerRegistry;
use anyhow::anyhow;
use internal_dns::ServiceName;
use nexus_config::NexusConfig;
use nexus_config::RegionAllocationStrategy;
use nexus_config::Tunables;
use nexus_config::UpdatesConfig;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGD_PORT;
use omicron_common::address::MGS_PORT;
use omicron_common::api::external::Error;
use omicron_common::api::internal::shared::SwitchLocation;
use oximeter_producer::Server as ProducerServer;
use slog::Logger;
use std::collections::HashMap;
use std::net::SocketAddrV6;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use std::sync::OnceLock;
use uuid::Uuid;

// The implementation of Nexus is large, and split into a number of submodules
// by resource.
mod address_lot;
mod allow_list;
pub(crate) mod background;
mod bfd;
mod bgp;
mod certificate;
mod crucible;
mod deployment;
mod device_auth;
mod disk;
mod external_dns;
pub(crate) mod external_endpoints;
mod external_ip;
mod iam;
mod image;
mod instance;
mod instance_network;
mod ip_pool;
mod metrics;
mod network_interface;
pub(crate) mod oximeter;
mod probe;
mod project;
mod quota;
mod rack;
pub(crate) mod saga;
mod session;
mod silo;
mod sled;
mod sled_instance;
mod snapshot;
mod ssh_key;
mod switch;
mod switch_interface;
mod switch_port;
pub mod test_interfaces;
mod update;
mod utilization;
mod volume;
mod vpc;
mod vpc_router;
mod vpc_subnet;

// Sagas are not part of the "Nexus" implementation, but they are
// application logic.
pub(crate) mod sagas;

// TODO: When referring to API types, we should try to include
// the prefix unless it is unambiguous.

pub(crate) use nexus_db_queries::db::queries::disk::MAX_DISKS_PER_INSTANCE;

use nexus_db_model::AllSchemaVersions;
pub(crate) use nexus_db_model::MAX_NICS_PER_INSTANCE;
use tokio::sync::mpsc;

// XXX: Might want to recast as max *floating* IPs, we have at most one
//      ephemeral (so bounded in saga by design).
//      The value here is arbitrary, but we need *a* limit for the instance
//      create saga to have a bounded DAG. We might want to only enforce
//      this during instance create (rather than live attach) in future.
pub(crate) const MAX_EXTERNAL_IPS_PER_INSTANCE: usize =
    nexus_db_queries::db::queries::external_ip::MAX_EXTERNAL_IPS_PER_INSTANCE
        as usize;
pub(crate) const MAX_EPHEMERAL_IPS_PER_INSTANCE: usize = 1;

pub const MAX_VCPU_PER_INSTANCE: u16 = 64;

pub const MIN_MEMORY_BYTES_PER_INSTANCE: u32 = 1 << 30; // 1 GiB
pub const MAX_MEMORY_BYTES_PER_INSTANCE: u64 = 256 * (1 << 30); // 256 GiB

pub const MIN_DISK_SIZE_BYTES: u32 = 1 << 30; // 1 GiB
pub const MAX_DISK_SIZE_BYTES: u64 = 1023 * (1 << 30); // 1023 GiB

/// This value is aribtrary
pub const MAX_SSH_KEYS_PER_INSTANCE: u32 = 100;

/// Manages an Oxide fleet -- the heart of the control plane
pub struct Nexus {
    /// uuid for this nexus instance.
    id: Uuid,

    /// uuid for this rack
    rack_id: Uuid,

    /// general server log
    log: Logger,

    /// persistent storage for resources in the control plane
    db_datastore: Arc<db::DataStore>,

    /// handle to global authz information
    authz: Arc<authz::Authz>,

    /// saga execution coordinator (SEC)
    sagas: Arc<SagaExecutor>,

    /// External dropshot servers
    external_server: std::sync::Mutex<Option<DropshotServer>>,

    /// External dropshot server that listens on the internal network to allow
    /// connections from the tech port; see RFD 431.
    techport_external_server: std::sync::Mutex<Option<DropshotServer>>,

    /// Internal dropshot server
    internal_server: std::sync::Mutex<Option<DropshotServer>>,

    /// Status of background task to populate database
    populate_status: tokio::sync::watch::Receiver<PopulateStatus>,

    /// The metric producer server from which oximeter collects metric data.
    producer_server: std::sync::Mutex<Option<ProducerServer>>,

    /// Reusable `reqwest::Client`, to be cloned and used with the Progenitor-
    /// generated `Client::new_with_client`.
    ///
    /// (This does not need to be in an `Arc` because `reqwest::Client` uses
    /// `Arc` internally.)
    reqwest_client: reqwest::Client,

    /// Client to the timeseries database.
    timeseries_client: LazyTimeseriesClient,

    /// Contents of the trusted root role for the TUF repository.
    #[allow(dead_code)]
    updates_config: Option<UpdatesConfig>,

    /// The tunable parameters from a configuration file
    tunables: Tunables,

    /// Operational context used for Instance allocation
    opctx_alloc: OpContext,

    /// Operational context used for external request authentication
    opctx_external_authn: OpContext,

    /// Max issue delay for samael crate - used only for testing
    // the samael crate has an extra check (beyond the check against the SAML
    // response NotOnOrAfter) that fails if the issue instant was too long ago.
    // this amount of time is called "max issue delay" and we have to set that
    // in order for our integration tests that POST static SAML responses to
    // Nexus to not all fail.
    samael_max_issue_delay: std::sync::Mutex<Option<chrono::Duration>>,

    /// DNS resolver for internal services
    internal_resolver: internal_dns::resolver::Resolver,

    /// DNS resolver Nexus uses to resolve an external host
    external_resolver: Arc<external_dns::Resolver>,

    /// DNS servers used in `external_resolver`, used to provide DNS servers to
    /// instances via DHCP
    // TODO: This needs to be moved to the database.
    // https://github.com/oxidecomputer/omicron/issues/3732
    external_dns_servers: Vec<IpAddr>,

    /// Background task driver
    background_tasks_driver: OnceLock<background::Driver>,

    /// Handles to various specific tasks
    background_tasks: background::BackgroundTasks,

    /// Default Crucible region allocation strategy
    default_region_allocation_strategy: RegionAllocationStrategy,
}

impl Nexus {
    /// Create a new Nexus instance for the given rack id `rack_id`
    // TODO-polish revisit rack metadata
    pub(crate) async fn new_with_id(
        rack_id: Uuid,
        log: Logger,
        resolver: internal_dns::resolver::Resolver,
        pool: db::Pool,
        producer_registry: &ProducerRegistry,
        config: &NexusConfig,
        authz: Arc<authz::Authz>,
    ) -> Result<Arc<Nexus>, String> {
        let pool = Arc::new(pool);
        let all_versions = config
            .pkg
            .schema
            .as_ref()
            .map(|s| AllSchemaVersions::load(&s.schema_dir))
            .transpose()
            .map_err(|error| format!("{error:#}"))?;
        let db_datastore = Arc::new(
            db::DataStore::new(&log, Arc::clone(&pool), all_versions.as_ref())
                .await?,
        );
        db_datastore.register_producers(producer_registry);

        let my_sec_id = db::SecId::from(config.deployment.id);
        let sec_store = Arc::new(db::CockroachDbSecStore::new(
            my_sec_id,
            Arc::clone(&db_datastore),
            log.new(o!("component" => "SecStore")),
        )) as Arc<dyn steno::SecStore>;

        let sec_client = Arc::new(steno::sec(
            log.new(o!(
                "component" => "SEC",
                "sec_id" => my_sec_id.to_string()
            )),
            sec_store,
        ));

        // It's a bit of a red flag to use an unbounded channel.
        //
        // This particular channel is used to send a Uuid from the saga executor
        // to the saga recovery background task each time a saga is started.
        //
        // The usual argument for keeping a channel bounded is to ensure
        // backpressure.  But we don't really want that here.  These items don't
        // represent meaningful work for the saga recovery task, such that if it
        // were somehow processing these slowly, we'd want to slow down the saga
        // dispatch process.  Under normal conditions, we'd expect this queue to
        // grow as we dispatch new sagas until the saga recovery task runs, at
        // which point the queue will quickly be drained.  The only way this
        // could really grow without bound is if the saga recovery task gets
        // completely wedged and stops receiving these messages altogether.  In
        // this case, the maximum size this queue could grow over time is the
        // number of sagas we can launch in that time.  That's not ever likely
        // to be a significant amount of memory.
        //
        // We could put our money where our mouth is: pick a sufficiently large
        // bound and panic if we reach it.  But "sufficiently large" depends on
        // the saga creation rate and the period of the saga recovery background
        // task.  If someone changed the config, they'd have to remember to
        // update this here.  This doesn't seem worth it.
        let (saga_create_tx, saga_recovery_rx) = mpsc::unbounded_channel();
        let sagas = Arc::new(SagaExecutor::new(
            Arc::clone(&sec_client),
            log.new(o!("component" => "SagaExecutor")),
            saga_create_tx,
        ));

        let client_state = dpd_client::ClientState {
            tag: String::from("nexus"),
            log: log.new(o!(
                "component" => "DpdClient"
            )),
        };

        let mut dpd_clients: HashMap<SwitchLocation, Arc<dpd_client::Client>> =
            HashMap::new();

        let mut mg_clients: HashMap<
            SwitchLocation,
            Arc<mg_admin_client::Client>,
        > = HashMap::new();

        // Currently static dpd configuration mappings are still required for
        // testing
        for (location, config) in &config.pkg.dendrite {
            let address = config.address.ip().to_string();
            let port = config.address.port();
            let dpd_client = dpd_client::Client::new(
                &format!("http://[{address}]:{port}"),
                client_state.clone(),
            );
            dpd_clients.insert(*location, Arc::new(dpd_client));
        }
        for (location, config) in &config.pkg.mgd {
            let mg_client = mg_admin_client::Client::new(
                &format!("http://{}", config.address),
                log.clone(),
            );
            mg_clients.insert(*location, Arc::new(mg_client));
        }
        if config.pkg.dendrite.is_empty() {
            loop {
                let result = resolver
                    .lookup_all_ipv6(ServiceName::Dendrite)
                    .await
                    .map_err(|e| {
                        format!("Cannot lookup Dendrite addresses: {e}")
                    });
                match result {
                    Ok(addrs) => {
                        let mappings = map_switch_zone_addrs(
                            &log.new(o!("component" => "Nexus")),
                            addrs,
                        )
                        .await;
                        for (location, addr) in &mappings {
                            let port = DENDRITE_PORT;
                            let dpd_client = dpd_client::Client::new(
                                &format!("http://[{addr}]:{port}"),
                                client_state.clone(),
                            );
                            dpd_clients.insert(*location, Arc::new(dpd_client));
                        }
                        break;
                    }
                    Err(e) => {
                        warn!(log, "Failed to lookup Dendrite address: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(1))
                            .await;
                    }
                }
            }
        }
        if config.pkg.mgd.is_empty() {
            loop {
                let result = resolver
                    // TODO this should be ServiceName::Mgd, but in the upgrade
                    //      path, that does not exist because RSS has not
                    //      created it. So we just piggyback on Dendrite's SRV
                    //      record.
                    .lookup_all_ipv6(ServiceName::Dendrite)
                    .await
                    .map_err(|e| format!("Cannot lookup mgd addresses: {e}"));
                match result {
                    Ok(addrs) => {
                        let mappings = map_switch_zone_addrs(
                            &log.new(o!("component" => "Nexus")),
                            addrs,
                        )
                        .await;
                        for (location, addr) in &mappings {
                            let port = MGD_PORT;
                            let mgd_client = mg_admin_client::Client::new(
                                &format!(
                                    "http://{}",
                                    &std::net::SocketAddr::new(
                                        (*addr).into(),
                                        port,
                                    )
                                ),
                                log.clone(),
                            );
                            mg_clients.insert(*location, Arc::new(mgd_client));
                        }
                        break;
                    }
                    Err(e) => {
                        warn!(log, "Failed to lookup mgd address: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(1))
                            .await;
                    }
                }
            }
        }

        let reqwest_client = reqwest::ClientBuilder::new()
            .connect_timeout(std::time::Duration::from_secs(15))
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|e| e.to_string())?;

        // Connect to clickhouse - but do so lazily.
        // Clickhouse may not be executing when Nexus starts.
        let timeseries_client = if let Some(address) =
            &config.pkg.timeseries_db.address
        {
            // If an address was provided, use it instead of DNS.
            LazyTimeseriesClient::new_from_address(log.clone(), *address)
        } else {
            LazyTimeseriesClient::new_from_dns(log.clone(), resolver.clone())
        };

        // TODO-cleanup We may want to make the populator a first-class
        // background task.
        let populate_ctx = OpContext::for_background(
            log.new(o!("component" => "DataLoader")),
            Arc::clone(&authz),
            authn::Context::internal_db_init(),
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        let populate_args = PopulateArgs::new(rack_id);
        let populate_status = populate_start(
            populate_ctx,
            Arc::clone(&db_datastore),
            populate_args,
        );

        let background_ctx = OpContext::for_background(
            log.new(o!("component" => "BackgroundTasks")),
            Arc::clone(&authz),
            authn::Context::internal_api(),
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        let (background_tasks_initializer, background_tasks) =
            background::BackgroundTasksInitializer::new();

        let external_resolver = {
            if config.deployment.external_dns_servers.is_empty() {
                return Err("expected at least 1 external DNS server".into());
            }
            Arc::new(external_dns::Resolver::new(
                &config.deployment.external_dns_servers,
            ))
        };

        let nexus = Nexus {
            id: config.deployment.id,
            rack_id,
            log: log.new(o!()),
            db_datastore: Arc::clone(&db_datastore),
            authz: Arc::clone(&authz),
            sagas,
            external_server: std::sync::Mutex::new(None),
            techport_external_server: std::sync::Mutex::new(None),
            internal_server: std::sync::Mutex::new(None),
            producer_server: std::sync::Mutex::new(None),
            populate_status,
            reqwest_client,
            timeseries_client,
            updates_config: config.pkg.updates.clone(),
            tunables: config.pkg.tunables.clone(),
            opctx_alloc: OpContext::for_background(
                log.new(o!("component" => "InstanceAllocator")),
                Arc::clone(&authz),
                authn::Context::internal_read(),
                Arc::clone(&db_datastore)
                    as Arc<dyn nexus_auth::storage::Storage>,
            ),
            opctx_external_authn: OpContext::for_background(
                log.new(o!("component" => "ExternalAuthn")),
                Arc::clone(&authz),
                authn::Context::external_authn(),
                Arc::clone(&db_datastore)
                    as Arc<dyn nexus_auth::storage::Storage>,
            ),
            samael_max_issue_delay: std::sync::Mutex::new(None),
            internal_resolver: resolver.clone(),
            external_resolver,
            external_dns_servers: config
                .deployment
                .external_dns_servers
                .clone(),
            background_tasks_driver: OnceLock::new(),
            background_tasks,
            default_region_allocation_strategy: config
                .pkg
                .default_region_allocation_strategy
                .clone(),
        };

        // TODO-cleanup all the extra Arcs here seems wrong
        let nexus = Arc::new(nexus);
        nexus.sagas.set_nexus(nexus.clone());
        let saga_recovery_opctx = OpContext::for_background(
            log.new(o!("component" => "SagaRecoverer")),
            Arc::clone(&authz),
            authn::Context::internal_saga_recovery(),
            Arc::clone(&db_datastore) as Arc<dyn nexus_auth::storage::Storage>,
        );

        // Wait to start background tasks until after the populate step
        // finishes.  Among other things, the populate step installs role
        // assignments for internal identities that are used by the background
        // tasks.  If we don't do this here, those tasks would fail spuriously
        // on startup and not be retried for a while.
        let task_nexus = nexus.clone();
        let task_log = nexus.log.clone();
        let task_registry = producer_registry.clone();
        let task_config = config.clone();
        tokio::spawn(async move {
            match task_nexus.wait_for_populate().await {
                Ok(_) => {
                    info!(task_log, "populate complete");
                }
                Err(_) => {
                    error!(task_log, "populate failed");
                }
            };

            // That said, even if the populate step fails, we may as well try to
            // start the background tasks so that whatever can work will work.
            info!(task_log, "activating background tasks");

            let driver = background_tasks_initializer.start(
                &task_nexus.background_tasks,
                BackgroundTasksData {
                    opctx: background_ctx,
                    datastore: db_datastore,
                    config: task_config.pkg.background_tasks,
                    rack_id,
                    nexus_id: task_config.deployment.id,
                    resolver,
                    saga_starter: task_nexus.sagas.clone(),
                    producer_registry: task_registry,

                    saga_recovery_opctx,
                    saga_recovery_nexus: task_nexus.clone(),
                    saga_recovery_sec: sec_client.clone(),
                    saga_recovery_registry: sagas::ACTION_REGISTRY.clone(),
                    saga_recovery_rx,
                },
            );

            if let Err(_) = task_nexus.background_tasks_driver.set(driver) {
                panic!("multiple initialization of background_tasks_driver");
            }
        });

        Ok(nexus)
    }

    /// Return the ID for this Nexus instance.
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Return the rack ID for this Nexus instance.
    pub fn rack_id(&self) -> Uuid {
        self.rack_id
    }

    /// Return the tunable configuration parameters, e.g. for use in tests.
    pub fn tunables(&self) -> &Tunables {
        &self.tunables
    }

    pub fn authz(&self) -> &Arc<authz::Authz> {
        &self.authz
    }

    pub(crate) async fn wait_for_populate(&self) -> Result<(), anyhow::Error> {
        let mut my_rx = self.populate_status.clone();
        loop {
            my_rx
                .changed()
                .await
                .map_err(|error| anyhow!(error.to_string()))?;
            match &*my_rx.borrow() {
                PopulateStatus::NotDone => (),
                PopulateStatus::Done => return Ok(()),
                PopulateStatus::Failed(error) => {
                    return Err(anyhow!(error.clone()))
                }
            };
        }
    }

    pub(crate) async fn external_tls_config(
        &self,
        tls_enabled: bool,
    ) -> Option<rustls::ServerConfig> {
        // Wait for the background task to complete at least once.  We don't
        // care about its value.  To do this, we need our own copy of the
        // channel.
        let mut rx = self.background_tasks.external_endpoints.clone();
        let _ = rx.wait_for(|s| s.is_some()).await;
        if !tls_enabled {
            return None;
        }

        let mut rustls_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(NexusCertResolver::new(
                self.log.new(o!("component" => "NexusCertResolver")),
                self.background_tasks.external_endpoints.clone(),
            )));
        rustls_cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Some(rustls_cfg)
    }

    // Called to trigger inventory collection.
    pub(crate) fn activate_inventory_collection(&self) {
        self.background_tasks
            .activate(&self.background_tasks.task_inventory_collection);
    }

    // Called to hand off management of external servers to Nexus.
    pub(crate) async fn set_servers(
        &self,
        external_server: DropshotServer,
        techport_external_server: DropshotServer,
        internal_server: DropshotServer,
        producer_server: ProducerServer,
    ) {
        // If any servers already exist, close them.
        let _ = self.close_servers().await;

        // Insert the new servers.
        self.external_server.lock().unwrap().replace(external_server);
        self.techport_external_server
            .lock()
            .unwrap()
            .replace(techport_external_server);
        self.internal_server.lock().unwrap().replace(internal_server);
        self.producer_server.lock().unwrap().replace(producer_server);
    }

    pub(crate) async fn close_servers(&self) -> Result<(), String> {
        // NOTE: All these take the lock and swap out of the option immediately,
        // because they are synchronous mutexes, which cannot be held across the
        // await point these `close()` methods expose.
        let external_server = self.external_server.lock().unwrap().take();
        if let Some(server) = external_server {
            server.close().await?;
        }
        let techport_external_server =
            self.techport_external_server.lock().unwrap().take();
        if let Some(server) = techport_external_server {
            server.close().await?;
        }
        let internal_server = self.internal_server.lock().unwrap().take();
        if let Some(server) = internal_server {
            server.close().await?;
        }
        let producer_server = self.producer_server.lock().unwrap().take();
        if let Some(server) = producer_server {
            server.close().await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub(crate) async fn wait_for_shutdown(&self) -> Result<(), String> {
        // The internal server is the last server to be closed.
        //
        // We don't wait for the external servers to be closed; we just expect
        // that they'll be closed before the internal server.
        let server_fut = self
            .internal_server
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.wait_for_shutdown());
        if let Some(server_fut) = server_fut {
            server_fut.await?;
        }
        Ok(())
    }

    pub(crate) async fn get_external_server_address(
        &self,
    ) -> Option<std::net::SocketAddr> {
        self.external_server
            .lock()
            .unwrap()
            .as_ref()
            .map(|server| server.local_addr())
    }

    pub(crate) async fn get_techport_server_address(
        &self,
    ) -> Option<std::net::SocketAddr> {
        self.techport_external_server
            .lock()
            .unwrap()
            .as_ref()
            .map(|server| server.local_addr())
    }

    pub(crate) async fn get_internal_server_address(
        &self,
    ) -> Option<std::net::SocketAddr> {
        self.internal_server
            .lock()
            .unwrap()
            .as_ref()
            .map(|server| server.local_addr())
    }

    /// Returns an [`OpContext`] used for authenticating external requests
    pub fn opctx_external_authn(&self) -> &OpContext {
        &self.opctx_external_authn
    }

    /// Returns an [`OpContext`] used for balancing services.
    pub(crate) fn opctx_for_service_balancer(&self) -> OpContext {
        OpContext::for_background(
            self.log.new(o!("component" => "ServiceBalancer")),
            Arc::clone(&self.authz),
            authn::Context::internal_service_balancer(),
            Arc::clone(&self.db_datastore)
                as Arc<dyn nexus_auth::storage::Storage>,
        )
    }

    /// Returns an [`OpContext`] used for internal API calls.
    pub(crate) fn opctx_for_internal_api(&self) -> OpContext {
        OpContext::for_background(
            self.log.new(o!("component" => "InternalApi")),
            Arc::clone(&self.authz),
            authn::Context::internal_api(),
            Arc::clone(&self.db_datastore)
                as Arc<dyn nexus_auth::storage::Storage>,
        )
    }

    /// Used as the body of a "stub" endpoint -- one that's currently
    /// unimplemented but that we eventually intend to implement
    ///
    /// Even though an endpoint is unimplemented, it's useful if it implements
    /// the correct authn/authz behaviors behaviors for unauthenticated and
    /// authenticated, unauthorized requests.  This allows us to maintain basic
    /// authn/authz test coverage for stub endpoints, which in turn helps us
    /// ensure that all endpoints are covered.
    ///
    /// In order to implement the correct authn/authz behavior, we need to know
    /// a little about the endpoint.  This is given by the `visibility`
    /// argument.  See the examples below.
    ///
    /// # Examples
    ///
    /// ## A top-level API endpoint (always visible)
    ///
    /// For example, "/my-new-kind-of-resource".  The assumption is that the
    /// _existence_ of this endpoint is not a secret.  Use:
    ///
    /// ```
    /// use nexus_db_queries::context::OpContext;
    /// use nexus_db_queries::db::DataStore;
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_common::api::external::Error;
    ///
    /// async fn my_things_list(
    ///     nexus: &Nexus,
    ///     datastore: &DataStore,
    ///     opctx: &OpContext,
    /// ) -> Result<(), Error>
    /// {
    ///     Err(nexus.unimplemented_todo(opctx, Unimpl::Public).await)
    /// }
    /// ```
    ///
    /// ## An authz-protected resource under the top level
    ///
    /// For example, "/my-new-kind-of-resource/demo" (where "demo" is the name
    /// of a specific resource of type "my-new-kind-of-resource").  Use:
    ///
    /// ```
    /// use nexus_db_queries::context::OpContext;
    /// use nexus_db_queries::db::model::Name;
    /// use nexus_db_queries::db::DataStore;
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_common::api::external::Error;
    /// use omicron_common::api::external::LookupType;
    /// use omicron_common::api::external::ResourceType;
    ///
    /// async fn my_thing_fetch(
    ///     nexus: &Nexus,
    ///     datastore: &DataStore,
    ///     opctx: &OpContext,
    ///     the_name: &Name,
    /// ) -> Result<(), Error>
    /// {
    ///     // You will want to have defined your own ResourceType variant for
    ///     // this resource, even though it's still a stub.
    ///     let resource_type: ResourceType = todo!();
    ///     let lookup_type = LookupType::ByName(the_name.to_string());
    ///     let not_found_error = lookup_type.into_not_found(resource_type);
    ///     let unimp = Unimpl::ProtectedLookup(not_found_error);
    ///     Err(nexus.unimplemented_todo(opctx, unimp).await)
    /// }
    /// ```
    ///
    /// This does the bare minimum to produce an appropriate 404 "Not Found"
    /// error for authenticated, unauthorized users.
    ///
    /// ## An authz-protected API endpoint under some other (non-stub) resource
    ///
    /// ### ... when the endpoint never returns 404 (e.g., "list", "create")
    ///
    /// For example, "/organizations/my-org/my-new-kind-of-resource".  In this
    /// case, your function should do whatever lookup of the non-stub resource
    /// that the function will eventually do, and then treat it like the first
    /// example.
    ///
    /// Here's an example stub for the "list" endpoint for a new resource
    /// underneath Organizations:
    ///
    /// ```
    /// use nexus_db_queries::authz;
    /// use nexus_db_queries::context::OpContext;
    /// use nexus_db_queries::db::lookup::LookupPath;
    /// use nexus_db_queries::db::model::Name;
    /// use nexus_db_queries::db::DataStore;
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_common::api::external::Error;
    ///
    /// async fn project_list_my_thing(
    ///     nexus: &Nexus,
    ///     datastore: &DataStore,
    ///     opctx: &OpContext,
    ///     project_name: &Name,
    /// ) -> Result<(), Error>
    /// {
    ///     let (.., _authz_proj) = LookupPath::new(opctx, datastore)
    ///         .project_name(project_name)
    ///         .lookup_for(authz::Action::ListChildren)
    ///         .await?;
    ///     Err(nexus.unimplemented_todo(opctx, Unimpl::Public).await)
    /// }
    /// ```
    ///
    /// ### ... when the endpoint can return 404 (e.g., "get", "delete")
    ///
    /// You can treat this exactly like the second example above.  Here's an
    /// example stub for the "get" endpoint for that same resource:
    ///
    /// ```
    /// use nexus_db_queries::authz;
    /// use nexus_db_queries::context::OpContext;
    /// use nexus_db_queries::db::lookup::LookupPath;
    /// use nexus_db_queries::db::model::Name;
    /// use nexus_db_queries::db::DataStore;
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_common::api::external::Error;
    /// use omicron_common::api::external::LookupType;
    /// use omicron_common::api::external::ResourceType;
    ///
    /// async fn my_thing_fetch(
    ///     nexus: &Nexus,
    ///     datastore: &DataStore,
    ///     opctx: &OpContext,
    ///     the_name: &Name,
    /// ) -> Result<(), Error>
    /// {
    ///     // You will want to have defined your own ResourceType variant for
    ///     // this resource, even though it's still a stub.
    ///     let resource_type: ResourceType = todo!();
    ///     let lookup_type = LookupType::ByName(the_name.to_string());
    ///     let not_found_error = lookup_type.into_not_found(resource_type);
    ///     let unimp = Unimpl::ProtectedLookup(not_found_error);
    ///     Err(nexus.unimplemented_todo(opctx, unimp).await)
    /// }
    /// ```
    pub async fn unimplemented_todo(
        &self,
        opctx: &OpContext,
        visibility: Unimpl,
    ) -> Error {
        // Deny access to non-super-users.  This is really just for the benefit
        // of the authz coverage tests.  By requiring (and testing) correct
        // authz behavior for stubs, we ensure that that behavior is preserved
        // when the stub's implementation is fleshed out.
        match opctx.authorize(authz::Action::Modify, &authz::FLEET).await {
            Err(error @ Error::Forbidden) => {
                // Emulate the behavior of `Authz::authorize()`: if this is a
                // non-public resource, then the user should get a 404, not a
                // 403, when authorization fails.
                if let Unimpl::ProtectedLookup(lookup_error) = visibility {
                    lookup_error
                } else {
                    error
                }
            }
            Err(error) => error,
            Ok(_) => {
                // In the event that a superuser actually gets this far, produce
                // a server error.
                //
                // It's tempting to use other status codes here:
                //
                // "501 Not Implemented" is specifically when we don't recognize
                // the HTTP method and cannot implement it on _any_ resource.
                //
                // "405 Method Not Allowed" is specifically when an HTTP method
                // isn't supported.  That doesn't feel quite right either --
                // this is usually interpreted to mean "not part of the API",
                // which it obviously _is_, since the client found it in the API
                // spec.
                //
                // Neither of these is true: this HTTP method on this HTTP
                // resource is part of the API, and it will be supported by the
                // server, but it doesn't work yet.
                Error::internal_error("endpoint is not implemented")
            }
        }
    }

    pub fn datastore(&self) -> &Arc<db::DataStore> {
        &self.db_datastore
    }

    pub(crate) fn samael_max_issue_delay(&self) -> Option<chrono::Duration> {
        let mid = self.samael_max_issue_delay.lock().unwrap();
        *mid
    }

    pub fn resolver(&self) -> &internal_dns::resolver::Resolver {
        &self.internal_resolver
    }

    pub(crate) async fn dpd_clients(
        &self,
    ) -> Result<HashMap<SwitchLocation, dpd_client::Client>, String> {
        let resolver = self.resolver();
        dpd_clients(resolver, &self.log).await
    }

    pub(crate) async fn mg_clients(
        &self,
    ) -> Result<HashMap<SwitchLocation, mg_admin_client::Client>, String> {
        let resolver = self.resolver();
        let mappings =
            switch_zone_address_mappings(resolver, &self.log).await?;
        let mut clients: Vec<(SwitchLocation, mg_admin_client::Client)> =
            vec![];
        for (location, addr) in &mappings {
            let port = MGD_PORT;
            let socketaddr =
                std::net::SocketAddr::V6(SocketAddrV6::new(*addr, port, 0, 0));
            let client = mg_admin_client::Client::new(
                format!("http://{}", socketaddr).as_str(),
                self.log.clone(),
            );
            clients.push((*location, client));
        }
        Ok(clients.into_iter().collect::<HashMap<_, _>>())
    }
}

/// For unimplemented endpoints, indicates whether the resource identified
/// by this endpoint will always be publicly visible or not
///
/// For example, the resource "/system/images" is well-known (it's part of the
/// API).  Being unauthorized to list images will result in a "403
/// Forbidden".  It's `UnimplResourceVisibility::Public'.
///
/// By contrast, the resource "/system/images/some-image" is not publicly-known.
/// If you're not authorized to view it, you'll get a "404 Not Found".  It's
/// `Unimpl::ProtectedLookup(LookupType::ByName("some-image"))`.
pub enum Unimpl {
    #[allow(unused)]
    Public,
    ProtectedLookup(Error),
}

pub(crate) async fn dpd_clients(
    resolver: &internal_dns::resolver::Resolver,
    log: &slog::Logger,
) -> Result<HashMap<SwitchLocation, dpd_client::Client>, String> {
    let mappings = switch_zone_address_mappings(resolver, log).await?;
    let clients: HashMap<SwitchLocation, dpd_client::Client> = mappings
        .iter()
        .map(|(location, addr)| {
            let port = DENDRITE_PORT;

            let client_state = dpd_client::ClientState {
                tag: String::from("nexus"),
                log: log.new(o!(
                    "component" => "DpdClient"
                )),
            };

            let dpd_client = dpd_client::Client::new(
                &format!("http://[{addr}]:{port}"),
                client_state,
            );
            (*location, dpd_client)
        })
        .collect();
    Ok(clients)
}

async fn switch_zone_address_mappings(
    resolver: &internal_dns::resolver::Resolver,
    log: &slog::Logger,
) -> Result<HashMap<SwitchLocation, Ipv6Addr>, String> {
    let switch_zone_addresses = match resolver
        .lookup_all_ipv6(ServiceName::Dendrite)
        .await
    {
        Ok(addrs) => addrs,
        Err(e) => {
            error!(log, "failed to resolve addresses for Dendrite services"; "error" => %e);
            return Err(e.to_string());
        }
    };
    Ok(map_switch_zone_addrs(&log, switch_zone_addresses).await)
}

// TODO: #3596 Allow updating of Nexus from `handoff_to_nexus()`
// This logic is duplicated from RSS
// RSS needs to know which addresses are managing which slots, and so does Nexus,
// but it doesn't seem like we can just pass the discovered information off
// from RSS once Nexus is running since we can't mutate the state in Nexus
// via an API call. We probably will need to rethink how we're looking
// up switch addresses as a whole, since how DNS is currently setup for
// Dendrite is insufficient for what we need.
async fn map_switch_zone_addrs(
    log: &Logger,
    switch_zone_addresses: Vec<Ipv6Addr>,
) -> HashMap<SwitchLocation, Ipv6Addr> {
    use gateway_client::Client as MgsClient;
    info!(log, "Determining switch slots managed by switch zones");
    let mut switch_zone_addrs = HashMap::new();
    for addr in switch_zone_addresses {
        let mgs_client = MgsClient::new(
            &format!("http://[{}]:{}", addr, MGS_PORT),
            log.new(o!("component" => "MgsClient")),
        );

        info!(log, "determining switch slot managed by dendrite zone"; "zone_address" => #?addr);
        // TODO: #3599 Use retry function instead of looping on a fixed timer
        let switch_slot = loop {
            match mgs_client.sp_local_switch_id().await {
                Ok(switch) => {
                    info!(
                        log,
                        "identified switch slot for dendrite zone";
                        "slot" => #?switch,
                        "zone_address" => #?addr
                    );
                    break switch.slot;
                }
                Err(e) => {
                    warn!(
                        log,
                        "failed to identify switch slot for dendrite, will retry in 2 seconds";
                        "zone_address" => #?addr,
                        "reason" => #?e
                    );
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        };

        match switch_slot {
            0 => {
                switch_zone_addrs.insert(SwitchLocation::Switch0, addr);
            }
            1 => {
                switch_zone_addrs.insert(SwitchLocation::Switch1, addr);
            }
            _ => {
                warn!(log, "Expected a slot number of 0 or 1, found {switch_slot:#?} when querying {addr:#?}");
            }
        };
    }
    info!(
        log,
        "completed mapping dendrite zones to switch slots";
        "mappings" => #?switch_zone_addrs
    );
    switch_zone_addrs
}
