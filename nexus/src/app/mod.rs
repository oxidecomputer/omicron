// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus, the service that operates much of the control plane in an Oxide fleet

use self::external_endpoints::NexusCertResolver;
use crate::app::oximeter::LazyTimeseriesClient;
use crate::authn;
use crate::authz;
use crate::config;
use crate::db;
use crate::populate::populate_start;
use crate::populate::PopulateArgs;
use crate::populate::PopulateStatus;
use crate::saga_interface::SagaContext;
use crate::DropshotServer;
use ::oximeter::types::ProducerRegistry;
use anyhow::anyhow;
use internal_dns::ServiceName;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::Error;
use slog::Logger;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// The implementation of Nexus is large, and split into a number of submodules
// by resource.
mod address_lot;
pub mod background;
mod certificate;
mod device_auth;
mod disk;
pub mod external_endpoints;
mod external_ip;
mod iam;
mod image;
mod instance;
mod ip_pool;
mod metrics;
mod network_interface;
mod oximeter;
mod project;
mod rack;
pub mod saga;
mod session;
mod silo;
mod sled;
mod sled_instance;
mod snapshot;
mod switch;
mod switch_interface;
mod switch_port;
pub mod test_interfaces;
mod update;
mod volume;
mod vpc;
mod vpc_router;
mod vpc_subnet;

// Sagas are not part of the "Nexus" implementation, but they are
// application logic.
pub mod sagas;

// TODO: When referring to API types, we should try to include
// the prefix unless it is unambiguous.

pub(crate) const MAX_DISKS_PER_INSTANCE: u32 = 8;

pub(crate) const MAX_NICS_PER_INSTANCE: usize = 8;

// TODO-completeness: Support multiple external IPs
pub(crate) const MAX_EXTERNAL_IPS_PER_INSTANCE: usize = 1;

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

    /// saga execution coordinator
    sec_client: Arc<steno::SecClient>,

    /// Task representing completion of recovered Sagas
    recovery_task: std::sync::Mutex<Option<db::RecoveryTask>>,

    /// External dropshot servers
    external_server: std::sync::Mutex<Option<DropshotServer>>,

    /// Internal dropshot server
    internal_server: std::sync::Mutex<Option<DropshotServer>>,

    /// Status of background task to populate database
    populate_status: tokio::sync::watch::Receiver<PopulateStatus>,

    /// Client to the timeseries database.
    timeseries_client: LazyTimeseriesClient,

    /// Contents of the trusted root role for the TUF repository.
    updates_config: Option<config::UpdatesConfig>,

    /// The tunable parameters from a configuration file
    tunables: config::Tunables,

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

    resolver: Arc<Mutex<internal_dns::resolver::Resolver>>,

    /// Client for dataplane daemon / switch management API
    dpd_client: Arc<dpd_client::Client>,

    /// Background tasks
    background_tasks: background::BackgroundTasks,
}

impl Nexus {
    /// Create a new Nexus instance for the given rack id `rack_id`
    // TODO-polish revisit rack metadata
    pub async fn new_with_id(
        rack_id: Uuid,
        log: Logger,
        resolver: Arc<Mutex<internal_dns::resolver::Resolver>>,
        pool: db::Pool,
        producer_registry: &ProducerRegistry,
        config: &config::Config,
        authz: Arc<authz::Authz>,
    ) -> Result<Arc<Nexus>, String> {
        let pool = Arc::new(pool);
        let db_datastore = Arc::new(db::DataStore::new(Arc::clone(&pool)));
        db_datastore.register_producers(&producer_registry);

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

        let client_state = dpd_client::ClientState {
            tag: String::from("nexus"),
            log: log.new(o!(
                "component" => "DpdClient"
            )),
        };
        let (dpd_host, dpd_port) = if let Some(dpd_address) =
            &config.pkg.dendrite.address
        {
            (dpd_address.ip().to_string(), dpd_address.port())
        } else {
            let addr = resolver
                .lock()
                .await
                .lookup_socket_v6(ServiceName::Dendrite)
                .await
                .map_err(|e| format!("Cannot access Dendrite address: {e}"))?;
            (addr.ip().to_string(), addr.port())
        };
        let dpd_client = Arc::new(dpd_client::Client::new(
            &format!("http://[{dpd_host}]:{dpd_port}"),
            client_state,
        ));

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
            Arc::clone(&db_datastore),
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
            Arc::clone(&db_datastore),
        );
        let background_tasks = background::BackgroundTasks::start(
            &background_ctx,
            Arc::clone(&db_datastore),
            &config.pkg.background_tasks,
        );

        let nexus = Nexus {
            id: config.deployment.id,
            rack_id,
            log: log.new(o!()),
            db_datastore: Arc::clone(&db_datastore),
            authz: Arc::clone(&authz),
            sec_client: Arc::clone(&sec_client),
            recovery_task: std::sync::Mutex::new(None),
            external_server: std::sync::Mutex::new(None),
            internal_server: std::sync::Mutex::new(None),
            populate_status,
            timeseries_client,
            updates_config: config.pkg.updates.clone(),
            tunables: config.pkg.tunables.clone(),
            opctx_alloc: OpContext::for_background(
                log.new(o!("component" => "InstanceAllocator")),
                Arc::clone(&authz),
                authn::Context::internal_read(),
                Arc::clone(&db_datastore),
            ),
            opctx_external_authn: OpContext::for_background(
                log.new(o!("component" => "ExternalAuthn")),
                Arc::clone(&authz),
                authn::Context::external_authn(),
                Arc::clone(&db_datastore),
            ),
            samael_max_issue_delay: std::sync::Mutex::new(None),
            resolver,
            dpd_client,
            background_tasks,
        };

        // TODO-cleanup all the extra Arcs here seems wrong
        let nexus = Arc::new(nexus);
        let opctx = OpContext::for_background(
            log.new(o!("component" => "SagaRecoverer")),
            Arc::clone(&authz),
            authn::Context::internal_saga_recovery(),
            Arc::clone(&db_datastore),
        );
        let saga_logger = nexus.log.new(o!("saga_type" => "recovery"));
        let recovery_task = db::recover(
            opctx,
            my_sec_id,
            Arc::new(Arc::new(SagaContext::new(
                Arc::clone(&nexus),
                saga_logger,
                Arc::clone(&authz),
            ))),
            db_datastore,
            Arc::clone(&sec_client),
            sagas::ACTION_REGISTRY.clone(),
        );

        *nexus.recovery_task.lock().unwrap() = Some(recovery_task);

        // Kick all background tasks once the populate step finishes.  Among
        // other things, the populate step installs role assignments for
        // internal identities that are used by the background tasks.  If we
        // don't do this here, those tasks might fail spuriously on startup and
        // not be retried for a while.
        let task_nexus = nexus.clone();
        let task_log = nexus.log.clone();
        tokio::spawn(async move {
            match task_nexus.wait_for_populate().await {
                Ok(_) => {
                    info!(
                        task_log,
                        "populate complete; activating background tasks"
                    );
                    for task in task_nexus.background_tasks.driver.tasks() {
                        task_nexus.background_tasks.driver.activate(task);
                    }
                }
                Err(_) => {
                    error!(task_log, "populate failed");
                }
            }
        });

        Ok(nexus)
    }

    /// Return the tunable configuration parameters, e.g. for use in tests.
    pub fn tunables(&self) -> &config::Tunables {
        &self.tunables
    }

    pub async fn wait_for_populate(&self) -> Result<(), anyhow::Error> {
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

    pub async fn external_tls_config(
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
            .with_safe_default_cipher_suites()
            .with_safe_default_kx_groups()
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(NexusCertResolver::new(
                self.log.new(o!("component" => "NexusCertResolver")),
                self.background_tasks.external_endpoints.clone(),
            )));
        rustls_cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Some(rustls_cfg)
    }

    // Called to hand off management of external servers to Nexus.
    pub(crate) async fn set_servers(
        &self,
        external_server: DropshotServer,
        internal_server: DropshotServer,
    ) {
        // If any servers already exist, close them.
        let _ = self.close_servers().await;

        // Insert the new servers.
        self.external_server.lock().unwrap().replace(external_server);
        self.internal_server.lock().unwrap().replace(internal_server);
    }

    pub async fn close_servers(&self) -> Result<(), String> {
        let external_server = self.external_server.lock().unwrap().take();
        if let Some(server) = external_server {
            server.close().await?;
        }
        let internal_server = self.internal_server.lock().unwrap().take();
        if let Some(server) = internal_server {
            server.close().await?;
        }
        Ok(())
    }

    pub async fn wait_for_shutdown(&self) -> Result<(), String> {
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

    pub async fn get_external_server_address(
        &self,
    ) -> Option<std::net::SocketAddr> {
        self.external_server
            .lock()
            .unwrap()
            .as_ref()
            .map(|server| server.local_addr())
    }

    pub async fn get_internal_server_address(
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
    pub fn opctx_for_service_balancer(&self) -> OpContext {
        OpContext::for_background(
            self.log.new(o!("component" => "ServiceBalancer")),
            Arc::clone(&self.authz),
            authn::Context::internal_service_balancer(),
            Arc::clone(&self.db_datastore),
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
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_nexus::db::DataStore;
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
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_nexus::db::model::Name;
    /// use omicron_nexus::db::DataStore;
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
    /// use nexus_db_queries::context::OpContext;
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_nexus::authz;
    /// use omicron_nexus::db::lookup::LookupPath;
    /// use omicron_nexus::db::model::Name;
    /// use omicron_nexus::db::DataStore;
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
    /// use nexus_db_queries::context::OpContext;
    /// use omicron_nexus::app::Nexus;
    /// use omicron_nexus::app::Unimpl;
    /// use omicron_nexus::authz;
    /// use omicron_nexus::db::lookup::LookupPath;
    /// use omicron_nexus::db::model::Name;
    /// use omicron_nexus::db::DataStore;
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

    pub fn samael_max_issue_delay(&self) -> Option<chrono::Duration> {
        let mid = self.samael_max_issue_delay.lock().unwrap();
        *mid
    }

    // Convenience function that exists solely because writing
    // LookupPath::new(&opctx, &nexus.datastore()) in an endpoint handler feels
    // like too much
    pub fn db_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
    ) -> db::lookup::LookupPath {
        db::lookup::LookupPath::new(opctx, &self.db_datastore)
    }

    pub async fn set_resolver(
        &self,
        resolver: internal_dns::resolver::Resolver,
    ) {
        *self.resolver.lock().await = resolver;
    }

    pub async fn resolver(&self) -> internal_dns::resolver::Resolver {
        let resolver = self.resolver.lock().await;
        resolver.clone()
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
    Public,
    ProtectedLookup(Error),
}
