// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus, the service that operates much of the control plane in an Oxide fleet

use crate::authn;
use crate::authz;
use crate::config;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::{Asset, Resource};
use crate::db::lookup::LookupPath;
use crate::db::model::DatasetKind;
use crate::db::model::Name;
use crate::db::model::RouterRoute;
use crate::db::model::SiloUser;
use crate::db::model::UpdateArtifactKind;
use crate::db::model::VpcRouter;
use crate::db::model::VpcRouterKind;
use crate::db::model::VpcSubnet;
use crate::db::subnet_allocation::NetworkInterfaceError;
use crate::db::subnet_allocation::SubnetError;
use crate::defaults;
use crate::external_api::params;
use crate::internal_api::params::{OximeterInfo, ZpoolPutRequest};
use crate::populate::populate_start;
use crate::populate::PopulateStatus;
use crate::saga_interface::SagaContext;
use crate::sagas;
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use futures::future::ready;
use futures::StreamExt;
use hex;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::PaginationOrder;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::RouterRouteUpdateParams;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::UpdateArtifact;
use omicron_common::backoff;
use omicron_common::bail_unless;
use oximeter_client::Client as OximeterClient;
use oximeter_db::TimeseriesSchema;
use oximeter_db::TimeseriesSchemaPaginationParams;
use oximeter_producer::register;
use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};
use ring::digest;
use sled_agent_client::types::InstanceRuntimeStateMigrateParams;
use sled_agent_client::types::InstanceRuntimeStateRequested;
use sled_agent_client::types::InstanceStateRequested;
use sled_agent_client::Client as SledAgentClient;
use slog::Logger;
use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use steno::SagaId;
use steno::SagaResultOk;
use steno::SagaTemplate;
use steno::SagaType;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

// TODO: When referring to API types, we should try to include
// the prefix unless it is unambiguous.

/// Exposes additional [`Nexus`] interfaces for use by the test suite
#[async_trait]
pub trait TestInterfaces {
    /// Returns the SledAgentClient for an Instance from its id.  We may also
    /// want to split this up into instance_lookup_by_id() and instance_sled(),
    /// but after all it's a test suite special to begin with.
    async fn instance_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error>;

    /// Returns the SledAgentClient for a Disk from its id.
    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error>;

    async fn set_disk_as_faulted(&self, disk_id: &Uuid) -> Result<bool, Error>;

    async fn silo_user_create(
        &self,
        silo_id: Uuid,
        silo_user_id: Uuid,
    ) -> CreateResult<SiloUser>;
}

pub static BASE_ARTIFACT_DIR: &str = "/var/tmp/oxide_artifacts";

pub(crate) const MAX_DISKS_PER_INSTANCE: u32 = 8;

pub(crate) const MAX_NICS_PER_INSTANCE: u32 = 8;

/// Manages an Oxide fleet -- the heart of the control plane
pub struct Nexus {
    /// uuid for this nexus instance.
    id: Uuid,

    /// uuid for this rack (TODO should also be in persistent storage)
    rack_id: Uuid,

    /// general server log
    log: Logger,

    /// cached rack identity metadata
    api_rack_identity: db::model::RackIdentity,

    /// persistent storage for resources in the control plane
    db_datastore: Arc<db::DataStore>,

    /// handle to global authz information
    authz: Arc<authz::Authz>,

    /// saga execution coordinator
    sec_client: Arc<steno::SecClient>,

    /// Task representing completion of recovered Sagas
    recovery_task: std::sync::Mutex<Option<db::RecoveryTask>>,

    /// Status of background task to populate database
    populate_status: tokio::sync::watch::Receiver<PopulateStatus>,

    /// Client to the timeseries database.
    timeseries_client: oximeter_db::Client,

    /// Contents of the trusted root role for the TUF repository.
    updates_config: Option<config::UpdatesConfig>,

    /// Operational context used for Instance allocation
    opctx_alloc: OpContext,

    /// Operational context used for external request authentication
    opctx_external_authn: OpContext,
}

// TODO Is it possible to make some of these operations more generic?  A
// particularly good example is probably list() (or even lookup()), where
// with the right type parameters, generic code can be written to work on all
// types.
//
// TODO update and delete need to accommodate both with-etag and don't-care
// TODO audit logging ought to be part of this structure and its functions
impl Nexus {
    /// Create a new Nexus instance for the given rack id `rack_id`
    // TODO-polish revisit rack metadata
    pub fn new_with_id(
        rack_id: Uuid,
        log: Logger,
        pool: db::Pool,
        config: &config::Config,
        authz: Arc<authz::Authz>,
    ) -> Arc<Nexus> {
        let pool = Arc::new(pool);
        let my_sec_id = db::SecId::from(config.id);
        let db_datastore = Arc::new(db::DataStore::new(Arc::clone(&pool)));
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
        let timeseries_client =
            oximeter_db::Client::new(config.timeseries_db.address, &log);

        // TODO-cleanup We may want a first-class subsystem for managing startup
        // background tasks.  It could use a Future for each one, a status enum
        // for each one, status communication via channels, and a single task to
        // run them all.
        let populate_ctx = OpContext::for_background(
            log.new(o!("component" => "DataLoader")),
            Arc::clone(&authz),
            authn::Context::internal_db_init(),
            Arc::clone(&db_datastore),
        );
        let populate_status =
            populate_start(populate_ctx, Arc::clone(&db_datastore));

        let nexus = Nexus {
            id: config.id,
            rack_id,
            log: log.new(o!()),
            api_rack_identity: db::model::RackIdentity::new(rack_id),
            db_datastore: Arc::clone(&db_datastore),
            authz: Arc::clone(&authz),
            sec_client: Arc::clone(&sec_client),
            recovery_task: std::sync::Mutex::new(None),
            populate_status,
            timeseries_client,
            updates_config: config.updates.clone(),
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
            &sagas::ALL_TEMPLATES,
        );

        *nexus.recovery_task.lock().unwrap() = Some(recovery_task);
        nexus
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

    /// Returns an [`OpContext`] used for authenticating external requests
    pub fn opctx_external_authn(&self) -> &OpContext {
        &self.opctx_external_authn
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
    /// use omicron_nexus::context::OpContext;
    /// use omicron_nexus::db::DataStore;
    /// use omicron_nexus::nexus::Nexus;
    /// use omicron_nexus::nexus::Unimpl;
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
    /// use omicron_nexus::context::OpContext;
    /// use omicron_nexus::db::model::Name;
    /// use omicron_nexus::db::DataStore;
    /// use omicron_nexus::nexus::Nexus;
    /// use omicron_nexus::nexus::Unimpl;
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
    /// use omicron_nexus::authz;
    /// use omicron_nexus::context::OpContext;
    /// use omicron_nexus::db::lookup::LookupPath;
    /// use omicron_nexus::db::model::Name;
    /// use omicron_nexus::db::DataStore;
    /// use omicron_nexus::nexus::Nexus;
    /// use omicron_nexus::nexus::Unimpl;
    /// use omicron_common::api::external::Error;
    ///
    /// async fn organization_list_my_thing(
    ///     nexus: &Nexus,
    ///     datastore: &DataStore,
    ///     opctx: &OpContext,
    ///     organization_name: &Name,
    /// ) -> Result<(), Error>
    /// {
    ///     let (.., _authz_org) = LookupPath::new(opctx, datastore)
    ///         .organization_name(organization_name)
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
    /// use omicron_nexus::authz;
    /// use omicron_nexus::context::OpContext;
    /// use omicron_nexus::db::lookup::LookupPath;
    /// use omicron_nexus::db::model::Name;
    /// use omicron_nexus::db::DataStore;
    /// use omicron_nexus::nexus::Nexus;
    /// use omicron_nexus::nexus::Unimpl;
    /// use omicron_common::api::external::Error;
    /// use omicron_common::api::external::LookupType;
    /// use omicron_common::api::external::ResourceType;
    ///
    /// async fn my_thing_fetch(
    ///     nexus: &Nexus,
    ///     datastore: &DataStore,
    ///     opctx: &OpContext,
    ///     organization_name: &Name,
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

    // TODO-robustness we should have a limit on how many sled agents there can
    // be (for graceful degradation at large scale).
    pub async fn upsert_sled(
        &self,
        id: Uuid,
        address: SocketAddr,
    ) -> Result<(), Error> {
        info!(self.log, "registered sled agent"; "sled_uuid" => id.to_string());
        let sled = db::model::Sled::new(id, address);
        self.db_datastore.sled_upsert(sled).await?;
        Ok(())
    }

    /// Upserts a Zpool into the database, updating it if it already exists.
    pub async fn upsert_zpool(
        &self,
        id: Uuid,
        sled_id: Uuid,
        info: ZpoolPutRequest,
    ) -> Result<(), Error> {
        info!(self.log, "upserting zpool"; "sled_id" => sled_id.to_string(), "zpool_id" => id.to_string());
        let zpool = db::model::Zpool::new(id, sled_id, &info);
        self.db_datastore.zpool_upsert(zpool).await?;
        Ok(())
    }

    /// Upserts a dataset into the database, updating it if it already exists.
    pub async fn upsert_dataset(
        &self,
        id: Uuid,
        zpool_id: Uuid,
        address: SocketAddr,
        kind: DatasetKind,
    ) -> Result<(), Error> {
        info!(self.log, "upserting dataset"; "zpool_id" => zpool_id.to_string(), "dataset_id" => id.to_string(), "address" => address.to_string());
        let dataset = db::model::Dataset::new(id, zpool_id, address, kind);
        self.db_datastore.dataset_upsert(dataset).await?;
        Ok(())
    }

    /// Insert a new record of an Oximeter collector server.
    pub async fn upsert_oximeter_collector(
        &self,
        oximeter_info: &OximeterInfo,
    ) -> Result<(), Error> {
        // Insert the Oximeter instance into the DB. Note that this _updates_ the record,
        // specifically, the time_modified, ip, and port columns, if the instance has already been
        // registered.
        let db_info = db::model::OximeterInfo::new(&oximeter_info);
        self.db_datastore.oximeter_create(&db_info).await?;
        info!(
            self.log,
            "registered new oximeter metric collection server";
            "collector_id" => ?oximeter_info.collector_id,
            "address" => oximeter_info.address,
        );

        // Regardless, notify the collector of any assigned metric producers. This should be empty
        // if this Oximeter collector is registering for the first time, but may not be if the
        // service is re-registering after failure.
        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(100).unwrap(),
        };
        let producers = self
            .db_datastore
            .producers_list_by_oximeter_id(
                oximeter_info.collector_id,
                &pagparams,
            )
            .await?;
        if !producers.is_empty() {
            debug!(
                self.log,
                "registered oximeter collector that is already assigned producers, re-assigning them to the collector";
                "n_producers" => producers.len(),
                "collector_id" => ?oximeter_info.collector_id,
            );
            let client = self.build_oximeter_client(
                &oximeter_info.collector_id,
                oximeter_info.address,
            );
            for producer in producers.into_iter() {
                let producer_info = oximeter_client::types::ProducerEndpoint {
                    id: producer.id(),
                    address: SocketAddr::new(
                        producer.ip.ip(),
                        producer.port.try_into().unwrap(),
                    )
                    .to_string(),
                    base_route: producer.base_route,
                    interval: oximeter_client::types::Duration::from(
                        Duration::from_secs_f64(producer.interval),
                    ),
                };
                client
                    .producers_post(&producer_info)
                    .await
                    .map_err(Error::from)?;
            }
        }
        Ok(())
    }

    /// Register as a metric producer with the oximeter metric collection server.
    pub async fn register_as_producer(&self, address: SocketAddr) {
        let producer_endpoint = nexus::ProducerEndpoint {
            id: self.id,
            address,
            base_route: String::from("/metrics/collect"),
            interval: Duration::from_secs(10),
        };
        let register = || async {
            debug!(self.log, "registering nexus as metric producer");
            register(address, &self.log, &producer_endpoint)
                .await
                .map_err(backoff::BackoffError::transient)
        };
        let log_registration_failure = |error, delay| {
            warn!(
                self.log,
                "failed to register nexus as a metric producer, will retry in {:?}", delay;
                "error_message" => ?error,
            );
        };
        backoff::retry_notify(
            backoff::internal_service_policy(),
            register,
            log_registration_failure,
        ).await
        .expect("expected an infinite retry loop registering nexus as a metric producer");
    }

    // Internal helper to build an Oximeter client from its ID and address (common data between
    // model type and the API type).
    fn build_oximeter_client(
        &self,
        id: &Uuid,
        address: SocketAddr,
    ) -> OximeterClient {
        let client_log =
            self.log.new(o!("oximeter-collector" => id.to_string()));
        let client =
            OximeterClient::new(&format!("http://{}", address), client_log);
        info!(
            self.log,
            "registered oximeter collector client";
            "id" => id.to_string(),
        );
        client
    }

    /// List all registered Oximeter collector instances.
    pub async fn oximeter_list(
        &self,
        page_params: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::OximeterInfo> {
        self.db_datastore.oximeter_list(page_params).await
    }

    pub fn datastore(&self) -> &Arc<db::DataStore> {
        &self.db_datastore
    }

    /// Given a saga template and parameters, create a new saga and execute it.
    async fn execute_saga<P, S>(
        self: &Arc<Self>,
        saga_template: Arc<SagaTemplate<S>>,
        template_name: &str,
        saga_params: Arc<P>,
    ) -> Result<SagaResultOk, Error>
    where
        S: SagaType<
            ExecContextType = Arc<SagaContext>,
            SagaParamsType = Arc<P>,
        >,
        // TODO-cleanup The bound `P: Serialize` should not be necessary because
        // SagaParamsType must already impl Serialize.
        P: serde::Serialize,
    {
        let saga_id = SagaId(Uuid::new_v4());
        let saga_logger =
            self.log.new(o!("template_name" => template_name.to_owned()));
        let saga_context = Arc::new(Arc::new(SagaContext::new(
            Arc::clone(self),
            saga_logger,
            Arc::clone(&self.authz),
        )));
        let future = self
            .sec_client
            .saga_create(
                saga_id,
                saga_context,
                saga_template,
                template_name.to_owned(),
                saga_params,
            )
            .await
            .context("creating saga")
            .map_err(|error| {
                // TODO-error This could be a service unavailable error,
                // depending on the failure mode.  We need more information from
                // Steno.
                Error::internal_error(&format!("{:#}", error))
            })?;

        self.sec_client
            .saga_start(saga_id)
            .await
            .context("starting saga")
            .map_err(|error| Error::internal_error(&format!("{:#}", error)))?;

        let result = future.await;
        result.kind.map_err(|saga_error| {
            saga_error.error_source.convert::<Error>().unwrap_or_else(|e| {
                // TODO-error more context would be useful
                Error::InternalError { internal_message: e.to_string() }
            })
        })
    }

    /*
     * Silos
     */

    pub async fn silo_create(
        &self,
        opctx: &OpContext,
        new_silo_params: params::SiloCreate,
    ) -> CreateResult<db::model::Silo> {
        let silo = db::model::Silo::new(new_silo_params);
        self.db_datastore.silo_create(opctx, silo).await
    }

    pub async fn silo_fetch(
        &self,
        opctx: &OpContext,
        name: &Name,
    ) -> LookupResult<db::model::Silo> {
        let (.., db_silo) = LookupPath::new(opctx, &self.db_datastore)
            .silo_name(name)
            .fetch()
            .await?;
        Ok(db_silo)
    }

    pub async fn silos_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Silo> {
        self.db_datastore.silos_list_by_name(opctx, pagparams).await
    }

    pub async fn silos_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Silo> {
        self.db_datastore.silos_list_by_id(opctx, pagparams).await
    }

    pub async fn silo_delete(
        &self,
        opctx: &OpContext,
        name: &Name,
    ) -> DeleteResult {
        let (.., authz_silo, db_silo) =
            LookupPath::new(opctx, &self.db_datastore)
                .silo_name(name)
                .fetch_for(authz::Action::Delete)
                .await?;
        self.db_datastore.silo_delete(opctx, &authz_silo, &db_silo).await
    }

    // Organizations

    pub async fn organization_create(
        &self,
        opctx: &OpContext,
        new_organization: &params::OrganizationCreate,
    ) -> CreateResult<db::model::Organization> {
        self.db_datastore.organization_create(opctx, new_organization).await
    }

    pub async fn organization_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
    ) -> LookupResult<db::model::Organization> {
        let (.., db_organization) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .fetch()
            .await?;
        Ok(db_organization)
    }

    pub async fn organizations_list_by_name(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Organization> {
        self.db_datastore.organizations_list_by_name(opctx, pagparams).await
    }

    pub async fn organizations_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Organization> {
        self.db_datastore.organizations_list_by_id(opctx, pagparams).await
    }

    pub async fn organization_delete(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
    ) -> DeleteResult {
        let (.., authz_org, db_org) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .fetch()
                .await?;
        self.db_datastore.organization_delete(opctx, &authz_org, &db_org).await
    }

    pub async fn organization_update(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        new_params: &params::OrganizationUpdate,
    ) -> UpdateResult<db::model::Organization> {
        let (.., authz_organization) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .lookup_for(authz::Action::Modify)
                .await?;
        self.db_datastore
            .organization_update(
                opctx,
                &authz_organization,
                new_params.clone().into(),
            )
            .await
    }

    // Projects

    pub async fn project_create(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        new_project: &params::ProjectCreate,
    ) -> CreateResult<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        // Create a project.
        let db_project =
            db::model::Project::new(authz_org.id(), new_project.clone());
        let db_project = self
            .db_datastore
            .project_create(opctx, &authz_org, db_project)
            .await?;

        // TODO: We probably want to have "project creation" and "default VPC
        // creation" co-located within a saga for atomicity.
        //
        // Until then, we just perform the operations sequentially.

        // Create a default VPC associated with the project.
        // TODO-correctness We need to be using the project_id we just created.
        // project_create() should return authz::Project and we should use that
        // here.
        let _ = self
            .project_create_vpc(
                opctx,
                &organization_name,
                &new_project.identity.name.clone().into(),
                &params::VpcCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "default".parse().unwrap(),
                        description: "Default VPC".to_string(),
                    },
                    ipv6_prefix: Some(defaults::random_vpc_ipv6_prefix()?),
                    // TODO-robustness this will need to be None if we decide to
                    // handle the logic around name and dns_name by making
                    // dns_name optional
                    dns_name: "default".parse().unwrap(),
                },
            )
            .await?;

        Ok(db_project)
    }

    pub async fn project_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
    ) -> LookupResult<db::model::Project> {
        let (.., db_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .fetch()
            .await?;
        Ok(db_project)
    }

    pub async fn projects_list_by_name(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        self.db_datastore
            .projects_list_by_name(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn projects_list_by_id(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        let (.., authz_org) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        self.db_datastore
            .projects_list_by_id(opctx, &authz_org, pagparams)
            .await
    }

    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
    ) -> DeleteResult {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore.project_delete(opctx, &authz_project).await
    }

    pub async fn project_update(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        new_params: &params::ProjectUpdate,
    ) -> UpdateResult<db::model::Project> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .project_update(opctx, &authz_project, new_params.clone().into())
            .await
    }

    // Disks

    pub async fn project_list_disks(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Disk> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_disks(opctx, &authz_project, pagparams)
            .await
    }

    pub async fn project_create_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::DiskCreate,
    ) -> CreateResult<db::model::Disk> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        // Reject disks where the block size doesn't evenly divide the total
        // size
        if (params.size.to_bytes() % params.block_size().to_bytes()) != 0 {
            return Err(Error::InvalidValue {
                label: String::from("size and block_size"),
                message: String::from(
                    "total size must be a multiple of block size",
                ),
            });
        }

        // Until we implement snapshots, do not allow disks to be created from a
        // snapshot.
        if params.snapshot_id.is_some() {
            return Err(Error::InvalidValue {
                label: String::from("snapshot_id"),
                message: String::from("snapshots are not yet supported"),
            });
        }

        // Until we implement images, do not allow disks to be created from an
        // image.
        if params.image_id.is_some() {
            return Err(Error::InvalidValue {
                label: String::from("image_id"),
                message: String::from("images are not yet supported"),
            });
        }

        let saga_params = Arc::new(sagas::ParamsDiskCreate {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            project_id: authz_project.id(),
            create_params: params.clone(),
        });
        let saga_outputs = self
            .execute_saga(
                Arc::clone(&sagas::SAGA_DISK_CREATE_TEMPLATE),
                sagas::SAGA_DISK_CREATE_NAME,
                saga_params,
            )
            .await?;
        let disk_created = saga_outputs
            .lookup_output::<db::model::Disk>("created_disk")
            .map_err(|e| Error::InternalError {
                internal_message: e.to_string(),
            })?;
        Ok(disk_created)
    }

    pub async fn disk_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> LookupResult<db::model::Disk> {
        let (.., db_disk) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .disk_name(disk_name)
            .fetch()
            .await?;
        Ok(db_disk)
    }

    pub async fn project_delete_disk(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> DeleteResult {
        let (.., authz_disk) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .disk_name(disk_name)
            .lookup_for(authz::Action::Delete)
            .await?;

        let saga_params =
            Arc::new(sagas::ParamsDiskDelete { disk_id: authz_disk.id() });
        self.execute_saga(
            Arc::clone(&sagas::SAGA_DISK_DELETE_TEMPLATE),
            sagas::SAGA_DISK_DELETE_NAME,
            saga_params,
        )
        .await?;

        Ok(())
    }

    pub async fn images_list(
        &self,
        opctx: &OpContext,
        _pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Image> {
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn image_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        _params: &params::ImageCreate,
    ) -> CreateResult<db::model::Image> {
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn image_fetch(
        &self,
        opctx: &OpContext,
        image_name: &Name,
    ) -> LookupResult<db::model::Image> {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let error = lookup_type.into_not_found(ResourceType::Image);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(error))
            .await)
    }

    pub async fn image_delete(
        self: &Arc<Self>,
        opctx: &OpContext,
        image_name: &Name,
    ) -> DeleteResult {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let error = lookup_type.into_not_found(ResourceType::Image);
        Err(self
            .unimplemented_todo(opctx, Unimpl::ProtectedLookup(error))
            .await)
    }

    pub async fn project_list_images(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Image> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_create_image(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _params: &params::ImageCreate,
    ) -> CreateResult<db::model::Image> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_image_fetch(
        &self,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        image_name: &Name,
    ) -> LookupResult<db::model::Image> {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let not_found_error = lookup_type.into_not_found(ResourceType::Image);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub async fn project_delete_image(
        self: &Arc<Self>,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        image_name: &Name,
    ) -> DeleteResult {
        let lookup_type = LookupType::ByName(image_name.to_string());
        let not_found_error = lookup_type.into_not_found(ResourceType::Image);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub async fn project_create_snapshot(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _params: &params::SnapshotCreate,
    ) -> CreateResult<db::model::Snapshot> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn project_list_snapshots(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        _pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Snapshot> {
        let _ = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        Err(self.unimplemented_todo(opctx, Unimpl::Public).await)
    }

    pub async fn snapshot_fetch(
        &self,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        snapshot_name: &Name,
    ) -> LookupResult<db::model::Snapshot> {
        let lookup_type = LookupType::ByName(snapshot_name.to_string());
        let not_found_error =
            lookup_type.into_not_found(ResourceType::Snapshot);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    pub async fn project_delete_snapshot(
        self: &Arc<Self>,
        opctx: &OpContext,
        _organization_name: &Name,
        _project_name: &Name,
        snapshot_name: &Name,
    ) -> DeleteResult {
        let lookup_type = LookupType::ByName(snapshot_name.to_string());
        let not_found_error =
            lookup_type.into_not_found(ResourceType::Snapshot);
        let unimp = Unimpl::ProtectedLookup(not_found_error);
        Err(self.unimplemented_todo(opctx, unimp).await)
    }

    // Instances

    // TODO-design This interface should not exist.  See
    // SagaContext::alloc_server().
    pub async fn sled_allocate(&self) -> Result<Uuid, Error> {
        // We need an OpContext to query the database.  Normally we'd use
        // one from the current operation, usually a saga action or API call.
        // In this case, though, the caller may not have permissions to access
        // the sleds in the system.  We're really doing this as Nexus itself,
        // operating on behalf of the caller.
        let opctx = &self.opctx_alloc;

        // TODO: replace this with a real allocation policy.
        //
        // This implementation always assigns the first sled (by ID order).
        let pagparams = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(1).unwrap(),
        };
        let sleds = self.db_datastore.sled_list(&opctx, &pagparams).await?;

        sleds
            .first()
            .ok_or_else(|| Error::ServiceUnavailable {
                internal_message: String::from(
                    "no sleds available for new Instance",
                ),
            })
            .map(|s| s.id())
    }

    pub async fn project_list_instances(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Instance> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_instances(opctx, &authz_project, pagparams)
            .await
    }

    pub async fn project_create_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::InstanceCreate,
    ) -> CreateResult<db::model::Instance> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        // Validate parameters
        if params.disks.len() > MAX_DISKS_PER_INSTANCE as usize {
            return Err(Error::invalid_request(&format!(
                "cannot attach more than {} disks to instance!",
                MAX_DISKS_PER_INSTANCE
            )));
        }

        let saga_params = Arc::new(sagas::ParamsInstanceCreate {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            organization_name: organization_name.clone().into(),
            project_name: project_name.clone().into(),
            project_id: authz_project.id(),
            create_params: params.clone(),
        });

        let saga_outputs = self
            .execute_saga(
                Arc::clone(&sagas::SAGA_INSTANCE_CREATE_TEMPLATE),
                sagas::SAGA_INSTANCE_CREATE_NAME,
                saga_params,
            )
            .await?;
        // TODO-error more context would be useful
        let instance_id =
            saga_outputs.lookup_output::<Uuid>("instance_id").map_err(|e| {
                Error::InternalError { internal_message: e.to_string() }
            })?;
        // TODO-correctness TODO-robustness TODO-design It's not quite correct
        // to take this instance id and look it up again.  It's possible that
        // it's been modified or even deleted since the saga executed.  In that
        // case, we might return a different state of the Instance than the one
        // that the user created or even fail with a 404!  Both of those are
        // wrong behavior -- we should be returning the very instance that the
        // user created.
        //
        // How can we fix this?  Right now we have internal representations like
        // Instance and analaogous end-user-facing representations like
        // Instance.  The former is not even serializable.  The saga
        // _could_ emit the View version, but that's not great for two (related)
        // reasons: (1) other sagas might want to provision instances and get
        // back the internal representation to do other things with the
        // newly-created instance, and (2) even within a saga, it would be
        // useful to pass a single Instance representation along the saga,
        // but they probably would want the internal representation, not the
        // view.
        //
        // The saga could emit an Instance directly.  Today, Instance
        // etc. aren't supposed to even be serializable -- we wanted to be able
        // to have other datastore state there if needed.  We could have a third
        // InstanceInternalView...but that's starting to feel pedantic.  We
        // could just make Instance serializable, store that, and call it a
        // day.  Does it matter that we might have many copies of the same
        // objects in memory?
        //
        // If we make these serializable, it would be nice if we could leverage
        // the type system to ensure that we never accidentally send them out a
        // dropshot endpoint.  (On the other hand, maybe we _do_ want to do
        // that, for internal interfaces!  Can we do this on a
        // per-dropshot-server-basis?)
        //
        // TODO Even worse, post-authz, we do two lookups here instead of one.
        // Maybe sagas should be able to emit `authz::Instance`-type objects.
        let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(instance_id)
            .fetch()
            .await?;
        Ok(db_instance)
    }

    // TODO-correctness It's not totally clear what the semantics and behavior
    // should be here.  It might be nice to say that you can only do this
    // operation if the Instance is already stopped, in which case we can
    // execute this immediately by just removing it from the database, with the
    // same race we have with disk delete (i.e., if someone else is requesting
    // an instance boot, we may wind up in an inconsistent state).  On the other
    // hand, we could always allow this operation, issue the request to the SA
    // to destroy the instance (not just stop it), and proceed with deletion
    // when that finishes.  But in that case, although the HTTP DELETE request
    // completed, the object will still appear for a little while, which kind of
    // sucks.
    pub async fn project_destroy_instance(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> DeleteResult {
        // TODO-robustness We need to figure out what to do with Destroyed
        // instances?  Presumably we need to clean them up at some point, but
        // not right away so that callers can see that they've been destroyed.
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore.project_delete_instance(opctx, &authz_instance).await
    }

    pub async fn project_migrate_instance(
        self: &Arc<Self>,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        params: params::InstanceMigrate,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::Modify)
            .await?;

        // Kick off the migration saga
        let saga_params = Arc::new(sagas::ParamsInstanceMigrate {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            instance_id: authz_instance.id(),
            migrate_params: params,
        });
        self.execute_saga(
            Arc::clone(&sagas::SAGA_INSTANCE_MIGRATE_TEMPLATE),
            sagas::SAGA_INSTANCE_MIGRATE_NAME,
            saga_params,
        )
        .await?;

        // TODO correctness TODO robustness TODO design
        // Should we lookup the instance again here?
        // See comment in project_create_instance.
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    pub async fn instance_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> LookupResult<db::model::Instance> {
        let (.., db_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .fetch()
            .await?;
        Ok(db_instance)
    }

    fn check_runtime_change_allowed(
        &self,
        runtime: &nexus::InstanceRuntimeState,
        requested: &InstanceRuntimeStateRequested,
    ) -> Result<(), Error> {
        // Users are allowed to request a start or stop even if the instance is
        // already in the desired state (or moving to it), and we will issue a
        // request to the SA to make the state change in these cases in case the
        // runtime state we saw here was stale.  However, users are not allowed
        // to change the state of an instance that's migrating, failed or
        // destroyed.  But if we're already migrating, requesting a migration is
        // allowed to allow for idempotency.
        let allowed = match runtime.run_state {
            InstanceState::Creating => true,
            InstanceState::Starting => true,
            InstanceState::Running => true,
            InstanceState::Stopping => true,
            InstanceState::Stopped => true,
            InstanceState::Rebooting => true,

            InstanceState::Migrating => {
                requested.run_state == InstanceStateRequested::Migrating
            }
            InstanceState::Repairing => false,
            InstanceState::Failed => false,
            InstanceState::Destroyed => false,
        };

        if allowed {
            Ok(())
        } else {
            Err(Error::InvalidRequest {
                message: format!(
                    "instance state cannot be changed from state \"{}\"",
                    runtime.run_state
                ),
            })
        }
    }

    pub async fn sled_client(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        // TODO: We should consider injecting connection pooling here,
        // but for now, connections to sled agents are constructed
        // on an "as requested" basis.
        //
        // Franky, returning an "Arc" here without a connection pool is a little
        // silly; it's not actually used if each client connection exists as a
        // one-shot.
        let sled = self.sled_lookup(&self.opctx_alloc, id).await?;

        let log = self.log.new(o!("SledAgent" => id.clone().to_string()));
        let dur = std::time::Duration::from_secs(60);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .unwrap();
        Ok(Arc::new(SledAgentClient::new_with_client(
            &format!("http://{}", sled.address()),
            client,
            log,
        )))
    }

    /// Returns the SledAgentClient for the host where this Instance is running.
    async fn instance_sled(
        &self,
        instance: &db::model::Instance,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let sa_id = &instance.runtime().sled_uuid;
        self.sled_client(&sa_id).await
    }

    /// Reboot the specified instance.
    pub async fn instance_reboot(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> UpdateResult<db::model::Instance> {
        // To implement reboot, we issue a call to the sled agent to set a
        // runtime state of "reboot". We cannot simply stop the Instance and
        // start it again here because if we crash in the meantime, we might
        // leave it stopped.
        //
        // When an instance is rebooted, the "rebooting" flag remains set on
        // the runtime state as it transitions to "Stopping" and "Stopped".
        // This flag is cleared when the state goes to "Starting".  This way,
        // even if the whole rack powered off while this was going on, we would
        // never lose track of the fact that this Instance was supposed to be
        // running.
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Reboot,
            migration_params: None,
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Make sure the given Instance is running.
    pub async fn instance_start(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Running,
            migration_params: None,
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Make sure the given Instance is stopped.
    pub async fn instance_stop(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Stopped,
            migration_params: None,
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Idempotently place the instance in a 'Migrating' state.
    pub async fn instance_start_migrate(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        migration_id: Uuid,
        dst_propolis_id: Uuid,
    ) -> UpdateResult<db::model::Instance> {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .instance_id(instance_id)
                .fetch()
                .await
                .unwrap();
        let requested = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Migrating,
            migration_params: Some(InstanceRuntimeStateMigrateParams {
                migration_id,
                dst_propolis_id,
            }),
        };
        self.instance_set_runtime(
            opctx,
            &authz_instance,
            &db_instance,
            requested,
        )
        .await?;
        self.db_datastore.instance_refetch(opctx, &authz_instance).await
    }

    /// Modifies the runtime state of the Instance as requested.  This generally
    /// means booting or halting the Instance.
    pub(crate) async fn instance_set_runtime(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
        requested: InstanceRuntimeStateRequested,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_instance).await?;

        self.check_runtime_change_allowed(
            &db_instance.runtime().clone().into(),
            &requested,
        )?;

        // Gather disk information and turn that into DiskRequests
        let disks = self
            .db_datastore
            .instance_list_disks(
                &opctx,
                &authz_instance,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_DISKS_PER_INSTANCE)
                        .unwrap(),
                },
            )
            .await?;

        let mut disk_reqs = vec![];
        for (i, disk) in disks.iter().enumerate() {
            let volume = self.db_datastore.volume_get(disk.volume_id).await?;
            let gen: i64 = (&disk.runtime_state.gen.0).into();
            disk_reqs.push(sled_agent_client::types::DiskRequest {
                name: disk.name().to_string(),
                slot: sled_agent_client::types::Slot(i as u8),
                read_only: false,
                device: "nvme".to_string(),
                gen: gen as u64,
                volume_construction_request: serde_json::from_str(
                    &volume.data(),
                )?,
            });
        }

        let nics: Vec<external::NetworkInterface> = self
            .db_datastore
            .instance_list_network_interfaces(
                &opctx,
                &authz_instance,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_NICS_PER_INSTANCE)
                        .unwrap(),
                },
            )
            .await?
            .iter()
            .map(|x| x.clone().into())
            .collect();

        // Ask the sled agent to begin the state change.  Then update the
        // database to reflect the new intermediate state.  If this update is
        // not the newest one, that's fine.  That might just mean the sled agent
        // beat us to it.

        let instance_hardware = sled_agent_client::types::InstanceHardware {
            runtime: sled_agent_client::types::InstanceRuntimeState::from(
                db_instance.runtime().clone(),
            ),
            nics: nics.iter().map(|nic| nic.clone().into()).collect(),
            disks: disk_reqs,
            cloud_init_bytes: Some(base64::encode(
                db_instance.generate_cidata()?,
            )),
        };

        let sa = self.instance_sled(&db_instance).await?;

        let new_runtime = sa
            .instance_put(
                &db_instance.id(),
                &sled_agent_client::types::InstanceEnsureBody {
                    initial: instance_hardware,
                    target: requested,
                    migrate: None,
                },
            )
            .await
            .map_err(Error::from)?;

        let new_runtime: nexus::InstanceRuntimeState =
            new_runtime.into_inner().into();

        self.db_datastore
            .instance_update_runtime(&db_instance.id(), &new_runtime.into())
            .await
            .map(|_| ())
    }

    /// Lists disks attached to the instance.
    pub async fn instance_list_disks(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Disk> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .instance_list_disks(opctx, &authz_instance, pagparams)
            .await
    }

    /// Attach a disk to an instance.
    pub async fn instance_attach_disk(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        disk_name: &Name,
    ) -> UpdateResult<db::model::Disk> {
        let (.., authz_project, authz_disk, db_disk) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(disk_name)
                .fetch()
                .await?;
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .instance_name(instance_name)
                .fetch()
                .await?;
        let instance_id = &authz_instance.id();

        // Enforce attached disks limit
        let attached_disks = self
            .instance_list_disks(
                opctx,
                organization_name,
                project_name,
                instance_name,
                &DataPageParams {
                    marker: None,
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: std::num::NonZeroU32::new(MAX_DISKS_PER_INSTANCE)
                        .unwrap(),
                },
            )
            .await?;

        if attached_disks.len() == MAX_DISKS_PER_INSTANCE as usize {
            return Err(Error::invalid_request(&format!(
                "cannot attach more than {} disks to instance!",
                MAX_DISKS_PER_INSTANCE
            )));
        }

        fn disk_attachment_error(
            disk: &db::model::Disk,
        ) -> CreateResult<db::model::Disk> {
            let disk_status = match disk.runtime().state().into() {
                DiskState::Destroyed => "disk is destroyed",
                DiskState::Faulted => "disk is faulted",
                DiskState::Creating => "disk is detached",
                DiskState::Detached => "disk is detached",

                // It would be nice to provide a more specific message here, but
                // the appropriate identifier to provide the user would be the
                // other instance's name.  Getting that would require another
                // database hit, which doesn't seem worth it for this.
                DiskState::Attaching(_) => {
                    "disk is attached to another instance"
                }
                DiskState::Attached(_) => {
                    "disk is attached to another instance"
                }
                DiskState::Detaching(_) => {
                    "disk is attached to another instance"
                }
            };
            let message = format!(
                "cannot attach disk \"{}\": {}",
                disk.name().as_str(),
                disk_status
            );
            Err(Error::InvalidRequest { message })
        }

        match &db_disk.state().into() {
            // If we're already attaching or attached to the requested instance,
            // there's nothing else to do.
            // TODO-security should it be an error if you're not authorized to
            // do this and we did not actually have to do anything?
            DiskState::Attached(id) if id == instance_id => return Ok(db_disk),

            // If the disk is currently attaching or attached to another
            // instance, fail this request.  Users must explicitly detach first
            // if that's what they want.  If it's detaching, they have to wait
            // for it to become detached.
            // TODO-debug: the error message here could be better.  We'd have to
            // look up the other instance by id (and gracefully handle it not
            // existing).
            DiskState::Attached(id) => {
                assert_ne!(id, instance_id);
                return disk_attachment_error(&db_disk);
            }
            DiskState::Detaching(_) => {
                return disk_attachment_error(&db_disk);
            }
            DiskState::Attaching(id) if id != instance_id => {
                return disk_attachment_error(&db_disk);
            }
            DiskState::Destroyed => {
                return disk_attachment_error(&db_disk);
            }
            DiskState::Faulted => {
                return disk_attachment_error(&db_disk);
            }

            DiskState::Creating => (),
            DiskState::Detached => (),
            DiskState::Attaching(id) => {
                assert_eq!(id, instance_id);
            }
        }

        match &db_instance.runtime_state.state.state() {
            // If there's a propolis zone for this instance, ask the Sled Agent
            // to hot-plug the disk.
            //
            // TODO this will probably involve volume construction requests as
            // well!
            InstanceState::Running | InstanceState::Starting => {
                self.disk_set_runtime(
                    opctx,
                    &authz_disk,
                    &db_disk,
                    self.instance_sled(&db_instance).await?,
                    sled_agent_client::types::DiskStateRequested::Attached(
                        *instance_id,
                    ),
                )
                .await?;
            }

            _ => {
                // If there is not a propolis zone, then disk attach only occurs
                // in the DB.
                let new_runtime = db_disk.runtime().attach(*instance_id);

                self.db_datastore
                    .disk_update_runtime(opctx, &authz_disk, &new_runtime)
                    .await?;
            }
        }

        self.db_datastore.disk_refetch(opctx, &authz_disk).await
    }

    /// Detach a disk from an instance.
    pub async fn instance_detach_disk(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        disk_name: &Name,
    ) -> UpdateResult<db::model::Disk> {
        let (.., authz_project, authz_disk, db_disk) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .disk_name(disk_name)
                .fetch()
                .await?;
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .instance_name(instance_name)
                .fetch()
                .await?;
        let instance_id = &authz_instance.id();

        match &db_disk.state().into() {
            // This operation is a noop if the disk is not attached or already
            // detaching from the same instance.
            // TODO-security should it be an error if you're not authorized to
            // do this and we did not actually have to do anything?
            DiskState::Creating => return Ok(db_disk),
            DiskState::Detached => return Ok(db_disk),
            DiskState::Destroyed => return Ok(db_disk),
            DiskState::Faulted => return Ok(db_disk),
            DiskState::Detaching(id) if id == instance_id => {
                return Ok(db_disk)
            }

            // This operation is not allowed if the disk is attached to some
            // other instance.
            DiskState::Attaching(id) if id != instance_id => {
                return Err(Error::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }
            DiskState::Attached(id) if id != instance_id => {
                return Err(Error::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }
            DiskState::Detaching(_) => {
                return Err(Error::InvalidRequest {
                    message: String::from("disk is attached elsewhere"),
                });
            }

            // These are the cases where we have to do something.
            DiskState::Attaching(_) => (),
            DiskState::Attached(_) => (),
        }

        // If there's a propolis zone for this instance, ask the Sled
        // Agent to hot-remove the disk.
        match &db_instance.runtime_state.state.state() {
            InstanceState::Running | InstanceState::Starting => {
                self.disk_set_runtime(
                    opctx,
                    &authz_disk,
                    &db_disk,
                    self.instance_sled(&db_instance).await?,
                    sled_agent_client::types::DiskStateRequested::Detached,
                )
                .await?;
            }

            _ => {
                // If there is not a propolis zone, then disk detach only occurs
                // in the DB.
                let new_runtime = db_disk.runtime().detach();

                self.db_datastore
                    .disk_update_runtime(opctx, &authz_disk, &new_runtime)
                    .await?;
            }
        }

        self.db_datastore.disk_refetch(opctx, &authz_disk).await
    }

    /// Modifies the runtime state of the Disk as requested.  This generally
    /// means attaching or detaching the disk.
    async fn disk_set_runtime(
        &self,
        opctx: &OpContext,
        authz_disk: &authz::Disk,
        db_disk: &db::model::Disk,
        sa: Arc<SledAgentClient>,
        requested: sled_agent_client::types::DiskStateRequested,
    ) -> Result<(), Error> {
        let runtime: DiskRuntimeState = db_disk.runtime().into();

        opctx.authorize(authz::Action::Modify, authz_disk).await?;

        // Ask the Sled Agent to begin the state change.  Then update the
        // database to reflect the new intermediate state.
        let new_runtime = sa
            .disk_put(
                &authz_disk.id(),
                &sled_agent_client::types::DiskEnsureBody {
                    initial_runtime:
                        sled_agent_client::types::DiskRuntimeState::from(
                            runtime,
                        ),
                    target: requested,
                },
            )
            .await
            .map_err(Error::from)?;

        let new_runtime: DiskRuntimeState = new_runtime.into_inner().into();

        self.db_datastore
            .disk_update_runtime(opctx, authz_disk, &new_runtime.into())
            .await
            .map(|_| ())
    }

    ///  Lists network interfaces attached to the instance.
    pub async fn instance_list_network_interfaces(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::NetworkInterface> {
        let (.., authz_instance) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .instance_list_network_interfaces(opctx, &authz_instance, pagparams)
            .await
    }

    /// Create a network interface attached to the provided instance.
    // TODO-performance: Add a version of this that accepts the instance ID
    // directly. This will avoid all the internal database lookups in the event
    // that we create many NICs for the same instance, such as in a saga.
    pub async fn instance_create_network_interface(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        params: &params::NetworkInterfaceCreate,
    ) -> CreateResult<db::model::NetworkInterface> {
        let (.., authz_project, authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch()
                .await?;

        // TODO-completeness: We'd like to relax this once hot-plug is
        // supported.
        //
        // TODO-correctness: There's a TOCTOU race here. Someone might start the
        // instance between this check and when we actually create the NIC
        // record. One solution is to place the state verification in the query
        // to create the NIC. Unfortunately, that query is already very
        // complicated.
        let stopped =
            db::model::InstanceState::new(external::InstanceState::Stopped);
        if db_instance.runtime_state.state != stopped {
            return Err(external::Error::invalid_request(
                "Instance must be stopped to attach a new network interface",
            ));
        }

        // NOTE: We need to lookup the VPC and VPC Subnet, since we need both
        // IDs for creating the network interface.
        let vpc_name = db::model::Name(params.vpc_name.clone());
        let subnet_name = db::model::Name(params.subnet_name.clone());
        let (.., authz_vpc, authz_subnet, db_subnet) =
            LookupPath::new(opctx, &self.db_datastore)
                .project_id(authz_project.id())
                .vpc_name(&vpc_name)
                .vpc_subnet_name(&subnet_name)
                .fetch()
                .await?;
        let mac = db::model::MacAddr::new()?;
        let interface_id = Uuid::new_v4();
        let interface = db::model::IncompleteNetworkInterface::new(
            interface_id,
            authz_instance.id(),
            authz_vpc.id(),
            db_subnet,
            mac,
            params.identity.clone(),
            params.ip,
        )?;
        let interface = self
            .db_datastore
            .instance_create_network_interface(
                opctx,
                &authz_subnet,
                &authz_instance,
                interface,
            )
            .await
            .map_err(NetworkInterfaceError::into_external)?;
        Ok(interface)
    }

    /// Delete a network interface from the provided instance.
    pub async fn instance_delete_network_interface(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        interface_name: &Name,
    ) -> DeleteResult {
        let (.., authz_instance, db_instance) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .instance_name(instance_name)
                .fetch_for(authz::Action::Modify)
                .await?;
        let (.., authz_interface) = LookupPath::new(opctx, &self.db_datastore)
            .instance_id(authz_instance.id())
            .network_interface_name(interface_name)
            .lookup_for(authz::Action::Delete)
            .await?;

        // TODO-completeness: We'd like to relax this once hot-plug is supported
        let stopped =
            db::model::InstanceState::new(external::InstanceState::Stopped);
        if db_instance.runtime_state.state != stopped {
            return Err(external::Error::invalid_request(
                "Instance must be stopped to detach a network interface",
            ));
        }
        self.db_datastore
            .instance_delete_network_interface(opctx, &authz_interface)
            .await
    }

    /// Fetch a network interface attached to the given instance.
    pub async fn network_interface_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        interface_name: &Name,
    ) -> LookupResult<db::model::NetworkInterface> {
        let (.., db_interface) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .instance_name(instance_name)
            .network_interface_name(interface_name)
            .fetch()
            .await?;
        Ok(db_interface)
    }

    pub async fn project_list_vpcs(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .project_list_vpcs(&opctx, &authz_project, pagparams)
            .await
    }

    pub async fn project_create_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        params: &params::VpcCreate,
    ) -> CreateResult<db::model::Vpc> {
        let (.., authz_project) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let vpc_id = Uuid::new_v4();
        let system_router_id = Uuid::new_v4();
        let default_route_id = Uuid::new_v4();
        let default_subnet_id = Uuid::new_v4();

        // TODO: This is both fake and utter nonsense. It should be eventually
        // replaced with the proper behavior for creating the default route
        // which may not even happen here. Creating the vpc, its system router,
        // and that routers default route should all be a part of the same
        // transaction.
        let vpc = db::model::Vpc::new(
            vpc_id,
            authz_project.id(),
            system_router_id,
            params.clone(),
        )?;
        let (authz_vpc, db_vpc) = self
            .db_datastore
            .project_create_vpc(opctx, &authz_project, vpc)
            .await?;

        // TODO: Ultimately when the VPC is created a system router w/ an
        // appropriate setup should also be created.  Given that the underlying
        // systems aren't wired up yet this is a naive implementation to
        // populate the database with a starting router. Eventually this code
        // should be replaced with a saga that'll handle creating the VPC and
        // its underlying system
        let router = db::model::VpcRouter::new(
            system_router_id,
            vpc_id,
            VpcRouterKind::System,
            params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: "system".parse().unwrap(),
                    description: "Routes are automatically added to this \
                        router as vpc subnets are created"
                        .into(),
                },
            },
        );
        let (authz_router, _) = self
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        let route = db::model::RouterRoute::new(
            default_route_id,
            system_router_id,
            RouterRouteKind::Default,
            RouterRouteCreateParams {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description: "The default route of a vpc".to_string(),
                },
                target: RouteTarget::InternetGateway(
                    "outbound".parse().unwrap(),
                ),
                destination: RouteDestination::Vpc(
                    params.identity.name.clone(),
                ),
            },
        );

        self.db_datastore
            .router_create_route(opctx, &authz_router, route)
            .await?;

        // Allocate the first /64 sub-range from the requested or created
        // prefix.
        let ipv6_block = external::Ipv6Net(
            ipnetwork::Ipv6Network::new(db_vpc.ipv6_prefix.network(), 64)
                .map_err(|_| {
                    external::Error::internal_error(
                        "Failed to allocate default IPv6 subnet",
                    )
                })?,
        );

        // TODO: batch this up with everything above
        let subnet = db::model::VpcSubnet::new(
            default_subnet_id,
            vpc_id,
            IdentityMetadataCreateParams {
                name: "default".parse().unwrap(),
                description: format!(
                    "The default subnet for {}",
                    params.identity.name
                ),
            },
            *defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK,
            ipv6_block,
        );

        // Create the subnet record in the database. Overlapping IP ranges
        // should be translated into an internal error. That implies that
        // there's already an existing VPC Subnet, but we're explicitly creating
        // the _first_ VPC in the project. Something is wrong, and likely a bug
        // in our code.
        self.db_datastore
            .vpc_create_subnet(opctx, &authz_vpc, subnet)
            .await
            .map_err(|err| match err {
                SubnetError::OverlappingIpRange(ip) => {
                    let ipv4_block = &defaults::DEFAULT_VPC_SUBNET_IPV4_BLOCK;
                    error!(
                        self.log,
                        concat!(
                            "failed to create default VPC Subnet, IP address ",
                            "range '{}' overlaps with existing",
                        ),
                        ip;
                        "vpc_id" => ?vpc_id,
                        "subnet_id" => ?default_subnet_id,
                        "ipv4_block" => ?**ipv4_block,
                        "ipv6_block" => ?ipv6_block,
                    );
                    external::Error::internal_error(
                        "Failed to create default VPC Subnet, \
                            found overlapping IP address ranges",
                    )
                }
                SubnetError::External(e) => e,
            })?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            defaults::DEFAULT_FIREWALL_RULES.clone(),
        );
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await?;
        Ok(db_vpc)
    }

    pub async fn vpc_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> LookupResult<db::model::Vpc> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    pub async fn project_update_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcUpdate,
    ) -> UpdateResult<db::model::Vpc> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .project_update_vpc(opctx, &authz_vpc, params.clone().into())
            .await
    }

    pub async fn project_delete_vpc(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> DeleteResult {
        let (.., authz_vpc, db_vpc) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .fetch()
                .await?;

        let authz_vpc_router = authz::VpcRouter::new(
            authz_vpc.clone(),
            db_vpc.system_router_id,
            LookupType::ById(db_vpc.system_router_id),
        );

        // TODO: This should eventually use a saga to call the
        // networking subsystem to have it clean up the networking resources
        self.db_datastore.vpc_delete_router(&opctx, &authz_vpc_router).await?;
        self.db_datastore.project_delete_vpc(opctx, &authz_vpc).await?;

        // Delete all firewall rules after deleting the VPC, to ensure no
        // firewall rules get added between rules deletion and VPC deletion.
        self.db_datastore
            .vpc_delete_all_firewall_rules(&opctx, &authz_vpc)
            .await
    }

    pub async fn vpc_list_firewall_rules(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> ListResultVec<db::model::VpcFirewallRule> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Read)
            .await?;
        let rules = self
            .db_datastore
            .vpc_list_firewall_rules(&opctx, &authz_vpc)
            .await?;
        Ok(rules)
    }

    pub async fn vpc_update_firewall_rules(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &VpcFirewallRuleUpdateParams,
    ) -> UpdateResult<Vec<db::model::VpcFirewallRule>> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            authz_vpc.id(),
            params.clone(),
        );
        self.db_datastore
            .vpc_update_firewall_rules(opctx, &authz_vpc, rules)
            .await
    }

    pub async fn vpc_list_subnets(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcSubnet> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore.vpc_list_subnets(opctx, &authz_vpc, pagparams).await
    }

    pub async fn vpc_subnet_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
    ) -> LookupResult<db::model::VpcSubnet> {
        let (.., db_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_subnet_name(subnet_name)
            .fetch()
            .await?;
        Ok(db_vpc)
    }

    // TODO: When a subnet is created it should add a route entry into the VPC's
    // system router
    pub async fn vpc_create_subnet(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcSubnetCreate,
    ) -> CreateResult<db::model::VpcSubnet> {
        let (.., authz_vpc, db_vpc) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .fetch()
                .await?;

        // Validate IPv4 range
        if !params.ipv4_block.network().is_private() {
            return Err(external::Error::invalid_request(
                "VPC Subnet IPv4 address ranges must be from a private range",
            ));
        }
        if params.ipv4_block.prefix() < defaults::MIN_VPC_IPV4_SUBNET_PREFIX
            || params.ipv4_block.prefix() > defaults::MAX_VPC_IPV4_SUBNET_PREFIX
        {
            return Err(external::Error::invalid_request(&format!(
                concat!(
                    "VPC Subnet IPv4 address ranges must have prefix ",
                    "length between {} and {}, inclusive"
                ),
                defaults::MIN_VPC_IPV4_SUBNET_PREFIX,
                defaults::MAX_VPC_IPV4_SUBNET_PREFIX
            )));
        }

        // Allocate an ID and insert the record.
        //
        // If the client provided an IPv6 range, we try to insert that or fail
        // with a conflict error.
        //
        // If they did _not_, we randomly generate a subnet valid for the VPC's
        // prefix, and the insert that. There's a small retry loop if we get
        // unlucky and conflict with an existing IPv6 range. In the case we
        // cannot find a subnet within a small number of retries, we fail the
        // request with a 503.
        //
        // TODO-robustness: We'd really prefer to allocate deterministically.
        // See <https://github.com/oxidecomputer/omicron/issues/685> for
        // details.
        let subnet_id = Uuid::new_v4();
        match params.ipv6_block {
            None => {
                const NUM_RETRIES: usize = 2;
                let mut retry = 0;
                let result = loop {
                    let ipv6_block = db_vpc
                        .ipv6_prefix
                        .random_subnet(
                            external::Ipv6Net::VPC_SUBNET_IPV6_PREFIX_LENGTH,
                        )
                        .map(|block| block.0)
                        .ok_or_else(|| {
                            external::Error::internal_error(
                                "Failed to create random IPv6 subnet",
                            )
                        })?;
                    let subnet = db::model::VpcSubnet::new(
                        subnet_id,
                        authz_vpc.id(),
                        params.identity.clone(),
                        params.ipv4_block,
                        ipv6_block,
                    );
                    let result = self
                        .db_datastore
                        .vpc_create_subnet(opctx, &authz_vpc, subnet)
                        .await;
                    match result {
                        // Allow NUM_RETRIES retries, after the first attempt.
                        //
                        // Note that we only catch IPv6 overlaps. The client
                        // always specifies the IPv4 range, so we fail the
                        // request if that overlaps with an existing range.
                        Err(SubnetError::OverlappingIpRange(ip))
                            if retry <= NUM_RETRIES && ip.is_ipv6() =>
                        {
                            debug!(
                                self.log,
                                "autogenerated random IPv6 range overlap";
                                "subnet_id" => ?subnet_id,
                                "ipv6_block" => %ipv6_block.0
                            );
                            retry += 1;
                            continue;
                        }
                        other => break other,
                    }
                };
                match result {
                    Err(SubnetError::OverlappingIpRange(ip))
                        if ip.is_ipv6() =>
                    {
                        // TODO-monitoring TODO-debugging
                        //
                        // We should maintain a counter for this occurrence, and
                        // export that via `oximeter`, so that we can see these
                        // failures through the timeseries database. The main
                        // goal here is for us to notice that this is happening
                        // before it becomes a major issue for customers.
                        let vpc_id = authz_vpc.id();
                        error!(
                            self.log,
                            "failed to generate unique random IPv6 address \
                            range in {} retries",
                            NUM_RETRIES;
                            "vpc_id" => ?vpc_id,
                            "subnet_id" => ?subnet_id,
                        );
                        Err(external::Error::internal_error(
                            "Unable to allocate unique IPv6 address range \
                            for VPC Subnet",
                        ))
                    }
                    Err(SubnetError::OverlappingIpRange(_)) => {
                        // Overlapping IPv4 ranges, which is always a client error.
                        Err(result.unwrap_err().into_external())
                    }
                    Err(SubnetError::External(e)) => Err(e),
                    Ok(subnet) => Ok(subnet),
                }
            }
            Some(ipv6_block) => {
                if !ipv6_block.is_vpc_subnet(&db_vpc.ipv6_prefix) {
                    return Err(external::Error::invalid_request(&format!(
                        concat!(
                        "VPC Subnet IPv6 address range '{}' is not valid for ",
                        "VPC with IPv6 prefix '{}'",
                    ),
                        ipv6_block, db_vpc.ipv6_prefix.0 .0,
                    )));
                }
                let subnet = db::model::VpcSubnet::new(
                    subnet_id,
                    db_vpc.id(),
                    params.identity.clone(),
                    params.ipv4_block,
                    ipv6_block,
                );
                self.db_datastore
                    .vpc_create_subnet(opctx, &authz_vpc, subnet)
                    .await
                    .map_err(SubnetError::into_external)
            }
        }
    }

    // TODO: When a subnet is deleted it should remove its entry from the VPC's
    // system router.
    pub async fn vpc_delete_subnet(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
    ) -> DeleteResult {
        let (.., authz_subnet) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_subnet_name(subnet_name)
            .lookup_for(authz::Action::Delete)
            .await?;
        self.db_datastore.vpc_delete_subnet(opctx, &authz_subnet).await
    }

    pub async fn vpc_update_subnet(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
        params: &params::VpcSubnetUpdate,
    ) -> UpdateResult<VpcSubnet> {
        let (.., authz_subnet) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_subnet_name(subnet_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .vpc_update_subnet(&opctx, &authz_subnet, params.clone().into())
            .await
    }

    pub async fn subnet_list_network_interfaces(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::NetworkInterface> {
        let (.., authz_subnet) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_subnet_name(subnet_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .subnet_list_network_interfaces(opctx, &authz_subnet, pagparams)
            .await
    }

    pub async fn vpc_list_routers(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcRouter> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        let routers = self
            .db_datastore
            .vpc_list_routers(opctx, &authz_vpc, pagparams)
            .await?;
        Ok(routers)
    }

    pub async fn vpc_router_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> LookupResult<db::model::VpcRouter> {
        let (.., db_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .fetch()
            .await?;
        Ok(db_router)
    }

    pub async fn vpc_create_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        kind: &VpcRouterKind,
        params: &params::VpcRouterCreate,
    ) -> CreateResult<db::model::VpcRouter> {
        let (.., authz_vpc) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let id = Uuid::new_v4();
        let router = db::model::VpcRouter::new(
            id,
            authz_vpc.id(),
            *kind,
            params.clone(),
        );
        let (_, router) = self
            .db_datastore
            .vpc_create_router(&opctx, &authz_vpc, router)
            .await?;
        Ok(router)
    }

    // TODO: When a router is deleted all its routes should be deleted
    // TODO: When a router is deleted it should be unassociated w/ any subnets it may be associated with
    //       or trigger an error
    pub async fn vpc_delete_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> DeleteResult {
        let (.., authz_router, db_router) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .vpc_router_name(router_name)
                .fetch()
                .await?;
        // TODO-performance shouldn't this check be part of the "update"
        // database query?  This shouldn't affect correctness, assuming that a
        // router kind cannot be changed, but it might be able to save us a
        // database round-trip.
        if db_router.kind == VpcRouterKind::System {
            return Err(Error::MethodNotAllowed {
                internal_message: "Cannot delete system router".to_string(),
            });
        }
        self.db_datastore.vpc_delete_router(opctx, &authz_router).await
    }

    pub async fn vpc_update_router(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        params: &params::VpcRouterUpdate,
    ) -> UpdateResult<VpcRouter> {
        let (.., authz_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .vpc_update_router(opctx, &authz_router, params.clone().into())
            .await
    }

    /// VPC Router routes

    pub async fn router_list_routes(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::RouterRoute> {
        let (.., authz_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .lookup_for(authz::Action::ListChildren)
            .await?;
        self.db_datastore
            .router_list_routes(opctx, &authz_router, pagparams)
            .await
    }

    pub async fn route_fetch(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
    ) -> LookupResult<db::model::RouterRoute> {
        let (.., db_route) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .router_route_name(route_name)
            .fetch()
            .await?;
        Ok(db_route)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn router_create_route(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        kind: &RouterRouteKind,
        params: &RouterRouteCreateParams,
    ) -> CreateResult<db::model::RouterRoute> {
        let (.., authz_router) = LookupPath::new(opctx, &self.db_datastore)
            .organization_name(organization_name)
            .project_name(project_name)
            .vpc_name(vpc_name)
            .vpc_router_name(router_name)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        let id = Uuid::new_v4();
        let route = db::model::RouterRoute::new(
            id,
            authz_router.id(),
            *kind,
            params.clone(),
        );
        let route = self
            .db_datastore
            .router_create_route(&opctx, &authz_router, route)
            .await?;
        Ok(route)
    }

    pub async fn router_delete_route(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
    ) -> DeleteResult {
        let (.., authz_route, db_route) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .vpc_router_name(router_name)
                .router_route_name(route_name)
                .fetch()
                .await?;

        // Only custom routes can be deleted
        // TODO Shouldn't this constraint be checked by the database query?
        if db_route.kind.0 != RouterRouteKind::Custom {
            return Err(Error::MethodNotAllowed {
                internal_message: "DELETE not allowed on system routes"
                    .to_string(),
            });
        }
        self.db_datastore.router_delete_route(opctx, &authz_route).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn router_update_route(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
        params: &RouterRouteUpdateParams,
    ) -> UpdateResult<RouterRoute> {
        let (.., authz_route, db_route) =
            LookupPath::new(opctx, &self.db_datastore)
                .organization_name(organization_name)
                .project_name(project_name)
                .vpc_name(vpc_name)
                .vpc_router_name(router_name)
                .router_route_name(route_name)
                .fetch()
                .await?;
        // TODO: Write a test for this once there's a way to test it (i.e.
        // subnets automatically register to the system router table)
        match db_route.kind.0 {
            RouterRouteKind::Custom | RouterRouteKind::Default => (),
            _ => {
                return Err(Error::MethodNotAllowed {
                    internal_message: format!(
                        "routes of type {} from the system table of VPC {:?} \
                        are not modifiable",
                        db_route.kind.0, vpc_name
                    ),
                })
            }
        }
        Ok(self
            .db_datastore
            .router_update_route(&opctx, &authz_route, params.clone().into())
            .await?)
    }

    // Racks.  We simulate just one for now.

    fn as_rack(&self) -> db::model::Rack {
        db::model::Rack {
            identity: self.api_rack_identity.clone(),
            tuf_base_url: None,
        }
    }

    pub async fn racks_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<db::model::Rack> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        if let Some(marker) = pagparams.marker {
            if *marker >= self.rack_id {
                return Ok(futures::stream::empty().boxed());
            }
        }

        Ok(futures::stream::once(ready(Ok(self.as_rack()))).boxed())
    }

    pub async fn rack_lookup(
        &self,
        opctx: &OpContext,
        rack_id: &Uuid,
    ) -> LookupResult<db::model::Rack> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            *rack_id,
            LookupType::ById(*rack_id),
        );
        opctx.authorize(authz::Action::Read, &authz_rack).await?;

        if *rack_id == self.rack_id {
            Ok(self.as_rack())
        } else {
            Err(Error::not_found_by_id(ResourceType::Rack, rack_id))
        }
    }

    // Sleds

    pub async fn sleds_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Sled> {
        self.db_datastore.sled_list(&opctx, pagparams).await
    }

    pub async fn sled_lookup(
        &self,
        opctx: &OpContext,
        sled_id: &Uuid,
    ) -> LookupResult<db::model::Sled> {
        let (.., db_sled) = LookupPath::new(opctx, &self.db_datastore)
            .sled_id(*sled_id)
            .fetch()
            .await?;
        Ok(db_sled)
    }

    // Sagas

    pub async fn sagas_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<external::Saga> {
        // The endpoint we're serving only supports `ScanById`, which only
        // supports an ascending scan.
        bail_unless!(
            pagparams.direction == dropshot::PaginationOrder::Ascending
        );
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let marker = pagparams.marker.map(|s| SagaId::from(*s));
        let saga_list = self
            .sec_client
            .saga_list(marker, pagparams.limit)
            .await
            .into_iter()
            .map(external::Saga::from)
            .map(Ok);
        Ok(futures::stream::iter(saga_list).boxed())
    }

    pub async fn saga_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> LookupResult<external::Saga> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.sec_client
            .saga_get(steno::SagaId::from(id))
            .await
            .map(external::Saga::from)
            .map(Ok)
            .map_err(|_: ()| {
                Error::not_found_by_id(ResourceType::SagaDbg, &id)
            })?
    }

    // Built-in users

    pub async fn users_builtin_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::UserBuiltin> {
        self.db_datastore.users_builtin_list_by_name(opctx, pagparams).await
    }

    pub async fn user_builtin_fetch(
        &self,
        opctx: &OpContext,
        name: &Name,
    ) -> LookupResult<db::model::UserBuiltin> {
        let (.., db_user_builtin) = LookupPath::new(opctx, &self.db_datastore)
            .user_builtin_name(name)
            .fetch()
            .await?;
        Ok(db_user_builtin)
    }

    // Built-in roles

    pub async fn roles_builtin_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (String, String)>,
    ) -> ListResultVec<db::model::RoleBuiltin> {
        self.db_datastore.roles_builtin_list_by_name(opctx, pagparams).await
    }

    pub async fn role_builtin_fetch(
        &self,
        opctx: &OpContext,
        name: &str,
    ) -> LookupResult<db::model::RoleBuiltin> {
        let (.., db_role_builtin) = LookupPath::new(opctx, &self.db_datastore)
            .role_builtin_name(name)
            .fetch()
            .await?;
        Ok(db_role_builtin)
    }

    // Internal control plane interfaces.

    /// Invoked by a sled agent to publish an updated runtime state for an
    /// Instance.
    pub async fn notify_instance_updated(
        &self,
        id: &Uuid,
        new_runtime_state: &nexus::InstanceRuntimeState,
    ) -> Result<(), Error> {
        let log = &self.log;

        let result = self
            .db_datastore
            .instance_update_runtime(id, &(new_runtime_state.clone().into()))
            .await;

        match result {
            Ok(true) => {
                info!(log, "instance updated by sled agent";
                    "instance_id" => %id,
                    "propolis_id" => %new_runtime_state.propolis_uuid,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            Ok(false) => {
                info!(log, "instance update from sled agent ignored (old)";
                    "instance_id" => %id,
                    "propolis_id" => %new_runtime_state.propolis_uuid,
                    "requested_state" => %new_runtime_state.run_state);
                Ok(())
            }

            // If the instance doesn't exist, swallow the error -- there's
            // nothing to do here.
            // TODO-robustness This could only be possible if we've removed an
            // Instance from the datastore altogether.  When would we do that?
            // We don't want to do it as soon as something's destroyed, I think,
            // and in that case, we'd need some async task for cleaning these
            // up.
            Err(Error::ObjectNotFound { .. }) => {
                warn!(log, "non-existent instance updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            // If the datastore is unavailable, propagate that to the caller.
            // TODO-robustness Really this should be any _transient_ error.  How
            // can we distinguish?  Maybe datastore should emit something
            // different from Error with an Into<Error>.
            Err(error) => {
                warn!(log, "failed to update instance from sled agent";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state,
                    "error" => ?error);
                Err(error)
            }
        }
    }

    pub async fn notify_disk_updated(
        &self,
        opctx: &OpContext,
        id: Uuid,
        new_state: &DiskRuntimeState,
    ) -> Result<(), Error> {
        let log = &self.log;
        let (.., authz_disk) = LookupPath::new(&opctx, &self.db_datastore)
            .disk_id(id)
            .lookup_for(authz::Action::Modify)
            .await?;

        let result = self
            .db_datastore
            .disk_update_runtime(opctx, &authz_disk, &new_state.clone().into())
            .await;

        // TODO-cleanup commonize with notify_instance_updated()
        match result {
            Ok(true) => {
                info!(log, "disk updated by sled agent";
                    "disk_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            Ok(false) => {
                info!(log, "disk update from sled agent ignored (old)";
                    "disk_id" => %id);
                Ok(())
            }

            // If the disk doesn't exist, swallow the error -- there's
            // nothing to do here.
            // TODO-robustness This could only be possible if we've removed a
            // disk from the datastore altogether.  When would we do that?
            // We don't want to do it as soon as something's destroyed, I think,
            // and in that case, we'd need some async task for cleaning these
            // up.
            Err(Error::ObjectNotFound { .. }) => {
                warn!(log, "non-existent disk updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            // If the datastore is unavailable, propagate that to the caller.
            Err(error) => {
                warn!(log, "failed to update disk from sled agent";
                    "disk_id" => %id,
                    "new_state" => ?new_state,
                    "error" => ?error);
                Err(error)
            }
        }
    }

    // Timeseries

    /// List existing timeseries schema.
    pub async fn timeseries_schema_list(
        &self,
        opctx: &OpContext,
        pag_params: &TimeseriesSchemaPaginationParams,
        limit: NonZeroU32,
    ) -> Result<dropshot::ResultsPage<TimeseriesSchema>, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.timeseries_client
            .timeseries_schema_list(&pag_params.page, limit)
            .await
            .map_err(|e| match e {
                oximeter_db::Error::DatabaseUnavailable(_) => {
                    Error::ServiceUnavailable {
                        internal_message: e.to_string(),
                    }
                }
                _ => Error::InternalError { internal_message: e.to_string() },
            })
    }

    /// Assign a newly-registered metric producer to an oximeter collector server.
    pub async fn assign_producer(
        &self,
        producer_info: nexus::ProducerEndpoint,
    ) -> Result<(), Error> {
        let (collector, id) = self.next_collector().await?;
        let db_info = db::model::ProducerEndpoint::new(&producer_info, id);
        self.db_datastore.producer_endpoint_create(&db_info).await?;
        collector
            .producers_post(&oximeter_client::types::ProducerEndpoint::from(
                &producer_info,
            ))
            .await
            .map_err(Error::from)?;
        info!(
            self.log,
            "assigned collector to new producer";
            "producer_id" => ?producer_info.id,
            "collector_id" => ?id,
        );
        Ok(())
    }

    /// Return an oximeter collector to assign a newly-registered producer
    async fn next_collector(&self) -> Result<(OximeterClient, Uuid), Error> {
        // TODO-robustness Replace with a real load-balancing strategy.
        let page_params = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(1).unwrap(),
        };
        let oxs = self.db_datastore.oximeter_list(&page_params).await?;
        let info = oxs.first().ok_or_else(|| Error::ServiceUnavailable {
            internal_message: String::from("no oximeter collectors available"),
        })?;
        let address =
            SocketAddr::from((info.ip.ip(), info.port.try_into().unwrap()));
        let id = info.id;
        Ok((self.build_oximeter_client(&id, address), id))
    }

    pub async fn session_fetch(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> LookupResult<authn::ConsoleSessionWithSiloId> {
        let (.., db_console_session) =
            LookupPath::new(opctx, &self.db_datastore)
                .console_session_token(&token)
                .fetch()
                .await?;

        let (.., db_silo_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(db_console_session.silo_user_id)
            .fetch()
            .await?;

        Ok(authn::ConsoleSessionWithSiloId {
            console_session: db_console_session,
            silo_id: db_silo_user.silo_id,
        })
    }

    pub async fn session_create(
        &self,
        opctx: &OpContext,
        user_id: Uuid,
    ) -> CreateResult<db::model::ConsoleSession> {
        if !self.login_allowed(opctx, user_id).await? {
            return Err(Error::Unauthenticated {
                internal_message: "User not allowed to login".to_string(),
            });
        }

        let session =
            db::model::ConsoleSession::new(generate_session_token(), user_id);

        Ok(self.db_datastore.session_create(opctx, session).await?)
    }

    // update last_used to now
    pub async fn session_update_last_used(
        &self,
        opctx: &OpContext,
        token: &str,
    ) -> UpdateResult<authn::ConsoleSessionWithSiloId> {
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            token.to_string(),
            LookupType::ByCompositeId(token.to_string()),
        );
        Ok(self
            .db_datastore
            .session_update_last_used(opctx, &authz_session)
            .await?)
    }

    pub async fn session_hard_delete(
        &self,
        opctx: &OpContext,
        token: &str,
    ) -> DeleteResult {
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            token.to_string(),
            LookupType::ByCompositeId(token.to_string()),
        );
        self.db_datastore.session_hard_delete(opctx, &authz_session).await
    }

    fn tuf_base_url(&self) -> Option<String> {
        self.updates_config.as_ref().map(|c| {
            let rack = self.as_rack();
            rack.tuf_base_url.unwrap_or_else(|| c.default_base_url.clone())
        })
    }

    pub async fn updates_refresh_metadata(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let updates_config = self.updates_config.as_ref().ok_or_else(|| {
            Error::InvalidRequest {
                message: "updates system not configured".into(),
            }
        })?;
        let base_url =
            self.tuf_base_url().ok_or_else(|| Error::InvalidRequest {
                message: "updates system not configured".into(),
            })?;
        let trusted_root = tokio::fs::read(&updates_config.trusted_root)
            .await
            .map_err(|e| Error::InternalError {
                internal_message: format!(
                    "error trying to read trusted root: {}",
                    e
                ),
            })?;

        let artifacts = tokio::task::spawn_blocking(move || {
            crate::updates::read_artifacts(&trusted_root, base_url)
        })
        .await
        .unwrap()
        .map_err(|e| Error::InternalError {
            internal_message: format!("error trying to refresh updates: {}", e),
        })?;

        // FIXME: if we hit an error in any of these database calls, the
        // available artifact table will be out of sync with the current
        // artifacts.json. can we do a transaction or something?

        let mut current_version = None;
        for artifact in &artifacts {
            current_version = Some(artifact.targets_role_version);
            self.db_datastore
                .update_available_artifact_upsert(&opctx, artifact.clone())
                .await?;
        }

        // ensure table is in sync with current copy of artifacts.json
        if let Some(current_version) = current_version {
            self.db_datastore
                .update_available_artifact_hard_delete_outdated(
                    &opctx,
                    current_version,
                )
                .await?;
        }

        // demo-grade update logic: tell all sleds to apply all artifacts
        for sled in self
            .db_datastore
            .sled_list(
                &opctx,
                &DataPageParams {
                    marker: None,
                    direction: PaginationOrder::Ascending,
                    limit: NonZeroU32::new(100).unwrap(),
                },
            )
            .await?
        {
            let client = self.sled_client(&sled.id()).await?;
            for artifact in &artifacts {
                info!(
                    self.log,
                    "telling sled {} to apply {}",
                    sled.id(),
                    artifact.target_name
                );
                client
                    .update_artifact(
                        &sled_agent_client::types::UpdateArtifact {
                            name: artifact.name.clone(),
                            version: artifact.version,
                            kind: artifact.kind.0.into(),
                        },
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// Downloads a file from within [`BASE_ARTIFACT_DIR`].
    pub async fn download_artifact(
        &self,
        opctx: &OpContext,
        artifact: UpdateArtifact,
    ) -> Result<Vec<u8>, Error> {
        let mut base_url =
            self.tuf_base_url().ok_or_else(|| Error::InvalidRequest {
                message: "updates system not configured".into(),
            })?;
        if !base_url.ends_with('/') {
            base_url.push('/');
        }

        // We cache the artifact based on its checksum, so fetch that from the
        // database.
        let (.., artifact_entry) = LookupPath::new(opctx, &self.db_datastore)
            .update_available_artifact_tuple(
                &artifact.name,
                artifact.version,
                UpdateArtifactKind(artifact.kind),
            )
            .fetch()
            .await?;
        let filename = format!(
            "{}.{}.{}-{}",
            artifact_entry.target_sha256,
            artifact.kind,
            artifact.name,
            artifact.version
        );
        let path = Path::new(BASE_ARTIFACT_DIR).join(&filename);

        if !path.exists() {
            // If the artifact doesn't exist, we should download it.
            //
            // TODO: There also exists the question of "when should we *remove*
            // things from BASE_ARTIFACT_DIR", which we should also resolve.
            // Demo-quality solution could be "destroy it on boot" or something?
            // (we aren't doing that yet).
            info!(self.log, "Accessing {} - needs to be downloaded", filename);
            tokio::fs::create_dir_all(BASE_ARTIFACT_DIR).await.map_err(
                |e| {
                    Error::internal_error(&format!(
                        "Failed to create artifacts directory: {}",
                        e
                    ))
                },
            )?;

            let mut response = reqwest::get(format!(
                "{}targets/{}.{}",
                base_url,
                artifact_entry.target_sha256,
                artifact_entry.target_name
            ))
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "Failed to fetch artifact: {}",
                    e
                ))
            })?;

            // To ensure another request isn't trying to use this target while we're downloading it
            // or before we've verified it, write to a random path in the same directory, then move
            // it to the correct path after verification.
            let temp_path = path.with_file_name(format!(
                ".{}.{:x}",
                filename,
                rand::thread_rng().gen::<u64>()
            ));
            let mut file =
                tokio::fs::File::create(&temp_path).await.map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to create file: {}",
                        e
                    ))
                })?;

            let mut context = digest::Context::new(&digest::SHA256);
            let mut length: i64 = 0;
            while let Some(chunk) = response.chunk().await.map_err(|e| {
                Error::internal_error(&format!(
                    "Failed to read HTTP body: {}",
                    e
                ))
            })? {
                file.write_all(&chunk).await.map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to write to file: {}",
                        e
                    ))
                })?;
                context.update(&chunk);
                length += i64::try_from(chunk.len()).unwrap();

                if length > artifact_entry.target_length {
                    return Err(Error::internal_error(&format!(
                        "target {} is larger than expected",
                        artifact_entry.target_name
                    )));
                }
            }
            drop(file);

            if hex::encode(context.finish()) == artifact_entry.target_sha256
                && length == artifact_entry.target_length
            {
                tokio::fs::rename(temp_path, &path).await.map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to rename file after verification: {}",
                        e
                    ))
                })?
            } else {
                return Err(Error::internal_error(&format!(
                    "failed to verify target {}",
                    artifact_entry.target_name
                )));
            }

            info!(
                self.log,
                "wrote {} to artifact dir", artifact_entry.target_name
            );
        } else {
            info!(self.log, "Accessing {} - already exists", path.display());
        }

        // TODO: These artifacts could be quite large - we should figure out how to
        // stream this file back instead of holding it entirely in-memory in a
        // Vec<u8>.
        //
        // Options:
        // - RFC 7233 - "Range Requests" (is this HTTP/1.1 only?)
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests
        // - "Roll our own". See:
        // https://stackoverflow.com/questions/20969331/standard-method-for-http-partial-upload-resume-upload
        let body = tokio::fs::read(&path).await.map_err(|e| {
            Error::internal_error(&format!(
                "Cannot read artifact from filesystem: {}",
                e
            ))
        })?;
        Ok(body)
    }

    async fn login_allowed(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> Result<bool, Error> {
        // Was this silo user deleted?
        let fetch_result = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(silo_user_id)
            .fetch()
            .await;

        match fetch_result {
            Err(e) => {
                match e {
                    Error::ObjectNotFound { type_name: _, lookup_type: _ } => {
                        // if the silo user was deleted, they're not allowed to
                        // log in :)
                        return Ok(false);
                    }

                    _ => {
                        return Err(e);
                    }
                }
            }

            Ok(_) => {
                // they're allowed
            }
        }

        Ok(true)
    }

    pub async fn silo_user_fetch(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> LookupResult<SiloUser> {
        let (.., db_silo_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(silo_user_id)
            .fetch()
            .await?;
        Ok(db_silo_user)
    }
}

/// For unimplemented endpoints, indicates whether the resource identified
/// by this endpoint will always be publicly visible or not
///
/// For example, the resource "/images" is well-known (it's part of the
/// API).  Being unauthorized to list images will result in a "403
/// Forbidden".  It's `UnimplResourceVisibility::Public'.
///
/// By contrast, the resource "/images/some-image" is not publicly-known.
/// If you're not authorized to view it, you'll get a "404 Not Found".  It's
/// `Unimpl::ProtectedLookup(LookupType::ByName("some-image"))`.
pub enum Unimpl {
    Public,
    ProtectedLookup(Error),
}

fn generate_session_token() -> String {
    // TODO: "If getrandom is unable to provide secure entropy this method will panic."
    // Should we explicitly handle that?
    // TODO: store generator somewhere so we don't reseed every time
    let mut rng = StdRng::from_entropy();
    // OWASP recommends at least 64 bits of entropy, OAuth 2 spec 128 minimum, 160 recommended
    // 20 bytes = 160 bits of entropy
    // TODO: the size should be a constant somewhere, maybe even in config?
    let mut random_bytes: [u8; 20] = [0; 20];
    rng.fill_bytes(&mut random_bytes);
    hex::encode(random_bytes)
}

#[async_trait]
impl TestInterfaces for Nexus {
    async fn instance_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore),
        );
        let (.., db_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(*id)
            .fetch()
            .await?;
        self.instance_sled(&db_instance).await
    }

    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore),
        );
        let (.., db_disk) = LookupPath::new(&opctx, &self.db_datastore)
            .disk_id(*id)
            .fetch()
            .await?;
        let (.., db_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(db_disk.runtime().attach_instance_id.unwrap())
            .fetch()
            .await?;
        self.instance_sled(&db_instance).await
    }

    async fn set_disk_as_faulted(&self, disk_id: &Uuid) -> Result<bool, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore),
        );

        let (.., authz_disk, db_disk) =
            LookupPath::new(&opctx, &self.db_datastore)
                .disk_id(*disk_id)
                .fetch()
                .await?;

        let new_runtime = db_disk.runtime_state.faulted();

        self.db_datastore
            .disk_update_runtime(&opctx, &authz_disk, &new_runtime)
            .await
    }

    async fn silo_user_create(
        &self,
        silo_id: Uuid,
        silo_user_id: Uuid,
    ) -> CreateResult<SiloUser> {
        let silo_user = SiloUser::new(silo_id, silo_user_id);
        Ok(self.db_datastore.silo_user_create(silo_user).await?)
    }
}
