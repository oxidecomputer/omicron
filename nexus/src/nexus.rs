// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Nexus, the service that operates much of the control plane in an Oxide fleet
 */

use crate::authn;
use crate::authz;
use crate::config;
use crate::context::OpContext;
use crate::db;
use crate::db::identity::{Asset, Resource};
use crate::db::model::DatasetKind;
use crate::db::model::Name;
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
use ipnetwork::Ipv4Network;
use ipnetwork::Ipv6Network;
use lazy_static::lazy_static;
use omicron_common::api::external;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::DiskAttachment;
use omicron_common::api::external::DiskState;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::Ipv4Net;
use omicron_common::api::external::Ipv6Net;
use omicron_common::api::external::ListResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::PaginationOrder;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::RouteDestination;
use omicron_common::api::external::RouteTarget;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::RouterRouteUpdateParams;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_common::api::external::VpcFirewallRuleUpdateResult;
use omicron_common::api::external::VpcRouterKind;
use omicron_common::api::internal::nexus;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested;
use omicron_common::api::internal::sled_agent::InstanceStateRequested;
use omicron_common::backoff;
use omicron_common::bail_unless;
use oximeter_client::Client as OximeterClient;
use oximeter_producer::register;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use sled_agent_client::Client as SledAgentClient;
use slog::Logger;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use steno::SagaId;
use steno::SagaResultOk;
use steno::SagaTemplate;
use steno::SagaType;
use uuid::Uuid;

// TODO: When referring to API types, we should try to include
// the prefix unless it is unambiguous.

/**
 * Exposes additional [`Nexus`] interfaces for use by the test suite
 */
#[async_trait]
pub trait TestInterfaces {
    /**
     * Returns the SledAgentClient for an Instance from its id.  We may also
     * want to split this up into instance_lookup_by_id() and instance_sled(),
     * but after all it's a test suite special to begin with.
     */
    async fn instance_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error>;

    /**
     * Returns the SledAgentClient for a Disk from its id.
     */
    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error>;

    async fn session_create_with(
        &self,
        session: db::model::ConsoleSession,
    ) -> CreateResult<db::model::ConsoleSession>;
}

/**
 * Manages an Oxide fleet -- the heart of the control plane
 */
pub struct Nexus {
    /** uuid for this nexus instance. */
    id: Uuid,

    /** uuid for this rack (TODO should also be in persistent storage) */
    rack_id: Uuid,

    /** general server log */
    log: Logger,

    /** cached rack identity metadata */
    api_rack_identity: db::model::RackIdentity,

    /** persistent storage for resources in the control plane */
    db_datastore: Arc<db::DataStore>,

    /** saga execution coordinator */
    sec_client: Arc<steno::SecClient>,

    /** Task representing completion of recovered Sagas */
    recovery_task: std::sync::Mutex<Option<db::RecoveryTask>>,

    /** Status of background task to populate database */
    populate_status: tokio::sync::watch::Receiver<PopulateStatus>,
}

/*
 * TODO Is it possible to make some of these operations more generic?  A
 * particularly good example is probably list() (or even lookup()), where
 * with the right type parameters, generic code can be written to work on all
 * types.
 * TODO update and delete need to accommodate both with-etag and don't-care
 * TODO audit logging ought to be part of this structure and its functions
 */
impl Nexus {
    /**
     * Create a new Nexus instance for the given rack id `rack_id`
     */
    /* TODO-polish revisit rack metadata */
    pub fn new_with_id(
        rack_id: &Uuid,
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

        /*
         * TODO-cleanup We may want a first-class subsystem for managing startup
         * background tasks.  It could use a Future for each one, a status enum
         * for each one, status communication via channels, and a single task to
         * run them all.
         */
        let populate_ctx = OpContext::for_background(
            log.new(o!("component" => "DataLoader")),
            Arc::clone(&authz),
            authn::Context::internal_db_init(),
        );
        let populate_status =
            populate_start(populate_ctx, Arc::clone(&db_datastore));

        let nexus = Nexus {
            id: config.id,
            rack_id: *rack_id,
            log: log.new(o!()),
            api_rack_identity: db::model::RackIdentity::new(*rack_id),
            db_datastore: Arc::clone(&db_datastore),
            sec_client: Arc::clone(&sec_client),
            recovery_task: std::sync::Mutex::new(None),
            populate_status,
        };

        /* TODO-cleanup all the extra Arcs here seems wrong */
        let nexus_arc = Arc::new(nexus);
        let opctx = OpContext::for_background(
            log.new(o!("component" => "SagaRecoverer")),
            authz,
            authn::Context::internal_saga_recovery(),
        );
        let recovery_task = db::recover(
            opctx,
            my_sec_id,
            Arc::new(Arc::new(SagaContext::new(Arc::clone(&nexus_arc)))),
            db_datastore,
            Arc::clone(&sec_client),
            &sagas::ALL_TEMPLATES,
        );

        *nexus_arc.recovery_task.lock().unwrap() = Some(recovery_task);
        nexus_arc
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

    /*
     * TODO-robustness we should have a limit on how many sled agents there can
     * be (for graceful degradation at large scale).
     */
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
        info!(self.log, "upserting dataset"; "zpool_id" => zpool_id.to_string(), "dataset_id" => id.to_string());
        let dataset = db::model::Dataset::new(id, zpool_id, address, kind);
        self.db_datastore.dataset_upsert(dataset).await?;
        Ok(())
    }

    /**
     * Insert a new record of an Oximeter collector server.
     */
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
                .map_err(backoff::BackoffError::Transient)
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

    /**
     * List all registered Oximeter collector instances.
     */
    pub async fn oximeter_list(
        &self,
        page_params: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::OximeterInfo> {
        self.db_datastore.oximeter_list(page_params).await
    }

    pub fn datastore(&self) -> &db::DataStore {
        &self.db_datastore
    }

    /**
     * Given a saga template and parameters, create a new saga and execute it.
     */
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
        /*
         * TODO-cleanup The bound `P: Serialize` should not be necessary because
         * SagaParamsType must already impl Serialize.
         */
        P: serde::Serialize,
    {
        let saga_id = SagaId(Uuid::new_v4());
        let saga_context =
            Arc::new(Arc::new(SagaContext::new(Arc::clone(self))));
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
                /*
                 * TODO-error This could be a service unavailable error,
                 * depending on the failure mode.  We need more information from
                 * Steno.
                 */
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
                /* TODO-error more context would be useful */
                Error::InternalError { internal_message: e.to_string() }
            })
        })
    }

    /*
     * Organizations
     */

    pub async fn organization_create(
        &self,
        opctx: &OpContext,
        new_organization: &params::OrganizationCreate,
    ) -> CreateResult<db::model::Organization> {
        let db_org = db::model::Organization::new(new_organization.clone());
        self.db_datastore.organization_create(opctx, db_org).await
    }

    pub async fn organization_fetch(
        &self,
        name: &Name,
    ) -> LookupResult<db::model::Organization> {
        self.db_datastore.organization_fetch(name).await
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

    pub async fn organization_delete(&self, name: &Name) -> DeleteResult {
        self.db_datastore.organization_delete(name).await
    }

    pub async fn organization_update(
        &self,
        name: &Name,
        new_params: &params::OrganizationUpdate,
    ) -> UpdateResult<db::model::Organization> {
        self.db_datastore
            .organization_update(name, new_params.clone().into())
            .await
    }

    /*
     * Projects
     */

    pub async fn project_create(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        new_project: &params::ProjectCreate,
    ) -> CreateResult<db::model::Project> {
        let org =
            self.db_datastore.organization_lookup(organization_name).await?;

        // Create a project.
        let db_project = db::model::Project::new(org.id(), new_project.clone());
        let db_project =
            self.db_datastore.project_create(opctx, &org, db_project).await?;

        // TODO: We probably want to have "project creation" and "default VPC
        // creation" co-located within a saga for atomicity.
        //
        // Until then, we just perform the operations sequentially.

        // Create a default VPC associated with the project.
        let _ = self
            .project_create_vpc(
                &organization_name,
                &new_project.identity.name.clone().into(),
                &params::VpcCreate {
                    identity: IdentityMetadataCreateParams {
                        name: "default".parse().unwrap(),
                        description: "Default VPC".to_string(),
                    },
                    // TODO-robustness this will need to be None if we decide to handle
                    // the logic around name and dns_name by making dns_name optional
                    dns_name: "default".parse().unwrap(),
                },
            )
            .await?;

        Ok(db_project)
    }

    pub async fn project_fetch(
        &self,
        organization_name: &Name,
        project_name: &Name,
    ) -> LookupResult<db::model::Project> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        self.db_datastore.project_fetch(&organization_id, project_name).await
    }

    pub async fn projects_list_by_name(
        &self,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Project> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        self.db_datastore
            .projects_list_by_name(&organization_id, pagparams)
            .await
    }

    pub async fn projects_list_by_id(
        &self,
        organization_name: &Name,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Project> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        self.db_datastore.projects_list_by_id(&organization_id, pagparams).await
    }

    pub async fn project_delete(
        &self,
        organization_name: &Name,
        project_name: &Name,
    ) -> DeleteResult {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        self.db_datastore.project_delete(&organization_id, project_name).await
    }

    pub async fn project_update(
        &self,
        organization_name: &Name,
        project_name: &Name,
        new_params: &params::ProjectUpdate,
    ) -> UpdateResult<db::model::Project> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        self.db_datastore
            .project_update(
                &organization_id,
                project_name,
                new_params.clone().into(),
            )
            .await
    }

    /*
     * Disks
     */

    pub async fn project_list_disks(
        &self,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Disk> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        self.db_datastore.project_list_disks(&project_id, pagparams).await
    }

    pub async fn project_create_disk(
        self: &Arc<Self>,
        organization_name: &Name,
        project_name: &Name,
        params: &params::DiskCreate,
    ) -> CreateResult<db::model::Disk> {
        let project =
            self.project_fetch(organization_name, project_name).await?;

        /*
         * Until we implement snapshots, do not allow disks to be created with a
         * snapshot id.
         */
        if params.snapshot_id.is_some() {
            return Err(Error::InvalidValue {
                label: String::from("snapshot_id"),
                message: String::from("snapshots are not yet supported"),
            });
        }

        let saga_params = Arc::new(sagas::ParamsDiskCreate {
            project_id: project.id(),
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

    pub async fn project_lookup_disk(
        &self,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> LookupResult<(db::model::Disk, authz::ProjectChild)> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        let disk = self
            .db_datastore
            .disk_fetch_by_name(&project_id, disk_name)
            .await?;
        let disk_id = disk.id();
        Ok((
            disk,
            authz::FLEET
                .organization(organization_id)
                .project(project_id)
                .resource(ResourceType::Disk, disk_id),
        ))
    }

    pub async fn project_delete_disk(
        &self,
        opctx: &OpContext,
        organization_name: &Name,
        project_name: &Name,
        disk_name: &Name,
    ) -> DeleteResult {
        let (disk, authz_disk) = self
            .project_lookup_disk(organization_name, project_name, disk_name)
            .await?;
        let runtime = disk.runtime();
        bail_unless!(runtime.state().state() != &DiskState::Destroyed);

        if runtime.state().is_attached() {
            return Err(Error::InvalidRequest {
                message: String::from("disk is attached"),
            });
        }

        /*
         * TODO-correctness It's not clear how this handles the case where we
         * begin this delete operation while some other request is ongoing to
         * attach the disk.  We won't be able to see that in the state here.  We
         * might be able to detect this when we go update the disk's state to
         * Attaching (because a SQL UPDATE will update 0 rows), but we'd sort of
         * already be in a bad state because the destroyed disk will be
         * attaching (and eventually attached) on some sled, and if the wrong
         * combination of components crash at this point, we could wind up not
         * fixing that state.
         *
         * This is a consequence of the choice _not_ to record the Attaching
         * state in the database before beginning the attach process.  If we did
         * that, we wouldn't have this problem, but we'd have a similar problem
         * of dealing with the case of a crash after recording this state and
         * before actually beginning the attach process.  Sagas can maybe
         * address that.
         */
        self.db_datastore.project_delete_disk(opctx, authz_disk).await
    }

    /*
     * Instances
     */

    /*
     * TODO-design This interface should not exist.  See
     * SagaContext::alloc_server().
     */
    pub async fn sled_allocate(&self) -> Result<Uuid, Error> {
        // TODO: replace this with a real allocation policy.
        //
        // This implementation always assigns the first sled (by ID order).
        let pagparams = DataPageParams {
            marker: None,
            direction: dropshot::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(1).unwrap(),
        };
        let sleds = self.db_datastore.sled_list(&pagparams).await?;

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
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Instance> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        self.db_datastore.project_list_instances(&project_id, pagparams).await
    }

    pub async fn project_create_instance(
        self: &Arc<Self>,
        organization_name: &Name,
        project_name: &Name,
        params: &params::InstanceCreate,
    ) -> CreateResult<db::model::Instance> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;

        let saga_params = Arc::new(sagas::ParamsInstanceCreate {
            project_id,
            create_params: params.clone(),
        });

        let saga_outputs = self
            .execute_saga(
                Arc::clone(&sagas::SAGA_INSTANCE_CREATE_TEMPLATE),
                sagas::SAGA_INSTANCE_CREATE_NAME,
                saga_params,
            )
            .await?;
        /* TODO-error more context would be useful  */
        let instance_id =
            saga_outputs.lookup_output::<Uuid>("instance_id").map_err(|e| {
                Error::InternalError { internal_message: e.to_string() }
            })?;
        /*
         * TODO-correctness TODO-robustness TODO-design It's not quite correct
         * to take this instance id and look it up again.  It's possible that
         * it's been modified or even deleted since the saga executed.  In that
         * case, we might return a different state of the Instance than the one
         * that the user created or even fail with a 404!  Both of those are
         * wrong behavior -- we should be returning the very instance that the
         * user created.
         *
         * How can we fix this?  Right now we have internal representations like
         * Instance and analaogous end-user-facing representations like
         * Instance.  The former is not even serializable.  The saga
         * _could_ emit the View version, but that's not great for two (related)
         * reasons: (1) other sagas might want to provision instances and get
         * back the internal representation to do other things with the
         * newly-created instance, and (2) even within a saga, it would be
         * useful to pass a single Instance representation along the saga,
         * but they probably would want the internal representation, not the
         * view.
         *
         * The saga could emit an Instance directly.  Today, Instance
         * etc. aren't supposed to even be serializable -- we wanted to be able
         * to have other datastore state there if needed.  We could have a third
         * InstanceInternalView...but that's starting to feel pedantic.  We
         * could just make Instance serializable, store that, and call it a
         * day.  Does it matter that we might have many copies of the same
         * objects in memory?
         *
         * If we make these serializable, it would be nice if we could leverage
         * the type system to ensure that we never accidentally send them out a
         * dropshot endpoint.  (On the other hand, maybe we _do_ want to do
         * that, for internal interfaces!  Can we do this on a
         * per-dropshot-server-basis?)
         */
        let instance = self.db_datastore.instance_fetch(&instance_id).await?;
        Ok(instance)
    }

    /*
     * TODO-correctness It's not totally clear what the semantics and behavior
     * should be here.  It might be nice to say that you can only do this
     * operation if the Instance is already stopped, in which case we can
     * execute this immediately by just removing it from the database, with the
     * same race we have with disk delete (i.e., if someone else is requesting
     * an instance boot, we may wind up in an inconsistent state).  On the other
     * hand, we could always allow this operation, issue the request to the SA
     * to destroy the instance (not just stop it), and proceed with deletion
     * when that finishes.  But in that case, although the HTTP DELETE request
     * completed, the object will still appear for a little while, which kind of
     * sucks.
     */
    pub async fn project_destroy_instance(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> DeleteResult {
        /*
         * TODO-robustness We need to figure out what to do with Destroyed
         * instances?  Presumably we need to clean them up at some point, but
         * not right away so that callers can see that they've been destroyed.
         */
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        let instance = self
            .db_datastore
            .instance_fetch_by_name(&project_id, instance_name)
            .await?;
        self.db_datastore.project_delete_instance(&instance.id()).await
    }

    pub async fn project_lookup_instance(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> LookupResult<db::model::Instance> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        self.db_datastore
            .instance_fetch_by_name(&project_id, instance_name)
            .await
    }

    fn check_runtime_change_allowed(
        &self,
        runtime: &nexus::InstanceRuntimeState,
    ) -> Result<(), Error> {
        /*
         * Users are allowed to request a start or stop even if the instance is
         * already in the desired state (or moving to it), and we will issue a
         * request to the SA to make the state change in these cases in case the
         * runtime state we saw here was stale.  However, users are not allowed
         * to change the state of an instance that's failed or destroyed.
         */
        let allowed = match runtime.run_state {
            InstanceState::Creating => true,
            InstanceState::Starting => true,
            InstanceState::Running => true,
            InstanceState::Stopping => true,
            InstanceState::Stopped => true,
            InstanceState::Rebooting => true,

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
        let sled = self.sled_lookup(id).await?;

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

    /**
     * Returns the SledAgentClient for the host where this Instance is running.
     */
    async fn instance_sled(
        &self,
        instance: &db::model::Instance,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let sa_id = &instance.runtime().sled_uuid;
        self.sled_client(&sa_id).await
    }

    /**
     * Reboot the specified instance.
     */
    pub async fn instance_reboot(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> UpdateResult<db::model::Instance> {
        /*
         * To implement reboot, we issue a call to the sled agent to set a
         * runtime state of "reboot". We cannot simply stop the Instance and
         * start it again here because if we crash in the meantime, we might
         * leave it stopped.
         *
         * When an instance is rebooted, the "rebooting" flag remains set on
         * the runtime state as it transitions to "Stopping" and "Stopped".
         * This flag is cleared when the state goes to "Starting".  This way,
         * even if the whole rack powered off while this was going on, we would
         * never lose track of the fact that this Instance was supposed to be
         * running.
         */
        let instance = self
            .project_lookup_instance(
                organization_name,
                project_name,
                instance_name,
            )
            .await?;

        self.check_runtime_change_allowed(&instance.runtime().clone().into())?;
        self.instance_set_runtime(
            &instance,
            self.instance_sled(&instance).await?,
            InstanceRuntimeStateRequested {
                run_state: InstanceStateRequested::Reboot,
            },
        )
        .await?;
        self.db_datastore.instance_fetch(&instance.id()).await
    }

    /**
     * Make sure the given Instance is running.
     */
    pub async fn instance_start(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> UpdateResult<db::model::Instance> {
        let instance = self
            .project_lookup_instance(
                organization_name,
                project_name,
                instance_name,
            )
            .await?;

        self.check_runtime_change_allowed(&instance.runtime().clone().into())?;
        self.instance_set_runtime(
            &instance,
            self.instance_sled(&instance).await?,
            InstanceRuntimeStateRequested {
                run_state: InstanceStateRequested::Running,
            },
        )
        .await?;
        self.db_datastore.instance_fetch(&instance.id()).await
    }

    /**
     * Make sure the given Instance is stopped.
     */
    pub async fn instance_stop(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
    ) -> UpdateResult<db::model::Instance> {
        let instance = self
            .project_lookup_instance(
                organization_name,
                project_name,
                instance_name,
            )
            .await?;

        self.check_runtime_change_allowed(&instance.runtime().clone().into())?;
        self.instance_set_runtime(
            &instance,
            self.instance_sled(&instance).await?,
            InstanceRuntimeStateRequested {
                run_state: InstanceStateRequested::Stopped,
            },
        )
        .await?;
        self.db_datastore.instance_fetch(&instance.id()).await
    }

    /**
     * Modifies the runtime state of the Instance as requested.  This generally
     * means booting or halting the Instance.
     */
    async fn instance_set_runtime(
        &self,
        instance: &db::model::Instance,
        sa: Arc<SledAgentClient>,
        requested: InstanceRuntimeStateRequested,
    ) -> Result<(), Error> {
        /*
         * Ask the sled agent to begin the state change.  Then update the
         * database to reflect the new intermediate state.  If this update is
         * not the newest one, that's fine.  That might just mean the sled agent
         * beat us to it.
         */

        let runtime: nexus::InstanceRuntimeState =
            instance.runtime().clone().into();

        // TODO: Populate this with an appropriate NIC.
        // See also: sic_create_instance_record in sagas.rs for a similar
        // construction.
        let instance_hardware = sled_agent_client::types::InstanceHardware {
            runtime: sled_agent_client::types::InstanceRuntimeState::from(
                runtime,
            ),
            nics: vec![],
        };

        let new_runtime = sa
            .instance_put(
                &instance.id(),
                &sled_agent_client::types::InstanceEnsureBody {
                    initial: instance_hardware,
                    target: requested.into(),
                },
            )
            .await
            .map_err(Error::from)?;

        let new_runtime: nexus::InstanceRuntimeState = new_runtime.into();

        self.db_datastore
            .instance_update_runtime(&instance.id(), &new_runtime.into())
            .await
            .map(|_| ())
    }

    /**
     * Lists disks attached to the instance.
     */
    pub async fn instance_list_disks(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::DiskAttachment> {
        let instance = self
            .project_lookup_instance(
                organization_name,
                project_name,
                instance_name,
            )
            .await?;
        self.db_datastore.instance_list_disks(&instance.id(), pagparams).await
    }

    /**
     * Fetch information about whether this disk is attached to this instance.
     */
    pub async fn instance_get_disk(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        disk_name: &Name,
    ) -> LookupResult<DiskAttachment> {
        let instance = self
            .project_lookup_instance(
                organization_name,
                project_name,
                instance_name,
            )
            .await?;
        // TODO: This shouldn't be looking up multiple database entries by name,
        // it should resolve names to IDs first.
        let (disk, _) = self
            .project_lookup_disk(organization_name, project_name, disk_name)
            .await?;
        if let Some(instance_id) = disk.runtime_state.attach_instance_id {
            if instance_id == instance.id() {
                return Ok(DiskAttachment {
                    instance_id: instance.id(),
                    disk_name: disk.name().clone().into(),
                    disk_id: disk.id(),
                    disk_state: disk.state().into(),
                });
            }
        }

        Err(Error::not_found_other(
            ResourceType::DiskAttachment,
            format!(
                "disk \"{}\" is not attached to instance \"{}\"",
                disk_name.as_str(),
                instance_name.as_str()
            ),
        ))
    }

    /**
     * Attach a disk to an instance.
     */
    pub async fn instance_attach_disk(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        disk_name: &Name,
    ) -> CreateResult<DiskAttachment> {
        let instance = self
            .project_lookup_instance(
                organization_name,
                project_name,
                instance_name,
            )
            .await?;
        // TODO: This shouldn't be looking up multiple database entries by name,
        // it should resolve names to IDs first.
        let (disk, _) = self
            .project_lookup_disk(organization_name, project_name, disk_name)
            .await?;
        let instance_id = &instance.id();

        fn disk_attachment_for(
            instance: &db::model::Instance,
            disk: &db::model::Disk,
        ) -> CreateResult<DiskAttachment> {
            assert_eq!(
                instance.id(),
                disk.runtime_state.attach_instance_id.unwrap()
            );
            Ok(DiskAttachment {
                instance_id: instance.id(),
                disk_id: disk.id(),
                disk_name: disk.name().clone().into(),
                disk_state: disk.runtime().state().into(),
            })
        }

        fn disk_attachment_error(
            disk: &db::model::Disk,
        ) -> CreateResult<DiskAttachment> {
            let disk_status = match disk.runtime().state().into() {
                DiskState::Destroyed => "disk is destroyed",
                DiskState::Faulted => "disk is faulted",
                DiskState::Creating => "disk is detached",
                DiskState::Detached => "disk is detached",

                /*
                 * It would be nice to provide a more specific message here, but
                 * the appropriate identifier to provide the user would be the
                 * other instance's name.  Getting that would require another
                 * database hit, which doesn't seem worth it for this.
                 */
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

        match &disk.state().into() {
            /*
             * If we're already attaching or attached to the requested instance,
             * there's nothing else to do.
             */
            DiskState::Attached(id) if id == instance_id => {
                return disk_attachment_for(&instance, &disk);
            }

            /*
             * If the disk is currently attaching or attached to another
             * instance, fail this request.  Users must explicitly detach first
             * if that's what they want.  If it's detaching, they have to wait
             * for it to become detached.
             * TODO-debug: the error message here could be better.  We'd have to
             * look up the other instance by id (and gracefully handle it not
             * existing).
             */
            DiskState::Attached(id) => {
                assert_ne!(id, instance_id);
                return disk_attachment_error(&disk);
            }
            DiskState::Detaching(_) => {
                return disk_attachment_error(&disk);
            }
            DiskState::Attaching(id) if id != instance_id => {
                return disk_attachment_error(&disk);
            }
            DiskState::Destroyed => {
                return disk_attachment_error(&disk);
            }
            DiskState::Faulted => {
                return disk_attachment_error(&disk);
            }

            DiskState::Creating => (),
            DiskState::Detached => (),
            DiskState::Attaching(id) => {
                assert_eq!(id, instance_id);
            }
        }

        self.disk_set_runtime(
            &disk,
            self.instance_sled(&instance).await?,
            sled_agent_client::types::DiskStateRequested::Attached(
                *instance_id,
            ),
        )
        .await?;
        let disk = self.db_datastore.disk_fetch(&disk.id()).await?;
        disk_attachment_for(&instance, &disk)
    }

    /**
     * Detach a disk from an instance.
     */
    pub async fn instance_detach_disk(
        &self,
        organization_name: &Name,
        project_name: &Name,
        instance_name: &Name,
        disk_name: &Name,
    ) -> DeleteResult {
        let instance = self
            .project_lookup_instance(
                organization_name,
                project_name,
                instance_name,
            )
            .await?;
        // TODO: This shouldn't be looking up multiple database entries by name,
        // it should resolve names to IDs first.
        let (disk, _) = self
            .project_lookup_disk(organization_name, project_name, disk_name)
            .await?;
        let instance_id = &instance.id();

        match &disk.state().into() {
            /*
             * This operation is a noop if the disk is not attached or already
             * detaching from the same instance.
             */
            DiskState::Creating => return Ok(()),
            DiskState::Detached => return Ok(()),
            DiskState::Destroyed => return Ok(()),
            DiskState::Faulted => return Ok(()),
            DiskState::Detaching(id) if id == instance_id => return Ok(()),

            /*
             * This operation is not allowed if the disk is attached to some
             * other instance.
             */
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

            /* These are the cases where we have to do something. */
            DiskState::Attaching(_) => (),
            DiskState::Attached(_) => (),
        }

        self.disk_set_runtime(
            &disk,
            self.instance_sled(&instance).await?,
            sled_agent_client::types::DiskStateRequested::Detached,
        )
        .await?;
        Ok(())
    }

    /**
     * Modifies the runtime state of the Disk as requested.  This generally
     * means attaching or detaching the disk.
     */
    async fn disk_set_runtime(
        &self,
        disk: &db::model::Disk,
        sa: Arc<SledAgentClient>,
        requested: sled_agent_client::types::DiskStateRequested,
    ) -> Result<(), Error> {
        let runtime: DiskRuntimeState = disk.runtime().into();

        /*
         * Ask the SA to begin the state change.  Then update the database to
         * reflect the new intermediate state.
         */
        let new_runtime = sa
            .disk_put(
                &disk.id(),
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

        let new_runtime: DiskRuntimeState = new_runtime.into();

        self.db_datastore
            .disk_update_runtime(&disk.id(), &new_runtime.into())
            .await
            .map(|_| ())
    }

    pub async fn project_list_vpcs(
        &self,
        organization_name: &Name,
        project_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::Vpc> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        let vpcs =
            self.db_datastore.project_list_vpcs(&project_id, pagparams).await?;
        Ok(vpcs)
    }

    pub async fn project_create_vpc(
        &self,
        organization_name: &Name,
        project_name: &Name,
        params: &params::VpcCreate,
    ) -> CreateResult<db::model::Vpc> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        let vpc_id = Uuid::new_v4();
        let system_router_id = Uuid::new_v4();
        let default_route_id = Uuid::new_v4();
        let default_subnet_id = Uuid::new_v4();
        // TODO: Ultimately when the VPC is created a system router w/ an appropriate setup should also be created.
        // Given that the underlying systems aren't wired up yet this is a naive implementation to populate the database
        // with a starting router. Eventually this code should be replaced with a saga that'll handle creating the VPC and
        // its underlying system
        let router = db::model::VpcRouter::new(
            system_router_id,
            vpc_id,
            VpcRouterKind::System,
            params::VpcRouterCreate {
                identity: IdentityMetadataCreateParams {
                    name: "system".parse().unwrap(),
                    description: "Routes are automatically added to this router as vpc subnets are created".into()
                }
            }
            );
        let _ = self.db_datastore.vpc_create_router(router).await?;
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

        // TODO: This is both fake and utter nonsense. It should be eventually replaced with the proper behavior for creating
        // the default route which may not even happen here. Creating the vpc, its system router, and that routers default route
        // should all be apart of the same transaction.
        self.db_datastore.router_create_route(route).await?;
        let vpc = db::model::Vpc::new(
            vpc_id,
            project_id,
            system_router_id,
            params.clone(),
        );
        let vpc = self.db_datastore.project_create_vpc(vpc).await?;

        // TODO: batch this up with everything above
        let subnet = db::model::VpcSubnet::new(
            default_subnet_id,
            vpc_id,
            params::VpcSubnetCreate {
                identity: IdentityMetadataCreateParams {
                    name: "default".parse().unwrap(),
                    description: format!(
                        "The default subnet for {}",
                        params.identity.name
                    ),
                },
                ipv4_block: Some(Ipv4Net(
                    // TODO: This value should be replaced with the correct ipv4 range for a default subnet
                    "10.1.9.32/16".parse::<Ipv4Network>().unwrap(),
                )),
                ipv6_block: Some(Ipv6Net(
                    // TODO: This value should be replaced w/ the first `/64` ipv6 from the address block
                    "2001:db8::0/64".parse::<Ipv6Network>().unwrap(),
                )),
            },
        );
        self.db_datastore.vpc_create_subnet(subnet).await?;

        self.create_default_vpc_firewall(&vpc_id).await?;
        Ok(vpc)
    }

    async fn create_default_vpc_firewall(
        &self,
        vpc_id: &Uuid,
    ) -> CreateResult<()> {
        let rules = db::model::VpcFirewallRule::vec_from_params(
            *vpc_id,
            DEFAULT_FIREWALL_RULES.clone(),
        );
        self.db_datastore.vpc_update_firewall_rules(&vpc_id, rules).await?;
        Ok(())
    }

    pub async fn project_lookup_vpc(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> LookupResult<db::model::Vpc> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        Ok(self.db_datastore.vpc_fetch_by_name(&project_id, vpc_name).await?)
    }

    pub async fn project_update_vpc(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcUpdate,
    ) -> UpdateResult<()> {
        let organization_id = self
            .db_datastore
            .organization_lookup_id_by_name(organization_name)
            .await?;
        let project_id = self
            .db_datastore
            .project_lookup_id_by_name(&organization_id, project_name)
            .await?;
        let vpc =
            self.db_datastore.vpc_fetch_by_name(&project_id, vpc_name).await?;
        Ok(self
            .db_datastore
            .project_update_vpc(&vpc.id(), params.clone().into())
            .await?)
    }

    pub async fn project_delete_vpc(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
    ) -> DeleteResult {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        // TODO: This should eventually use a saga to call the
        // networking subsystem to have it clean up the networking resources
        self.db_datastore.vpc_delete_router(&vpc.system_router_id).await?;
        self.db_datastore.project_delete_vpc(&vpc.id()).await?;

        // Delete all firewall rules after deleting the VPC, to ensure no
        // firewall rules get added between rules deletion and VPC deletion.
        self.db_datastore.vpc_delete_all_firewall_rules(&vpc.id()).await
    }

    pub async fn vpc_list_firewall_rules(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcFirewallRule> {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        let subnets = self
            .db_datastore
            .vpc_list_firewall_rules(&vpc.id(), pagparams)
            .await?;
        Ok(subnets)
    }

    pub async fn vpc_update_firewall_rules(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &VpcFirewallRuleUpdateParams,
    ) -> UpdateResult<VpcFirewallRuleUpdateResult> {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        let rules = db::model::VpcFirewallRule::vec_from_params(
            vpc.id(),
            params.clone(),
        );
        let result = self
            .db_datastore
            .vpc_update_firewall_rules(&vpc.id(), rules)
            .await?
            .into_iter()
            .map(|rule| rule.into())
            .collect();
        Ok(result)
    }

    pub async fn vpc_list_subnets(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcSubnet> {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        let subnets =
            self.db_datastore.vpc_list_subnets(&vpc.id(), pagparams).await?;
        Ok(subnets)
    }

    pub async fn vpc_lookup_subnet(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
    ) -> LookupResult<db::model::VpcSubnet> {
        // TODO: join projects, vpcs, and subnets and do this in one query
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        Ok(self
            .db_datastore
            .vpc_subnet_fetch_by_name(&vpc.id(), subnet_name)
            .await?)
    }

    // TODO: When a subnet is created it should add a route entry into the VPC's system router
    pub async fn vpc_create_subnet(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        params: &params::VpcSubnetCreate,
    ) -> CreateResult<db::model::VpcSubnet> {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        let id = Uuid::new_v4();
        let subnet = db::model::VpcSubnet::new(id, vpc.id(), params.clone());
        let subnet = self.db_datastore.vpc_create_subnet(subnet).await?;
        Ok(subnet)
    }

    // TODO: When a subnet is deleted it should remove its entry from the VPC's system router.
    pub async fn vpc_delete_subnet(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
    ) -> DeleteResult {
        let subnet = self
            .vpc_lookup_subnet(
                organization_name,
                project_name,
                vpc_name,
                subnet_name,
            )
            .await?;
        self.db_datastore.vpc_delete_subnet(&subnet.id()).await
    }

    pub async fn vpc_update_subnet(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        subnet_name: &Name,
        params: &params::VpcSubnetUpdate,
    ) -> UpdateResult<()> {
        let subnet = self
            .vpc_lookup_subnet(
                organization_name,
                project_name,
                vpc_name,
                subnet_name,
            )
            .await?;
        Ok(self
            .db_datastore
            .vpc_update_subnet(&subnet.id(), params.clone().into())
            .await?)
    }

    pub async fn vpc_list_routers(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::VpcRouter> {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        let routers =
            self.db_datastore.vpc_list_routers(&vpc.id(), pagparams).await?;
        Ok(routers)
    }

    pub async fn vpc_lookup_router(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> LookupResult<db::model::VpcRouter> {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        Ok(self
            .db_datastore
            .vpc_router_fetch_by_name(&vpc.id(), router_name)
            .await?)
    }

    pub async fn vpc_create_router(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        kind: &VpcRouterKind,
        params: &params::VpcRouterCreate,
    ) -> CreateResult<db::model::VpcRouter> {
        let vpc = self
            .project_lookup_vpc(organization_name, project_name, vpc_name)
            .await?;
        let id = Uuid::new_v4();
        let router =
            db::model::VpcRouter::new(id, vpc.id(), *kind, params.clone());
        let router = self.db_datastore.vpc_create_router(router).await?;
        Ok(router)
    }

    // TODO: When a router is deleted all its routes should be deleted
    // TODO: When a router is deleted it should be unassociated w/ any subnets it may be associated with
    //       or trigger an error
    pub async fn vpc_delete_router(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
    ) -> DeleteResult {
        let router = self
            .vpc_lookup_router(
                organization_name,
                project_name,
                vpc_name,
                router_name,
            )
            .await?;
        if router.kind.0 == VpcRouterKind::System {
            return Err(Error::MethodNotAllowed {
                internal_message: "Cannot delete system router".to_string(),
            });
        }
        self.db_datastore.vpc_delete_router(&router.id()).await
    }

    pub async fn vpc_update_router(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        params: &params::VpcRouterUpdate,
    ) -> UpdateResult<()> {
        let router = self
            .vpc_lookup_router(
                organization_name,
                project_name,
                vpc_name,
                router_name,
            )
            .await?;
        Ok(self
            .db_datastore
            .vpc_update_router(&router.id(), params.clone().into())
            .await?)
    }

    /**
     * VPC Router routes
     */

    pub async fn router_list_routes(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<db::model::RouterRoute> {
        let router = self
            .vpc_lookup_router(
                organization_name,
                project_name,
                vpc_name,
                router_name,
            )
            .await?;
        let routes = self
            .db_datastore
            .router_list_routes(&router.id(), pagparams)
            .await?;
        Ok(routes)
    }

    pub async fn router_lookup_route(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
    ) -> LookupResult<db::model::RouterRoute> {
        let router = self
            .vpc_lookup_router(
                organization_name,
                project_name,
                vpc_name,
                router_name,
            )
            .await?;
        Ok(self
            .db_datastore
            .router_route_fetch_by_name(&router.id(), route_name)
            .await?)
    }

    pub async fn router_create_route(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        kind: &RouterRouteKind,
        params: &RouterRouteCreateParams,
    ) -> CreateResult<db::model::RouterRoute> {
        let router = self
            .vpc_lookup_router(
                organization_name,
                project_name,
                vpc_name,
                router_name,
            )
            .await?;
        let id = Uuid::new_v4();
        let route =
            db::model::RouterRoute::new(id, router.id(), *kind, params.clone());
        let route = self.db_datastore.router_create_route(route).await?;
        Ok(route)
    }

    pub async fn router_delete_route(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
    ) -> DeleteResult {
        let route = self
            .router_lookup_route(
                organization_name,
                project_name,
                vpc_name,
                router_name,
                route_name,
            )
            .await?;
        // Only custom routes can be deleted
        if route.kind.0 != RouterRouteKind::Custom {
            return Err(Error::MethodNotAllowed {
                internal_message: "DELETE not allowed on system routes"
                    .to_string(),
            });
        }
        self.db_datastore.router_delete_route(&route.id()).await
    }

    pub async fn router_update_route(
        &self,
        organization_name: &Name,
        project_name: &Name,
        vpc_name: &Name,
        router_name: &Name,
        route_name: &Name,
        params: &RouterRouteUpdateParams,
    ) -> UpdateResult<()> {
        let route = self
            .router_lookup_route(
                organization_name,
                project_name,
                vpc_name,
                router_name,
                route_name,
            )
            .await?;
        // TODO: Write a test for this once there's a way to test it (i.e. subnets automatically register to the system router table)
        match route.kind.0 {
            RouterRouteKind::Custom | RouterRouteKind::Default => (),
            _ => {
                return Err(Error::MethodNotAllowed {
                    internal_message: format!(
                        "routes of type {} from the system table of VPC {} are not modifiable",
                        route.kind.0,
                        vpc_name
                    ),
                })
            }
        }
        Ok(self
            .db_datastore
            .router_update_route(&route.id(), params.clone().into())
            .await?)
    }

    /*
     * Racks.  We simulate just one for now.
     */

    fn as_rack(&self) -> db::model::Rack {
        db::model::Rack { identity: self.api_rack_identity.clone() }
    }

    pub async fn racks_list(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<db::model::Rack> {
        if let Some(marker) = pagparams.marker {
            if *marker >= self.rack_id {
                return Ok(futures::stream::empty().boxed());
            }
        }

        Ok(futures::stream::once(ready(Ok(self.as_rack()))).boxed())
    }

    pub async fn rack_lookup(
        &self,
        rack_id: &Uuid,
    ) -> LookupResult<db::model::Rack> {
        if *rack_id == self.rack_id {
            Ok(self.as_rack())
        } else {
            Err(Error::not_found_by_id(ResourceType::Rack, rack_id))
        }
    }

    /*
     * Sleds
     */

    pub async fn sleds_list(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::Sled> {
        self.db_datastore.sled_list(pagparams).await
    }

    pub async fn sled_lookup(
        &self,
        sled_id: &Uuid,
    ) -> LookupResult<db::model::Sled> {
        self.db_datastore.sled_fetch(*sled_id).await
    }

    /*
     * Sagas
     */

    pub async fn sagas_list(
        &self,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResult<external::Saga> {
        /*
         * The endpoint we're serving only supports `ScanById`, which only
         * supports an ascending scan.
         */
        bail_unless!(
            pagparams.direction == dropshot::PaginationOrder::Ascending
        );
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

    pub async fn saga_get(&self, id: Uuid) -> LookupResult<external::Saga> {
        self.sec_client
            .saga_get(steno::SagaId::from(id))
            .await
            .map(external::Saga::from)
            .map(Ok)
            .map_err(|_: ()| {
                Error::not_found_by_id(ResourceType::SagaDbg, &id)
            })?
    }

    /*
     * Built-in users
     */

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
        self.db_datastore.user_builtin_fetch(opctx, name).await
    }

    /*
     * Internal control plane interfaces.
     */

    /**
     * Invoked by a sled agent to publish an updated runtime state for an
     * Instance.
     */
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
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            Ok(false) => {
                info!(log, "instance update from sled agent ignored (old)";
                    "instance_id" => %id);
                Ok(())
            }

            /*
             * If the instance doesn't exist, swallow the error -- there's
             * nothing to do here.
             * TODO-robustness This could only be possible if we've removed an
             * Instance from the datastore altogether.  When would we do that?
             * We don't want to do it as soon as something's destroyed, I think,
             * and in that case, we'd need some async task for cleaning these
             * up.
             */
            Err(Error::ObjectNotFound { .. }) => {
                warn!(log, "non-existent instance updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => %new_runtime_state.run_state);
                Ok(())
            }

            /*
             * If the datastore is unavailable, propagate that to the caller.
             * TODO-robustness Really this should be any _transient_ error.  How
             * can we distinguish?  Maybe datastore should emit something
             * different from Error with an Into<Error>.
             */
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
        id: &Uuid,
        new_state: &DiskRuntimeState,
    ) -> Result<(), Error> {
        let log = &self.log;

        let result = self
            .db_datastore
            .disk_update_runtime(id, &new_state.clone().into())
            .await;

        /* TODO-cleanup commonize with notify_instance_updated() */
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

            /*
             * If the disk doesn't exist, swallow the error -- there's
             * nothing to do here.
             * TODO-robustness This could only be possible if we've removed a
             * disk from the datastore altogether.  When would we do that?
             * We don't want to do it as soon as something's destroyed, I think,
             * and in that case, we'd need some async task for cleaning these
             * up.
             */
            Err(Error::ObjectNotFound { .. }) => {
                warn!(log, "non-existent disk updated by sled agent";
                    "instance_id" => %id,
                    "new_state" => ?new_state);
                Ok(())
            }

            /*
             * If the datastore is unavailable, propagate that to the caller.
             */
            Err(error) => {
                warn!(log, "failed to update disk from sled agent";
                    "disk_id" => %id,
                    "new_state" => ?new_state,
                    "error" => ?error);
                Err(error)
            }
        }
    }

    /**
     * Assign a newly-registered metric producer to an oximeter collector server.
     */
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

    /**
     * Return an oximeter collector to assign a newly-registered producer
     */
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
        token: String,
    ) -> LookupResult<db::model::ConsoleSession> {
        self.db_datastore.session_fetch(token).await
    }

    pub async fn session_create(
        &self,
        user_id: Uuid,
    ) -> CreateResult<db::model::ConsoleSession> {
        let session =
            db::model::ConsoleSession::new(generate_session_token(), user_id);
        Ok(self.db_datastore.session_create(session).await?)
    }

    // update last_used to now
    pub async fn session_update_last_used(
        &self,
        token: String,
    ) -> UpdateResult<db::model::ConsoleSession> {
        Ok(self.db_datastore.session_update_last_used(token).await?)
    }

    pub async fn session_hard_delete(&self, token: String) -> DeleteResult {
        self.db_datastore.session_hard_delete(token).await
    }
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
        let instance = self.db_datastore.instance_fetch(id).await?;
        self.instance_sled(&instance).await
    }

    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let disk = self.db_datastore.disk_fetch(id).await?;
        let instance_id = disk.runtime().attach_instance_id.unwrap();
        let instance = self.db_datastore.instance_fetch(&instance_id).await?;
        self.instance_sled(&instance).await
    }

    async fn session_create_with(
        &self,
        session: db::model::ConsoleSession,
    ) -> CreateResult<db::model::ConsoleSession> {
        Ok(self.db_datastore.session_create(session).await?)
    }
}

lazy_static! {
    static ref DEFAULT_FIREWALL_RULES: external::VpcFirewallRuleUpdateParams =
        serde_json::from_str(r#"{
            "allow-internal-inbound": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "hosts": [ { "type": "vpc", "value": "default" } ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound traffic to all instances within the VPC if originated within the VPC"
            },
            "allow-ssh": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "ports": [ "22" ], "protocols": [ "TCP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound TCP connections on port 22 from anywhere"
            },
            "allow-icmp": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "protocols": [ "ICMP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound ICMP traffic from anywhere"
            },
            "allow-rdp": {
                "status": "enabled",
                "direction": "inbound",
                "targets": [ { "type": "vpc", "value": "default" } ],
                "filters": { "ports": [ "3389" ], "protocols": [ "TCP" ] },
                "action": "allow",
                "priority": 65534,
                "description": "allow inbound TCP connections on port 3389 from anywhere"
            }
        }"#).unwrap();
}
