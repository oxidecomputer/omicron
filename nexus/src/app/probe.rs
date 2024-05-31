use crate::app::instance_network::InstanceNetwork;
use crate::app::ip_pool::IpPool;
use crate::app::sled::Sled;
use internal_dns::resolver::Resolver;
use nexus_db_model::Probe as ModelProbe;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::ProbeInfo;
use nexus_db_queries::db::lookup;
use nexus_db_queries::{authz, db};
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::{
    http_pagination::PaginatedBy, CreateResult, DataPageParams, DeleteResult,
    ListResultVec, LookupResult, NameOrId,
};
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/// Application level operations on probes
#[derive(Clone)]
pub struct Probe {
    log: Logger,
    datastore: Arc<db::DataStore>,
    sled: Sled,
    ip_pool: IpPool,
    instance_network: InstanceNetwork,
    internal_resolver: Resolver,
    opctx_alloc: OpContext,
}

impl Probe {
    pub fn new(
        log: Logger,
        datastore: Arc<db::DataStore>,
        sled: Sled,
        ip_pool: IpPool,
        instance_network: InstanceNetwork,
        internal_resolver: Resolver,
        opctx_alloc: OpContext,
    ) -> Probe {
        Probe {
            log,
            datastore,
            sled,
            ip_pool,
            instance_network,
            internal_resolver,
            opctx_alloc,
        }
    }

    /// List the probes in the given project.
    pub(crate) async fn probe_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ProbeInfo> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.datastore.probe_list(opctx, &authz_project, pagparams).await
    }

    /// List the probes for the given sled. This is used by sled agents to
    /// determine what probes they should be running.
    pub(crate) async fn probe_list_for_sled(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
        sled: Uuid,
    ) -> ListResultVec<ProbeInfo> {
        self.datastore.probe_list_for_sled(sled, opctx, pagparams).await
    }

    /// Get info about a particular probe.
    pub(crate) async fn probe_get(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        name_or_id: &NameOrId,
    ) -> LookupResult<ProbeInfo> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;
        self.datastore.probe_get(opctx, &authz_project, &name_or_id).await
    }

    /// Create a probe. This adds the probe to the data store and sets up the
    /// NAT state on the switch. Actual launching of the probe is done by the
    /// target sled agent asynchronously.
    pub(crate) async fn probe_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        new_probe_params: &params::ProbeCreate,
    ) -> CreateResult<ModelProbe> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        // resolve NameOrId into authz::IpPool
        let pool = match &new_probe_params.ip_pool {
            Some(pool) => Some(
                self.ip_pool
                    .ip_pool_lookup(opctx, &pool)?
                    .lookup_for(authz::Action::CreateChild)
                    .await?
                    .0,
            ),
            None => None,
        };

        let new_probe =
            ModelProbe::from_create(new_probe_params, authz_project.id());
        let probe = self
            .datastore
            .probe_create(opctx, &authz_project, &new_probe, pool)
            .await?;

        let (.., sled) = self
            .sled
            .sled_lookup(opctx, &new_probe_params.sled)?
            .fetch()
            .await?;

        let boundary_switches =
            self.instance_network.boundary_switches(&self.opctx_alloc).await?;

        for switch in &boundary_switches {
            let dpd_clients =
                super::dpd_clients(&self.internal_resolver, &self.log)
                    .await
                    .map_err(|e| {
                        Error::internal_error(&format!(
                            "failed to get dpd_clients: {e}"
                        ))
                    })?;

            let dpd_client = dpd_clients.get(switch).ok_or_else(|| {
                Error::internal_error(&format!(
                    "could not find dpd client for {switch}"
                ))
            })?;

            self.instance_network
                .probe_ensure_dpd_config(
                    opctx,
                    probe.id(),
                    sled.ip.into(),
                    None,
                    dpd_client,
                )
                .await?;
        }

        Ok(probe)
    }

    /// Delete a probe. This deletes the probe from the data store and tears
    /// down the associated NAT state.
    pub(crate) async fn probe_delete(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        name_or_id: NameOrId,
    ) -> DeleteResult {
        let probe = self.probe_get(opctx, project_lookup, &name_or_id).await?;

        self.instance_network.probe_delete_dpd_config(opctx, probe.id).await?;

        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;
        self.datastore.probe_delete(opctx, &authz_project, &name_or_id).await
    }
}
