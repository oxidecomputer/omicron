// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_lookup::lookup;
use nexus_db_model::Probe;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::params;
use nexus_types::external_api::shared::ProbeInfo;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::{
    CreateResult, DeleteResult, ListResultVec, LookupResult, NameOrId,
    http_pagination::PaginatedBy,
};

impl super::Nexus {
    /// List the probes in the given project.
    pub(crate) async fn probe_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ProbeInfo> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore.probe_list(opctx, &authz_project, pagparams).await
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
        self.db_datastore.probe_get(opctx, &authz_project, &name_or_id).await
    }

    /// Create a probe.
    ///
    /// This adds the probe to the data store, sets up the NAT state on the
    /// swtich, and notifies the sled-agent about the new probe.
    pub(crate) async fn probe_create(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        new_probe_params: &params::ProbeCreate,
    ) -> CreateResult<Probe> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        // resolve NameOrId into authz::IpPool
        let pool = match &new_probe_params.ip_pool {
            Some(pool) => Some(
                self.ip_pool_lookup(opctx, &pool)
                    .lookup_for(authz::Action::CreateChild)
                    .await?
                    .0,
            ),
            None => None,
        };

        let new_probe =
            Probe::from_create(new_probe_params, authz_project.id());
        let probe = self
            .db_datastore
            .probe_create(opctx, &authz_project, &new_probe, pool)
            .await?;

        let (.., sled) =
            self.sled_lookup(opctx, &new_probe_params.sled)?.fetch().await?;

        let boundary_switches =
            self.boundary_switches(&self.opctx_alloc).await?;

        for switch in &boundary_switches {
            let dpd_clients = self.dpd_clients().await.map_err(|e| {
                Error::internal_error(&format!(
                    "failed to get dpd_clients: {e}"
                ))
            })?;

            let dpd_client = dpd_clients.get(switch).ok_or_else(|| {
                Error::internal_error(&format!(
                    "could not find dpd client for {switch}"
                ))
            })?;

            self.probe_ensure_dpd_config(
                opctx,
                probe.id(),
                sled.ip.into(),
                None,
                dpd_client,
            )
            .await?;
        }
        self.background_tasks.task_probe_distributor.activate();

        Ok(probe)
    }

    /// Delete a probe.
    ///
    /// This deletes the probe from the data store, tears down the associated
    /// NAT state, and tells the sled-agent to delete the probe zone.
    pub(crate) async fn probe_delete(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        name_or_id: NameOrId,
    ) -> DeleteResult {
        let probe = self.probe_get(opctx, project_lookup, &name_or_id).await?;
        self.probe_delete_dpd_config(opctx, probe.id).await?;
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;
        self.db_datastore
            .probe_delete(opctx, &authz_project, &name_or_id)
            .await?;
        self.background_tasks.task_probe_distributor.activate();
        Ok(())
    }
}
