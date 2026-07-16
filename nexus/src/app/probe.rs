// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashSet;
use std::net::IpAddr;

use nexus_db_lookup::lookup;
use nexus_db_model::Probe;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::ip_pool;
use nexus_types::external_api::probe;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::{
    CreateResult, DeleteResult, ListResultVec, LookupResult, NameOrId,
    http_pagination::PaginatedBy,
};
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid, ProbeUuid};

use super::MAX_MULTICAST_GROUPS_PER_INSTANCE;

impl super::Nexus {
    /// List the probes in the given project.
    pub(crate) async fn probe_list(
        &self,
        opctx: &OpContext,
        project_lookup: &lookup::Project<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<probe::ProbeInfo> {
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
    ) -> LookupResult<probe::ProbeInfo> {
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
        new_probe_params: &probe::ProbeCreate,
    ) -> CreateResult<Probe> {
        let (.., authz_project) =
            project_lookup.lookup_for(authz::Action::CreateChild).await?;

        // Destructure pool_selector to get pool and ip_version
        let (pool, ip_version) = match &new_probe_params.pool_selector {
            ip_pool::PoolSelector::Explicit { pool } => {
                (Some(pool.clone()), None)
            }
            ip_pool::PoolSelector::Auto { ip_version } => (None, *ip_version),
        };

        // resolve NameOrId into authz::IpPool
        let pool = match pool {
            Some(pool) => Some(
                self.ip_pool_lookup(opctx, &pool)?
                    .lookup_for(authz::Action::CreateChild)
                    .await?
                    .0,
            ),
            None => None,
        };

        // Resolve and validate the requested multicast memberships before
        // inserting the probe row so a rejected request does not leave an
        // orphaned probe behind.
        let to_attach = self
            .resolve_probe_multicast_memberships(opctx, new_probe_params)
            .await?;

        let new_probe =
            Probe::from_create(new_probe_params, authz_project.id());
        // The probe row insert and all member attaches run in one transaction
        // inside the datastore, so the probe distributor never sees a committed
        // probe row without its committed member rows. A failed attach aborts
        // the transaction, so no probe row is left behind on error.
        let probe = self
            .db_datastore
            .probe_create(
                opctx,
                &authz_project,
                &new_probe,
                pool,
                ip_version.map(Into::into),
                &to_attach,
            )
            .await?;

        if !to_attach.is_empty() {
            self.background_tasks.task_multicast_reconciler.activate();
        }

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
                    "could not find dpd client for {switch:?}"
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

    /// Resolve and validate the multicast memberships requested for a new
    /// probe.
    ///
    /// Validation runs before the probe row is inserted so a rejected request
    /// does not leave an orphaned probe behind.
    ///
    /// This mirrors the validation in [`handle_multicast_group_changes`]
    /// (instance update path) and the cap check in
    /// [`project_create_instance`]. Probes have no membership-mutation
    /// API, so create is the only window to reject duplicates or exceed the
    /// per-parent cap threshold.
    ///
    /// # Returns
    ///
    /// The resolved group IDs paired with their source IPs, or an empty vector
    /// when multicast is disabled or no groups were requested. The borrowed
    /// source IPs are tied to `params`, which the caller holds across the
    /// subsequent attach operation.
    ///
    /// # Errors
    ///
    /// - More than [`MAX_MULTICAST_GROUPS_PER_INSTANCE`] groups requested
    /// - A group identifier fails to resolve
    /// - The same group appears more than once in the request
    ///
    /// [`handle_multicast_group_changes`]: super::Nexus::handle_multicast_group_changes
    /// [`project_create_instance`]: super::Nexus::project_create_instance
    async fn resolve_probe_multicast_memberships<'a>(
        &self,
        opctx: &OpContext,
        params: &'a probe::ProbeCreate,
    ) -> Result<Vec<(MulticastGroupUuid, Option<&'a [IpAddr]>)>, Error> {
        if !self.multicast_enabled() || params.multicast_groups.is_empty() {
            return Ok(Vec::new());
        }

        if params.multicast_groups.len() > MAX_MULTICAST_GROUPS_PER_INSTANCE {
            return Err(Error::invalid_request(format!(
                "A probe may not join more than \
                 {MAX_MULTICAST_GROUPS_PER_INSTANCE} multicast groups",
            )));
        }

        let mut to_attach = Vec::with_capacity(params.multicast_groups.len());
        let mut seen = HashSet::with_capacity(params.multicast_groups.len());
        for spec in &params.multicast_groups {
            let source_ips = spec.source_ips.as_deref();
            let group_id = self
                .resolve_multicast_group_identifier_with_sources(
                    opctx,
                    &spec.group,
                    source_ips,
                    spec.ip_version,
                )
                .await
                .map_err(|e| {
                    Error::invalid_request(format!(
                        "failed to resolve multicast group {:?}: {e}",
                        spec.group,
                    ))
                })?;
            if !seen.insert(group_id.into_untyped_uuid()) {
                return Err(Error::invalid_request(
                    "Duplicate multicast group specified in request",
                ));
            }
            to_attach.push((group_id, source_ips));
        }
        Ok(to_attach)
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
        // Mark memberships for permanent removal (sets `time_deleted`)
        // before soft-deleting the probe, mirroring the `instance_delete`
        // saga. The probe-"Left" reconciler finalizes dataplane teardown
        // on rows with `time_deleted` set.
        if self.multicast_enabled() {
            self.db_datastore
                .multicast_group_members_mark_for_removal_by_parent(
                    opctx,
                    nexus_db_model::MemberParentRef::Probe(
                        ProbeUuid::from_untyped_uuid(probe.id),
                    ),
                )
                .await?;
            self.background_tasks.task_multicast_reconciler.activate();
        }
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
