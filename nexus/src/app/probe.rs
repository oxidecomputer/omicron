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
use nexus_types::external_api::multicast;
use nexus_types::external_api::probe;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::{
    CreateResult, DeleteResult, IpVersion, ListResultVec, LookupResult,
    NameOrId, http_pagination::PaginatedBy,
};
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use super::MAX_MULTICAST_GROUPS_PER_INSTANCE;
use super::multicast::validate_member_source_ips;

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
        let (to_attach, created_group_ids) = self
            .resolve_probe_multicast_memberships(opctx, new_probe_params)
            .await?;

        let new_probe =
            Probe::from_create(new_probe_params, authz_project.id());
        // The probe row insert and all member attaches run in one transaction
        // inside the datastore, so the probe distributor never sees a committed
        // probe row without its committed member rows. A failed attach aborts
        // the transaction, so no probe row is left behind on error, and any
        // group the resolution implicitly created is rolled back below.
        let probe = match self
            .db_datastore
            .probe_create(
                opctx,
                &authz_project,
                &new_probe,
                pool,
                ip_version.map(Into::into),
                &to_attach,
            )
            .await
        {
            Ok(probe) => probe,
            Err(err) => {
                self.rollback_created_multicast_groups(
                    opctx,
                    &created_group_ids,
                )
                .await;
                return Err(err);
            }
        };

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
    /// This mirrors the validation in [`plan_multicast_group_changes`]
    /// (instance update path) and the cap check in
    /// [`project_create_instance`]. Probes have no membership-mutation
    /// API, so create is the only window to reject duplicates or exceed the
    /// per-parent cap threshold.
    ///
    /// Returns the resolved group IDs paired with the requested source IPs,
    /// alongside the IDs of groups the resolution implicitly created, or
    /// empty vectors when multicast is disabled or no groups were requested.
    /// The borrowed source IPs are tied to `params`, which the caller holds
    /// across the subsequent attach operation. The caller uses the created
    /// IDs to roll back if the probe insert itself fails.
    ///
    /// A rejection after an earlier spec implicitly created its group rolls
    /// that creation back here, mirroring `resolve_multicast_group_specs`
    /// on the instance update path.
    ///
    /// # Errors
    ///
    /// - More than [`MAX_MULTICAST_GROUPS_PER_INSTANCE`] groups requested
    /// - A source list exceeds the per-member cap or holds duplicates
    /// - A group identifier fails to resolve
    /// - The same group appears more than once in the request
    /// - A membership names or resolves to an IPv6 group, which the probe
    ///   joiner does not yet support
    ///
    /// [`plan_multicast_group_changes`]: super::Nexus::plan_multicast_group_changes
    /// [`project_create_instance`]: super::Nexus::project_create_instance
    async fn resolve_probe_multicast_memberships<'a>(
        &self,
        opctx: &OpContext,
        params: &'a probe::ProbeCreate,
    ) -> Result<
        (
            Vec<(MulticastGroupUuid, Option<&'a [IpAddr]>)>,
            Vec<MulticastGroupUuid>,
        ),
        Error,
    > {
        if !self.multicast_enabled() || params.multicast_groups.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        if params.multicast_groups.len() > MAX_MULTICAST_GROUPS_PER_INSTANCE {
            return Err(Error::invalid_request(format!(
                "A probe may not join more than \
                 {MAX_MULTICAST_GROUPS_PER_INSTANCE} multicast groups",
            )));
        }

        // Reject duplicate identifiers before any resolution runs.
        for (idx, spec) in params.multicast_groups.iter().enumerate() {
            if params.multicast_groups[..idx]
                .iter()
                .any(|prior| prior.group == spec.group)
            {
                return Err(Error::invalid_request(
                    "Duplicate multicast group specified in request",
                ));
            }
        }

        let mut created_group_ids = Vec::new();
        let res = async {
            let mut to_attach =
                Vec::with_capacity(params.multicast_groups.len());
            let mut seen =
                HashSet::with_capacity(params.multicast_groups.len());
            for spec in &params.multicast_groups {
                // The probe joiner only pins IPv4 joins today (see the
                // `config/ipv6_scope` TODO in sled-agent's probe manager), so
                // an IPv6 membership would join on whatever interface the
                // kernel selects rather than the probe's OPTE port.
                //
                // For now, we reject the request forms that name IPv6
                // outright, before any implicit group creation can run.
                if matches!(spec.ip_version, Some(IpVersion::V6))
                    || matches!(
                        spec.group,
                        multicast::MulticastGroupIdentifier::Ip(ip)
                            if ip.is_ipv6()
                    )
                {
                    return Err(Error::invalid_request(
                        "probes do not support IPv6 multicast group \
                         memberships",
                    ));
                }

                let source_ips = spec.source_ips.as_deref();
                // Per-member source list shape (count + duplicates), mirroring
                // instance create. The group resolution below checks SSM
                // semantics but not the list shape.
                validate_member_source_ips(source_ips)?;
                // Default the create-by-name pool hint to V4 so implicit
                // creation can never mint an IPv6 group.
                let resolved = self
                    .resolve_multicast_group_identifier_with_sources(
                        opctx,
                        &spec.group,
                        source_ips,
                        spec.ip_version.or(Some(IpVersion::V4)),
                    )
                    .await
                    .map_err(|e| {
                        Error::invalid_request(format!(
                            "failed to resolve multicast group {:?}: {e}",
                            spec.group,
                        ))
                    })?;
                let group_id = resolved.id;
                if resolved.created {
                    created_group_ids.push(group_id);
                }

                // A name or ID identifier can still resolve to an IPv6 group,
                // so check the resolved group's address as well.
                let selector = multicast::MulticastGroupSelector {
                    multicast_group: multicast::MulticastGroupIdentifier::Id(
                        group_id.into_untyped_uuid(),
                    ),
                };
                let (.., db_group) = self
                    .multicast_group_lookup(opctx, &selector)
                    .await?
                    .fetch()
                    .await?;
                if db_group.multicast_ip.ip().is_ipv6() {
                    return Err(Error::invalid_request(
                        "probes do not support IPv6 multicast group \
                         memberships",
                    ));
                }

                if !seen.insert(group_id.into_untyped_uuid()) {
                    return Err(Error::invalid_request(
                        "Duplicate multicast group specified in request",
                    ));
                }
                to_attach.push((group_id, source_ips));
            }
            Ok(to_attach)
        }
        .await;

        match res {
            Ok(to_attach) => Ok((to_attach, created_group_ids)),
            Err(err) => {
                self.rollback_created_multicast_groups(
                    opctx,
                    &created_group_ids,
                )
                .await;
                Err(err)
            }
        }
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
        if self.multicast_enabled() {
            self.background_tasks.task_multicast_reconciler.activate();
        }
        self.background_tasks.task_probe_distributor.activate();
        Ok(())
    }
}
