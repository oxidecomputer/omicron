// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! IP Pools, collections of external IP addresses for guest instances

use crate::external_api::params;
use crate::external_api::shared;
use crate::external_api::views;
use chrono::Utc;
use ipnetwork::IpNetwork;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::IpPool;
use nexus_db_model::IpPoolType;
use nexus_db_model::IpPoolUpdate;
use nexus_db_model::IpVersion;
use nexus_db_queries::authz;
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::model::Name;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;
use std::matches;
use uuid::Uuid;

/// Helper to make it easier to 404 on attempts to manipulate internal pools
fn not_found_from_lookup(pool_lookup: &lookup::IpPool<'_>) -> Error {
    match pool_lookup {
        lookup::IpPool::Name(_, name) => {
            Error::not_found_by_name(ResourceType::IpPool, &name)
        }
        lookup::IpPool::OwnedName(_, name) => {
            Error::not_found_by_name(ResourceType::IpPool, &name)
        }
        lookup::IpPool::PrimaryKey(_, id) => {
            Error::not_found_by_id(ResourceType::IpPool, &id)
        }
        lookup::IpPool::Error(_, error) => error.to_owned(),
    }
}

impl super::Nexus {
    pub fn ip_pool_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
    ) -> LookupResult<lookup::IpPool<'a>> {
        match pool {
            NameOrId::Name(name) => {
                let pool = LookupPath::new(opctx, &self.db_datastore)
                    .ip_pool_name(Name::ref_cast(name));
                Ok(pool)
            }
            NameOrId::Id(id) => {
                let pool =
                    LookupPath::new(opctx, &self.db_datastore).ip_pool_id(*id);
                Ok(pool)
            }
        }
    }

    pub(crate) async fn ip_pool_create(
        &self,
        opctx: &OpContext,
        pool_params: &params::IpPoolCreate,
    ) -> CreateResult<IpPool> {
        // https://github.com/oxidecomputer/omicron/issues/8881
        let ip_version = pool_params.ip_version.into();

        let pool = match (
            pool_params.pool_type.clone(),
            pool_params.switch_port_uplinks.is_some(),
        ) {
            (shared::IpPoolType::Unicast, true) => {
                return Err(Error::invalid_request(
                    "switch_port_uplinks are only allowed for multicast IP pools",
                ));
            }
            (shared::IpPoolType::Unicast, false) => {
                if pool_params.mvlan.is_some() {
                    return Err(Error::invalid_request(
                        "mvlan is only allowed for multicast IP pools",
                    ));
                }
                IpPool::new(&pool_params.identity, ip_version)
            }
            (shared::IpPoolType::Multicast, _) => {
                let switch_port_ids = self
                    .resolve_switch_port_ids(
                        opctx,
                        self.rack_id(),
                        &pool_params.switch_port_uplinks,
                    )
                    .await?;

                IpPool::new_multicast(
                    &pool_params.identity,
                    ip_version,
                    switch_port_ids,
                    pool_params.mvlan,
                )
            }
        };

        self.db_datastore.ip_pool_create(opctx, pool).await
    }

    /// List IP pools in current silo
    pub(crate) async fn current_silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(db::model::IpPool, db::model::IpPoolResource)> {
        let authz_silo =
            opctx.authn.silo_required().internal_context("listing IP pools")?;

        // From the developer user's point of view, we treat IP pools linked to
        // their silo as silo resources, so they can list them if they can list
        // silo children
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        self.db_datastore.silo_ip_pool_list(opctx, &authz_silo, pagparams).await
    }

    /// Look up linked pool by name or ID. 404 on pools that exist but aren't
    /// linked to the current silo. Special logic to make sure non-fleet users
    /// can read the pool.
    pub async fn silo_ip_pool_fetch<'a>(
        &'a self,
        opctx: &'a OpContext,
        pool: &'a NameOrId,
    ) -> LookupResult<(
        authz::IpPool,
        db::model::IpPool,
        db::model::IpPoolResource,
    )> {
        let (authz_pool, pool) = self
            .ip_pool_lookup(opctx, pool)?
            // TODO-robustness: https://github.com/oxidecomputer/omicron/issues/3995
            // Checking CreateChild works because it is the permission for
            // allocating IPs from a pool, which any authenticated user has.
            // But what we really want to say is that any authenticated user
            // has actual Read permission on any IP pool linked to their silo.
            // Instead we are backing into this with the next line: never fail
            // this auth check as long as you're authed, then 404 if unlinked.
            // This is not a correctness issue per se because the logic as-is is
            // correct. The main problem is that it is fiddly to get right and
            // has to be done manually each time.
            .fetch_for(authz::Action::CreateChild)
            .await?;

        // 404 if no link is found in the current silo
        let link = self.db_datastore.ip_pool_fetch_link(opctx, pool.id()).await;
        match link {
            Ok(link) => Ok((authz_pool, pool, link)),
            Err(_) => Err(authz_pool.not_found()),
        }
    }

    /// List silos for a given pool
    pub(crate) async fn ip_pool_silo_list(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::IpPoolResource> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::ListChildren).await?;

        // check ability to list silos in general
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        self.db_datastore.ip_pool_silo_list(opctx, &authz_pool, pagparams).await
    }

    // List pools for a given silo
    pub(crate) async fn silo_ip_pool_list(
        &self,
        opctx: &OpContext,
        silo_lookup: &lookup::Silo<'_>,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<(db::model::IpPool, db::model::IpPoolResource)> {
        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Read).await?;
        // check ability to list pools in general
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        self.db_datastore.silo_ip_pool_list(opctx, &authz_silo, pagparams).await
    }

    pub(crate) async fn ip_pool_link_silo(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_link: &params::IpPoolLinkSilo,
    ) -> CreateResult<db::model::IpPoolResource> {
        let (authz_pool,) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        let (authz_silo,) = self
            .silo_lookup(&opctx, silo_link.silo.clone())?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .ip_pool_link_silo(
                opctx,
                db::model::IpPoolResource {
                    ip_pool_id: authz_pool.id(),
                    resource_type: db::model::IpPoolResourceType::Silo,
                    resource_id: authz_silo.id(),
                    is_default: silo_link.is_default,
                },
            )
            .await
    }

    pub(crate) async fn ip_pool_unlink_silo(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_lookup: &lookup::Silo<'_>,
    ) -> DeleteResult {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;

        self.db_datastore
            .ip_pool_unlink_silo(opctx, &authz_pool, &authz_silo)
            .await
    }

    pub(crate) async fn ip_pool_silo_update(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        silo_lookup: &lookup::Silo<'_>,
        update: &params::IpPoolSiloUpdate,
    ) -> CreateResult<db::model::IpPoolResource> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        let (.., authz_silo) =
            silo_lookup.lookup_for(authz::Action::Modify).await?;

        self.db_datastore
            .ip_pool_set_default(
                opctx,
                &authz_pool,
                &authz_silo,
                update.is_default,
            )
            .await
    }

    pub(crate) async fn ip_pools_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<db::model::IpPool> {
        self.db_datastore.ip_pools_list(opctx, pagparams).await
    }

    pub(crate) async fn ip_pool_delete(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
    ) -> DeleteResult {
        let (.., authz_pool, db_pool) =
            pool_lookup.fetch_for(authz::Action::Delete).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        self.db_datastore.ip_pool_delete(opctx, &authz_pool, &db_pool).await
    }

    pub(crate) async fn ip_pool_update(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        updates: &params::IpPoolUpdate,
    ) -> UpdateResult<db::model::IpPool> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::Modify).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        let switch_port_ids = self
            .resolve_switch_port_ids(
                opctx,
                self.rack_id(),
                &updates.switch_port_uplinks,
            )
            .await?;

        let updates_db = IpPoolUpdate {
            name: updates.identity.name.clone().map(Into::into),
            description: updates.identity.description.clone(),
            switch_port_uplinks: switch_port_ids,
            mvlan: updates.mvlan.map(|vid| u16::from(vid).into()),
            time_modified: Utc::now(),
        };

        self.db_datastore.ip_pool_update(opctx, &authz_pool, updates_db).await
    }

    pub(crate) async fn ip_pool_list_ranges(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        let (.., authz_pool) =
            pool_lookup.lookup_for(authz::Action::ListChildren).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub(crate) async fn ip_pool_add_range(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        range: &shared::IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (.., authz_pool, db_pool) =
            pool_lookup.fetch_for(authz::Action::Modify).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        // Disallow V6 ranges until IPv6 is fully supported by the networking
        // subsystem. Instead of changing the API to reflect that (making this
        // endpoint inconsistent with the rest) and changing it back when we
        // add support, we accept them at the API layer and error here. It
        // would be nice if we could do it in the datastore layer, but we'd
        // have no way of creating IPv6 ranges for the purpose of testing IP
        // pool utilization.
        //
        // See https://github.com/oxidecomputer/omicron/issues/8761.
        if matches!(range, shared::IpRange::V6(_)) {
            return Err(Error::invalid_request(
                "IPv6 ranges are not allowed yet",
            ));
        }

        let range_is_multicast = match range {
            shared::IpRange::V4(v4_range) => {
                let first = v4_range.first_address();
                let last = v4_range.last_address();
                first.is_multicast() && last.is_multicast()
            }
            shared::IpRange::V6(v6_range) => {
                let first = v6_range.first_address();
                let last = v6_range.last_address();
                first.is_multicast() && last.is_multicast()
            }
        };

        match db_pool.pool_type {
            IpPoolType::Multicast => {
                if !range_is_multicast {
                    return Err(Error::invalid_request(
                        "Cannot add unicast address range to multicast IP pool",
                    ));
                }

                // For multicast pools, validate ASM/SSM separation
                // This validation is done in the datastore layer
            }
            IpPoolType::Unicast => {
                if range_is_multicast {
                    return Err(Error::invalid_request(
                        "Cannot add multicast address range to unicast IP pool",
                    ));
                }
            }
        }

        self.db_datastore
            .ip_pool_add_range(opctx, &authz_pool, &db_pool, range)
            .await
    }

    pub(crate) async fn ip_pool_delete_range(
        &self,
        opctx: &OpContext,
        pool_lookup: &lookup::IpPool<'_>,
        range: &shared::IpRange,
    ) -> DeleteResult {
        let (.., authz_pool, _db_pool) =
            pool_lookup.fetch_for(authz::Action::Modify).await?;

        if self.db_datastore.ip_pool_is_internal(opctx, &authz_pool).await? {
            return Err(not_found_from_lookup(pool_lookup));
        }

        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }

    // The "ip_pool_service_..." functions look up IP pools for Oxide service usage,
    // rather than for VMs.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/1276): Should be
    // accessed via AZ UUID, probably.

    pub(crate) async fn ip_pool_service_fetch(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<db::model::IpPool> {
        // TODO: https://github.com/oxidecomputer/omicron/issues/8881
        let (authz_pool, db_pool) = self
            .db_datastore
            .ip_pools_service_lookup(opctx, IpVersion::V4)
            .await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        Ok(db_pool)
    }

    pub(crate) async fn ip_pool_service_list_ranges(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, IpNetwork>,
    ) -> ListResultVec<db::model::IpPoolRange> {
        // TODO: https://github.com/oxidecomputer/omicron/issues/8881
        let (authz_pool, ..) = self
            .db_datastore
            .ip_pools_service_lookup(opctx, IpVersion::V4)
            .await?;
        opctx.authorize(authz::Action::Read, &authz_pool).await?;
        self.db_datastore
            .ip_pool_list_ranges(opctx, &authz_pool, pagparams)
            .await
    }

    pub(crate) async fn ip_pool_service_add_range(
        &self,
        opctx: &OpContext,
        range: &shared::IpRange,
    ) -> UpdateResult<db::model::IpPoolRange> {
        let (authz_pool, db_pool) = self
            .db_datastore
            .ip_pools_service_lookup(opctx, range.version().into())
            .await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;

        // Disallow V6 ranges until IPv6 is fully supported by the networking
        // subsystem. Instead of changing the API to reflect that (making this
        // endpoint inconsistent with the rest) and changing it back when we
        // add support, we accept them at the API layer and error here. It
        // would be nice if we could do it in the datastore layer, but we'd
        // have no way of creating IPv6 ranges for the purpose of testing IP
        // pool utilization.
        //
        // See https://github.com/oxidecomputer/omicron/issues/8761.
        if matches!(range, shared::IpRange::V6(_)) {
            return Err(Error::invalid_request(
                "IPv6 ranges are not allowed yet",
            ));
        }

        // Validate that the range matches the pool type
        let range_is_multicast = match range {
            shared::IpRange::V4(v4_range) => {
                let first = v4_range.first_address();
                let last = v4_range.last_address();
                first.is_multicast() && last.is_multicast()
            }
            shared::IpRange::V6(v6_range) => {
                let first = v6_range.first_address();
                let last = v6_range.last_address();
                first.is_multicast() && last.is_multicast()
            }
        };

        match db_pool.pool_type {
            IpPoolType::Multicast => {
                if !range_is_multicast {
                    return Err(Error::invalid_request(
                        "Cannot add unicast address range to multicast IP pool",
                    ));
                }
            }
            IpPoolType::Unicast => {
                if range_is_multicast {
                    return Err(Error::invalid_request(
                        "Cannot add multicast address range to unicast IP pool",
                    ));
                }
            }
        }

        self.db_datastore
            .ip_pool_add_range(opctx, &authz_pool, &db_pool, range)
            .await
    }

    pub(crate) async fn ip_pool_service_delete_range(
        &self,
        opctx: &OpContext,
        range: &shared::IpRange,
    ) -> DeleteResult {
        let (authz_pool, ..) = self
            .db_datastore
            .ip_pools_service_lookup(opctx, range.version().into())
            .await?;
        opctx.authorize(authz::Action::Modify, &authz_pool).await?;
        self.db_datastore.ip_pool_delete_range(opctx, &authz_pool, range).await
    }

    async fn resolve_switch_port_ids(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        uplinks: &Option<Vec<params::SwitchPortUplink>>,
    ) -> Result<Option<Vec<Uuid>>, Error> {
        match uplinks {
            None => Ok(None),
            Some(list) => {
                let mut ids = Vec::with_capacity(list.len());

                for uplink in list {
                    let switch_location =
                        Name::from(uplink.switch_location.clone());
                    let port_name = Name::from(uplink.port_name.clone());
                    let id = self
                        .db_datastore
                        .switch_port_get_id(
                            opctx,
                            rack_id,
                            switch_location,
                            port_name,
                        )
                        .await
                        .map_err(|_| {
                            Error::invalid_value(
                                "switch_port_uplinks",
                                format!("Switch port '{}' not found", uplink),
                            )
                        })?;
                    ids.push(id);
                }
                Ok(Some(ids))
            }
        }
    }

    /// Convert IP pool with proper switch port name resolution in an async
    /// context.
    pub(crate) async fn ip_pool_to_view(
        &self,
        opctx: &OpContext,
        pool: db::model::IpPool,
    ) -> Result<views::IpPool, Error> {
        let identity = pool.identity();
        let pool_type = pool.pool_type;

        // Convert switch port UUIDs to "switch.port" format
        let switch_port_uplinks = self
            .resolve_switch_port_names(opctx, &pool.switch_port_uplinks)
            .await?;

        let mvlan = pool.mvlan.map(|vlan| vlan.into());

        Ok(views::IpPool {
            identity,
            ip_version: pool.ip_version.into(),
            pool_type: pool_type.into(),
            switch_port_uplinks,
            mvlan,
        })
    }

    // Convert switch port UUIDs to "switch.port" format for views
    async fn resolve_switch_port_names(
        &self,
        opctx: &OpContext,
        switch_port_ids: &Option<Vec<Uuid>>,
    ) -> Result<Option<Vec<String>>, Error> {
        match switch_port_ids {
            None => Ok(None),
            Some(ids) => {
                let mut names = Vec::with_capacity(ids.len());
                for &id in ids {
                    let switch_port = self
                        .db_datastore
                        .switch_port_get(opctx, id)
                        .await
                        .map_err(|_| {
                            Error::internal_error(&format!(
                                "Switch port with ID {} not found",
                                id
                            ))
                        })?;
                    let name = format!(
                        "{}.{}",
                        switch_port.switch_location, switch_port.port_name
                    );
                    names.push(name);
                }
                Ok(Some(names))
            }
        }
    }
}
