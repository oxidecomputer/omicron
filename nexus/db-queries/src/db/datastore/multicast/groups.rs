// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group management and IP allocation.
//!
//! This module provides database operations for multicast groups following
//! the bifurcated design from [RFD 488](https://rfd.shared.oxide.computer/rfd/488):
//!
//! - External groups: External-facing, allocated from IP pools, involving
//!   operators.
//! - Underlay groups: System-generated admin-scoped IPv6 multicast groups.

use std::net::IpAddr;

use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::{
    DatabaseErrorKind::UniqueViolation,
    Error::{DatabaseError, NotFound},
};
use ipnetwork::IpNetwork;
use ref_cast::RefCast;
use slog::{error, info};
use uuid::Uuid;

use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_lookup::DbConnection;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult,
    IdentityMetadataCreateParams, ListResultVec, LookupResult, LookupType,
    ResourceType, UpdateResult,
};
use omicron_common::vlan::VlanID;
use omicron_uuid_kinds::{GenericUuid, MulticastGroupUuid};

use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::DataStore;
use crate::db::model::{
    ExternalMulticastGroup, ExternalMulticastGroupUpdate,
    IncompleteExternalMulticastGroup, IncompleteExternalMulticastGroupParams,
    IpPoolType, MulticastGroup, MulticastGroupState, Name,
    UnderlayMulticastGroup, Vni,
};
use crate::db::pagination::paginated;
use crate::db::queries::external_multicast_group::NextExternalMulticastGroup;
use crate::db::update_and_check::{UpdateAndCheck, UpdateStatus};

/// Parameters for multicast group allocation.
#[derive(Debug, Clone)]
pub(crate) struct MulticastGroupAllocationParams {
    pub identity: IdentityMetadataCreateParams,
    pub ip: Option<IpAddr>,
    pub pool: Option<authz::IpPool>,
    pub source_ips: Option<Vec<IpAddr>>,
    pub mvlan: Option<VlanID>,
}

impl DataStore {
    /// List multicast groups by state.
    pub async fn multicast_groups_list_by_state(
        &self,
        opctx: &OpContext,
        state: MulticastGroupState,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

        paginated(dsl::multicast_group, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::state.eq(state))
            .select(MulticastGroup::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Set multicast group state.
    pub async fn multicast_group_set_state(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
        new_state: MulticastGroupState,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::multicast_group::dsl;

        let rows_updated = diesel::update(dsl::multicast_group)
            .filter(dsl::id.eq(group_id))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(new_state),
                dsl::time_modified.eq(diesel::dsl::now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if rows_updated == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroup,
                &group_id,
            ));
        }

        Ok(())
    }

    /// Allocate a new external multicast group.
    pub async fn multicast_group_create(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        params: &params::MulticastGroupCreate,
        authz_pool: Option<authz::IpPool>,
    ) -> CreateResult<ExternalMulticastGroup> {
        self.allocate_external_multicast_group(
            opctx,
            rack_id,
            MulticastGroupAllocationParams {
                identity: params.identity.clone(),
                ip: params.multicast_ip,
                pool: authz_pool,
                source_ips: params.source_ips.clone(),
                mvlan: params.mvlan,
            },
        )
        .await
    }

    /// Fetch an external multicast group by ID.
    pub async fn multicast_group_fetch(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> LookupResult<ExternalMulticastGroup> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.multicast_group_fetch_on_conn(
            opctx,
            &conn,
            group_id.into_untyped_uuid(),
        )
        .await
    }

    /// Fetch an external multicast group using provided connection.
    pub async fn multicast_group_fetch_on_conn(
        &self,
        _opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<nexus_db_lookup::DbConnection>,
        group_id: Uuid,
    ) -> LookupResult<ExternalMulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

        dsl::multicast_group
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(group_id))
            .select(ExternalMulticastGroup::as_select())
            .first_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    /// Check if an external multicast group is active.
    pub(crate) async fn multicast_group_is_active(
        &self,
        conn: &async_bb8_diesel::Connection<nexus_db_lookup::DbConnection>,
        group_id: Uuid,
    ) -> LookupResult<bool> {
        use nexus_db_schema::schema::multicast_group::dsl;

        let state = dsl::multicast_group
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(group_id))
            .select(dsl::state)
            .first_async::<MulticastGroupState>(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(state == MulticastGroupState::Active)
    }

    /// Lookup an external multicast group by IP address.
    pub async fn multicast_group_lookup_by_ip(
        &self,
        opctx: &OpContext,
        ip_addr: IpAddr,
    ) -> LookupResult<ExternalMulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

        dsl::multicast_group
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::multicast_ip.eq(IpNetwork::from(ip_addr)))
            .select(ExternalMulticastGroup::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ByName(ip_addr.to_string()),
                    ),
                )
            })
    }

    /// List multicast groups (fleet-wide).
    pub async fn multicast_groups_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ExternalMulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::multicast_group, dsl::id, pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::multicast_group,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .select(ExternalMulticastGroup::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Update a multicast group.
    pub async fn multicast_group_update(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
        params: &params::MulticastGroupUpdate,
    ) -> UpdateResult<ExternalMulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

        // Create update struct with mvlan=None (won't update field)
        let mut update = ExternalMulticastGroupUpdate::from(params.clone());

        // Handle mvlan manually like VpcSubnetUpdate handles custom_router_id
        // - None: leave as None (don't update field)
        // - Some(Nullable(Some(v))): set to update field to value
        // - Some(Nullable(None)): set to update field to NULL
        if let Some(mvlan) = &params.mvlan {
            update.mvlan = Some(mvlan.0.map(|vlan| u16::from(vlan) as i16));
        }

        diesel::update(dsl::multicast_group)
            .filter(dsl::id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .set(update)
            .returning(ExternalMulticastGroup::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    /// Mark a multicast group for soft deletion.
    ///
    /// Sets the `time_deleted` timestamp on the group, preventing it from
    /// appearing in normal queries. The group remains in the database
    /// until it's cleaned up by a background task.
    pub async fn mark_multicast_group_for_removal(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group::dsl;
        let now = Utc::now();

        diesel::update(dsl::multicast_group)
            .filter(dsl::id.eq(group_id))
            .filter(
                dsl::state
                    .eq(MulticastGroupState::Active)
                    .or(dsl::state.eq(MulticastGroupState::Creating)),
            )
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupState::Deleting),
                dsl::time_modified.eq(now),
            ))
            .returning(ExternalMulticastGroup::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(())
    }

    /// Delete a multicast group permanently.
    pub async fn multicast_group_delete(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group::dsl;

        diesel::delete(dsl::multicast_group)
            .filter(dsl::id.eq(group_id.into_untyped_uuid()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }

    /// Allocate an external multicast group from an IP Pool.
    ///
    /// The rack_id should come from the requesting nexus instance (the rack
    /// that received the API request).
    pub(crate) async fn allocate_external_multicast_group(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        params: MulticastGroupAllocationParams,
    ) -> CreateResult<ExternalMulticastGroup> {
        let group_id = Uuid::new_v4();
        let authz_pool = self
            .resolve_pool_for_allocation(
                opctx,
                params.pool,
                IpPoolType::Multicast,
            )
            .await?;

        // Enforce ASM/SSM semantics when allocating from a pool:
        // - If sources are provided without an explicit IP (implicit allocation),
        //   the pool must be SSM so we allocate an SSM address.
        // - If the pool is SSM and sources are empty/missing, reject.
        let sources_empty =
            params.source_ips.as_ref().map(|v| v.is_empty()).unwrap_or(true);

        let pool_is_ssm =
            self.multicast_pool_is_ssm(opctx, authz_pool.id()).await?;

        if !sources_empty && params.ip.is_none() && !pool_is_ssm {
            let pool_id = authz_pool.id();
            return Err(external::Error::invalid_request(&format!(
                "Cannot allocate SSM multicast group from ASM pool {pool_id}. Choose a multicast pool with SSM ranges (IPv4 232/8, IPv6 FF3x::/32) or provide an explicit SSM address."
            )));
        }

        if sources_empty && pool_is_ssm {
            let pool_id = authz_pool.id();
            return Err(external::Error::invalid_request(&format!(
                "SSM multicast pool {pool_id} requires one or more source IPs"
            )));
        }

        // Prepare source IPs from params if provided
        let source_ip_networks: Vec<IpNetwork> = params
            .source_ips
            .as_ref()
            .map(|source_ips| {
                source_ips.iter().map(|ip| IpNetwork::from(*ip)).collect()
            })
            .unwrap_or_default();

        // Fleet-scoped multicast groups always use DEFAULT_MULTICAST_VNI (77).
        // This reserved VNI is below MIN_GUEST_VNI (1024) and provides consistent
        // behavior across all multicast groups. VNI is not derived from VPC since
        // groups are fleet-wide and can span multiple projects/VPCs.
        let vni = Vni(external::Vni::DEFAULT_MULTICAST_VNI);

        // Create the incomplete group
        let data = IncompleteExternalMulticastGroup::new(
            IncompleteExternalMulticastGroupParams {
                id: group_id,
                name: Name(params.identity.name.clone()),
                description: params.identity.description.clone(),
                ip_pool_id: authz_pool.id(),
                rack_id,
                explicit_address: params.ip,
                source_ips: source_ip_networks,
                mvlan: params.mvlan.map(|vlan_id| u16::from(vlan_id) as i16),
                vni,
                // Set tag to group name for lifecycle management
                tag: Some(params.identity.name.to_string()),
            },
        );

        let conn = self.pool_connection_authorized(opctx).await?;
        Self::allocate_external_multicast_group_on_conn(&conn, data).await
    }

    /// Allocate an external multicast group using provided connection.
    pub(crate) async fn allocate_external_multicast_group_on_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        data: IncompleteExternalMulticastGroup,
    ) -> Result<ExternalMulticastGroup, external::Error> {
        let name = data.name.to_string();
        let explicit_ip = data.explicit_address.is_some();

        NextExternalMulticastGroup::new(data).get_result_async(conn).await.map_err(|e| {
            match e {
                NotFound => {
                    if explicit_ip {
                        external::Error::invalid_request(
                            "Requested multicast IP address is not available in the specified pool range",
                        )
                    } else {
                        external::Error::insufficient_capacity(
                            "No multicast IP addresses available",
                            "NextExternalMulticastGroup::new returned NotFound",
                        )
                    }
                }
                // Multicast group: name conflict
                DatabaseError(UniqueViolation, ..) => {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::MulticastGroup,
                            &name,
                        ),
                    )
                }
                _ => {
                    crate::db::queries::external_multicast_group::from_diesel(e)
                }
            }
        })
    }

    /// Deallocate an external multicast group address.
    ///
    /// Returns `Ok(true)` if the group was deallocated, `Ok(false)` if it was
    /// already deleted, `Err(_)` for any other condition including non-existent
    /// record.
    pub async fn deallocate_external_multicast_group(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
    ) -> Result<bool, external::Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.deallocate_external_multicast_group_on_conn(&conn, group_id).await
    }

    /// Transaction-safe variant of deallocate_external_multicast_group.
    pub(crate) async fn deallocate_external_multicast_group_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        group_id: Uuid,
    ) -> Result<bool, external::Error> {
        use nexus_db_schema::schema::multicast_group::dsl;

        let now = Utc::now();
        let result = diesel::update(dsl::multicast_group)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(group_id))
            .set(dsl::time_deleted.eq(now))
            .check_if_exists::<ExternalMulticastGroup>(group_id)
            .execute_and_check(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(match result.status {
            UpdateStatus::Updated => true,
            UpdateStatus::NotUpdatedButExists => false,
        })
    }

    /// Ensure an underlay multicast group exists for an external multicast
    /// group.
    pub async fn ensure_underlay_multicast_group(
        &self,
        opctx: &OpContext,
        external_group: MulticastGroup,
        multicast_ip: IpNetwork,
        vni: Vni,
    ) -> CreateResult<UnderlayMulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl as external_dsl;
        use nexus_db_schema::schema::underlay_multicast_group::dsl as underlay_dsl;

        let external_group_id = external_group.id();
        let tag = external_group.tag;

        // Try to create new underlay multicast group, or get existing one if concurrent creation
        let underlay_group = match diesel::insert_into(
            underlay_dsl::underlay_multicast_group,
        )
        .values((
            underlay_dsl::id.eq(Uuid::new_v4()),
            underlay_dsl::time_created.eq(Utc::now()),
            underlay_dsl::time_modified.eq(Utc::now()),
            underlay_dsl::multicast_ip.eq(multicast_ip),
            underlay_dsl::vni.eq(vni),
            underlay_dsl::tag.eq(tag.clone()),
        ))
        .returning(UnderlayMulticastGroup::as_returning())
        .get_result_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        {
            Ok(created_group) => {
                info!(
                    opctx.log,
                    "Created new underlay multicast group";
                    "group_id" => %created_group.id,
                    "multicast_ip" => %multicast_ip,
                    "vni" => u32::from(vni.0)
                );
                created_group
            }
            Err(e) => match e {
                DatabaseError(UniqueViolation, ..) => {
                    // Concurrent creation - fetch the existing group
                    // This is expected behavior for idempotent operations
                    info!(
                        opctx.log,
                        "Concurrent underlay multicast group creation detected, fetching existing";
                        "multicast_ip" => %multicast_ip,
                        "vni" => u32::from(vni.0)
                    );

                    underlay_dsl::underlay_multicast_group
                        .filter(underlay_dsl::multicast_ip.eq(multicast_ip))
                        .filter(underlay_dsl::time_deleted.is_null())
                        .first_async::<UnderlayMulticastGroup>(
                            &*self.pool_connection_authorized(opctx).await?,
                        )
                        .await
                        .map_err(|e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        })?
                }
                _ => {
                    error!(
                        opctx.log,
                        "Failed to create underlay multicast group";
                        "error" => ?e,
                        "multicast_ip" => %multicast_ip,
                        "vni" => u32::from(vni.0),
                        "tag" => ?tag
                    );
                    return Err(public_error_from_diesel(
                        e,
                        ErrorHandler::Server,
                    ));
                }
            },
        };

        // Link the external group to the underlay group if not already linked
        // This makes the function truly idempotent
        if external_group.underlay_group_id != Some(underlay_group.id) {
            diesel::update(external_dsl::multicast_group)
                .filter(external_dsl::id.eq(external_group_id))
                .filter(external_dsl::time_deleted.is_null())
                .set(external_dsl::underlay_group_id.eq(underlay_group.id))
                .execute_async(&*self.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        }

        Ok(underlay_group)
    }

    /// Fetch an underlay multicast group by ID.
    pub async fn underlay_multicast_group_fetch(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
    ) -> LookupResult<UnderlayMulticastGroup> {
        use nexus_db_schema::schema::underlay_multicast_group::dsl;

        dsl::underlay_multicast_group
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(group_id))
            .select(UnderlayMulticastGroup::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    /// Fetch underlay multicast group using provided connection.
    pub async fn underlay_multicast_group_fetch_on_conn(
        &self,
        _opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        group_id: Uuid,
    ) -> LookupResult<UnderlayMulticastGroup> {
        use nexus_db_schema::schema::underlay_multicast_group::dsl;

        dsl::underlay_multicast_group
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(group_id))
            .select(UnderlayMulticastGroup::as_select())
            .first_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::MulticastGroup,
                        LookupType::ById(group_id.into_untyped_uuid()),
                    ),
                )
            })
    }

    /// Delete an underlay multicast group permanently.
    ///
    /// This immediately removes the underlay group record from the database. It
    /// sho¨ld only be called when the group is already removed from the switch
    /// or when cleaning up failed operations.
    pub async fn underlay_multicast_group_delete(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::underlay_multicast_group::dsl;

        diesel::delete(dsl::underlay_multicast_group)
            .filter(dsl::id.eq(group_id))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::Ipv4Addr;

    use nexus_types::identity::Resource;
    use omicron_common::address::{IpRange, Ipv4Range};
    use omicron_common::api::external::{
        IdentityMetadataUpdateParams, NameOrId,
    };
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{
        GenericUuid, InstanceUuid, PropolisUuid, SledUuid,
    };

    use crate::db::datastore::Error;
    use crate::db::datastore::LookupType;
    use crate::db::model::IpPool;
    use crate::db::model::{
        Generation, InstanceRuntimeState, IpPoolResource, IpPoolResourceType,
        IpVersion, MulticastGroupMemberState,
    };
    use crate::db::pub_test_utils::helpers::{
        SledUpdateBuilder, create_project,
    };
    use crate::db::pub_test_utils::{TestDatabase, helpers, multicast};

    async fn create_test_sled(datastore: &DataStore) -> SledUuid {
        let sled_id = SledUuid::new_v4();
        let sled_update = SledUpdateBuilder::new().sled_id(sled_id).build();
        datastore.sled_upsert(sled_update).await.unwrap();
        sled_id
    }

    #[tokio::test]
    async fn test_multicast_group_datastore_pool_exhaustion() {
        let logctx =
            dev::test_setup_log("test_multicast_group_pool_exhaustion");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool_identity = IdentityMetadataCreateParams {
            name: "exhaust-pool".parse().unwrap(),
            description: "Pool exhaustion test".to_string(),
        };

        // Create multicast IP pool with very small range (2 addresses)
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            // Only 2 addresses
            Ipv4Range::new(
                Ipv4Addr::new(224, 100, 2, 1),
                Ipv4Addr::new(224, 100, 2, 2),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let link = IpPoolResource {
            resource_id: opctx.authn.silo_required().unwrap().id(),
            resource_type: IpPoolResourceType::Silo,
            ip_pool_id: ip_pool.id(),
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        // Allocate first address
        let params1 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "first-group".parse().unwrap(),
                description: "First group".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name("exhaust-pool".parse().unwrap())),
            mvlan: None,
        };
        datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params1,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create first group");

        // Allocate second address
        let params2 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "second-group".parse().unwrap(),
                description: "Second group".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name("exhaust-pool".parse().unwrap())),
            mvlan: None,
        };
        datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params2,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create second group");

        // Third allocation should fail due to exhaustion
        let params3 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "third-group".parse().unwrap(),
                description: "Should fail".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name("exhaust-pool".parse().unwrap())),
            mvlan: None,
        };
        let result3 = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params3,
                Some(authz_pool.clone()),
            )
            .await;
        assert!(
            result3.is_err(),
            "Third allocation should fail due to pool exhaustion"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_datastore_default_pool_allocation() {
        let logctx =
            dev::test_setup_log("test_multicast_group_default_pool_allocation");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool_identity = IdentityMetadataCreateParams {
            name: "default-multicast-pool".parse().unwrap(),
            description: "Default pool allocation test".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            external::LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 250, 1, 1),
                Ipv4Addr::new(224, 250, 1, 10),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let link = IpPoolResource {
            resource_id: opctx.authn.silo_required().unwrap().id(),
            resource_type: IpPoolResourceType::Silo,
            ip_pool_id: ip_pool.id(),
            is_default: true, // For default allocation
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        // Create group without specifying pool (should use default)
        let params_default = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "auto-alloc-group".parse().unwrap(),
                description: "Group using default pool".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: None, // No pool specified - should use default
            mvlan: None,
        };

        let group_default = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params_default,
                None,
            )
            .await
            .expect("Should create group from default pool");

        assert_eq!(group_default.state, MulticastGroupState::Creating);

        // Verify the IP is from our default pool's range
        let ip_str = group_default.multicast_ip.ip().to_string();
        assert!(
            ip_str.starts_with("224.250.1."),
            "IP should be from default pool range"
        );

        // Create group with explicit pool name
        let params_explicit = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "explicit-alloc-group".parse().unwrap(),
                description: "Group with explicit pool".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name(
                "default-multicast-pool".parse().unwrap(),
            )),
            mvlan: None,
        };
        let group_explicit = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params_explicit,
                None,
            )
            .await
            .expect("Should create group from explicit pool");

        assert_eq!(group_explicit.state, MulticastGroupState::Creating);

        // Verify the explicit group also got an IP from the same default pool range
        let ip_str_explicit = group_explicit.multicast_ip.ip().to_string();
        assert!(
            ip_str_explicit.starts_with("224.250.1."),
            "Explicit IP should also be from default pool range"
        );

        // Test state transitions on the default pool group
        datastore
            .multicast_group_set_state(
                &opctx,
                group_default.id(),
                MulticastGroupState::Active,
            )
            .await
            .expect("Should transition default group to 'Active'");

        let updated_group = datastore
            .multicast_group_fetch(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group_default.id()),
            )
            .await
            .expect("Should fetch updated group");
        assert_eq!(updated_group.state, MulticastGroupState::Active);

        // Test list by state functionality
        let pagparams = &DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };
        let active_groups = datastore
            .multicast_groups_list_by_state(
                &opctx,
                MulticastGroupState::Active,
                pagparams,
            )
            .await
            .expect("Should list active groups");
        assert!(active_groups.iter().any(|g| g.id() == group_default.id()));

        let creating_groups = datastore
            .multicast_groups_list_by_state(
                &opctx,
                MulticastGroupState::Creating,
                pagparams,
            )
            .await
            .expect("Should list creating groups");
        // The explicit group should still be "Creating"
        assert!(creating_groups.iter().any(|g| g.id() == group_explicit.id()));
        // The default group should not be in "Creating" anymore
        assert!(!creating_groups.iter().any(|g| g.id() == group_default.id()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_datastore_underlay_linkage() {
        let logctx =
            dev::test_setup_log("test_multicast_group_with_underlay_linkage");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool_identity = IdentityMetadataCreateParams {
            name: "test-multicast-pool".parse().unwrap(),
            description: "Comprehensive test pool".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            external::LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 1, 3, 1),
                Ipv4Addr::new(224, 1, 3, 5),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        // Create external multicast group with explicit address
        let params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-group".parse().unwrap(),
                description: "Comprehensive test group".to_string(),
            },
            multicast_ip: Some("224.1.3.3".parse().unwrap()),
            source_ips: None,
            pool: Some(NameOrId::Name("test-multicast-pool".parse().unwrap())),
            mvlan: None,
        };

        let external_group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create external group");

        // Verify initial state
        assert_eq!(external_group.multicast_ip.to_string(), "224.1.3.3/32");
        assert_eq!(external_group.state, MulticastGroupState::Creating);
        // With RPW pattern, underlay_group_id is initially None in "Creating" state
        assert_eq!(external_group.underlay_group_id, None);

        // Create underlay group using ensure method (this would normally be done by reconciler)
        let underlay_group = datastore
            .ensure_underlay_multicast_group(
                &opctx,
                external_group.clone(),
                "ff04::1".parse().unwrap(),
                external_group.vni,
            )
            .await
            .expect("Should create underlay group");

        // Verify underlay group properties
        assert!(underlay_group.multicast_ip.ip().is_ipv6());
        assert!(underlay_group.vni() > 0);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_operations_with_parent_id() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_operations_with_parent_id",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Set up multicast IP pool and group
        let pool_identity = IdentityMetadataCreateParams {
            name: "parent-id-test-pool".parse().unwrap(),
            description: "Pool for parent_id testing".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            external::LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 3, 1, 1),
                Ipv4Addr::new(224, 3, 1, 10),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        // Create test project for parent_id operations
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "test-project").await;

        // Create a multicast group using the real project
        let params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "parent-id-test-group".parse().unwrap(),
                description: "Group for parent_id testing".to_string(),
            },
            multicast_ip: Some("224.3.1.5".parse().unwrap()),
            source_ips: None,
            pool: Some(NameOrId::Name("parent-id-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(), // rack_id
                &params,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create multicast group");

        // Create test sled and instances
        let sled_id = create_test_sled(&datastore).await;
        let instance_record_1 = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "test-instance-1",
        )
        .await;
        let parent_id_1 = instance_record_1.as_untyped_uuid();
        let instance_record_2 = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "test-instance-2",
        )
        .await;
        let parent_id_2 = instance_record_2.as_untyped_uuid();
        let instance_record_3 = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "test-instance-3",
        )
        .await;
        let parent_id_3 = instance_record_3.as_untyped_uuid();

        // Create VMMs and associate instances with sled (required for multicast membership)
        let vmm1_id = PropolisUuid::new_v4();
        let vmm1 = crate::db::model::Vmm::new(
            vmm1_id,
            InstanceUuid::from_untyped_uuid(*parent_id_1),
            sled_id,
            "127.0.0.1".parse().unwrap(),
            12400,
            crate::db::model::VmmCpuPlatform::SledDefault,
        );
        datastore.vmm_insert(&opctx, vmm1).await.expect("Should create VMM1");

        let vmm2_id = PropolisUuid::new_v4();
        let vmm2 = crate::db::model::Vmm::new(
            vmm2_id,
            InstanceUuid::from_untyped_uuid(*parent_id_2),
            sled_id,
            "127.0.0.1".parse().unwrap(),
            12401,
            crate::db::model::VmmCpuPlatform::SledDefault,
        );
        datastore.vmm_insert(&opctx, vmm2).await.expect("Should create VMM2");

        let vmm3_id = PropolisUuid::new_v4();
        let vmm3 = crate::db::model::Vmm::new(
            vmm3_id,
            InstanceUuid::from_untyped_uuid(*parent_id_3),
            sled_id,
            "127.0.0.1".parse().unwrap(),
            12402,
            crate::db::model::VmmCpuPlatform::SledDefault,
        );
        datastore.vmm_insert(&opctx, vmm3).await.expect("Should create VMM3");

        // Update instances to point to their VMMs
        let instance1 = datastore
            .instance_refetch(
                &opctx,
                &authz::Instance::new(
                    authz_project.clone(),
                    instance_record_1.into_untyped_uuid(),
                    LookupType::by_id(instance_record_1),
                ),
            )
            .await
            .expect("Should fetch instance1");
        datastore
            .instance_update_runtime(
                &instance_record_1,
                &InstanceRuntimeState {
                    nexus_state: crate::db::model::InstanceState::Vmm,
                    propolis_id: Some(vmm1_id.into_untyped_uuid()),
                    dst_propolis_id: None,
                    migration_id: None,
                    gen: Generation::from(instance1.runtime().gen.next()),
                    time_updated: Utc::now(),
                    time_last_auto_restarted: None,
                },
            )
            .await
            .expect("Should set instance1 runtime state");

        let instance2 = datastore
            .instance_refetch(
                &opctx,
                &authz::Instance::new(
                    authz_project.clone(),
                    instance_record_2.into_untyped_uuid(),
                    LookupType::by_id(instance_record_2),
                ),
            )
            .await
            .expect("Should fetch instance2");
        datastore
            .instance_update_runtime(
                &instance_record_2,
                &InstanceRuntimeState {
                    nexus_state: crate::db::model::InstanceState::Vmm,
                    propolis_id: Some(vmm2_id.into_untyped_uuid()),
                    dst_propolis_id: None,
                    migration_id: None,
                    gen: Generation::from(instance2.runtime().gen.next()),
                    time_updated: Utc::now(),
                    time_last_auto_restarted: None,
                },
            )
            .await
            .expect("Should set instance2 runtime state");

        let instance3 = datastore
            .instance_refetch(
                &opctx,
                &authz::Instance::new(
                    authz_project.clone(),
                    instance_record_3.into_untyped_uuid(),
                    LookupType::by_id(instance_record_3),
                ),
            )
            .await
            .expect("Should fetch instance3");
        datastore
            .instance_update_runtime(
                &instance_record_3,
                &InstanceRuntimeState {
                    nexus_state: crate::db::model::InstanceState::Vmm,
                    propolis_id: Some(vmm3_id.into_untyped_uuid()),
                    dst_propolis_id: None,
                    migration_id: None,
                    gen: Generation::from(instance3.runtime().gen.next()),
                    time_updated: Utc::now(),
                    time_last_auto_restarted: None,
                },
            )
            .await
            .expect("Should set instance3 runtime state");

        // Transition group to "Active" state before adding members
        datastore
            .multicast_group_set_state(
                &opctx,
                group.id(),
                MulticastGroupState::Active,
            )
            .await
            .expect("Should transition group to 'Active' state");

        // Add members using parent_id
        let member1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_1),
            )
            .await
            .expect("Should add first member");

        let member2 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_2),
            )
            .await
            .expect("Should add second member");

        // Try to add the same parent_id again - should succeed idempotently
        let duplicate_result = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_1),
            )
            .await
            .expect("Should handle duplicate add idempotently");

        // Should return the same member (idempotent)
        assert_eq!(duplicate_result.id, member1.id);
        assert_eq!(duplicate_result.parent_id, member1.parent_id);

        // Verify member structure uses parent_id correctly
        assert_eq!(member1.external_group_id, group.id());
        assert_eq!(member1.parent_id, *parent_id_1);
        assert_eq!(member2.external_group_id, group.id());
        assert_eq!(member2.parent_id, *parent_id_2);

        // Verify generation sequence is working correctly
        // (database assigns sequential values)
        let gen1 = member1.version_added;
        let gen2 = member2.version_added;
        assert!(
            i64::from(&*gen1) > 0,
            "First member should have positive generation number"
        );
        assert!(
            gen2 > gen1,
            "Second member should have higher generation than first"
        );

        // List members
        let pagparams = &DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };

        let members = datastore
            .multicast_group_members_list(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
            .await
            .expect("Should list members");

        assert_eq!(members.len(), 2);
        assert!(members.iter().any(|m| m.parent_id == *parent_id_1));
        assert!(members.iter().any(|m| m.parent_id == *parent_id_2));

        // Remove member by parent_id
        datastore
            .multicast_group_member_detach_by_group_and_instance(
                &opctx,
                group.id(),
                *parent_id_1,
            )
            .await
            .expect("Should remove first member");

        // Verify only one active member remains
        let all_members = datastore
            .multicast_group_members_list(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
            .await
            .expect("Should list remaining members");

        // Filter for active members (non-"Left" state)
        let active_members: Vec<_> = all_members
            .into_iter()
            .filter(|m| m.state != MulticastGroupMemberState::Left)
            .collect();

        assert_eq!(active_members.len(), 1);
        assert_eq!(active_members[0].parent_id, *parent_id_2);

        // Verify member removal doesn't affect the group
        let updated_group = datastore
            .multicast_group_fetch(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .expect("Should fetch group after member removal");
        assert_eq!(updated_group.id(), group.id());
        assert_eq!(updated_group.multicast_ip, group.multicast_ip);

        // Add member back and remove all
        datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_1),
            )
            .await
            .expect("Should re-add first member");

        datastore
            .multicast_group_member_detach_by_group_and_instance(
                &opctx,
                group.id(),
                *parent_id_1,
            )
            .await
            .expect("Should remove first member again");

        datastore
            .multicast_group_member_detach_by_group_and_instance(
                &opctx,
                group.id(),
                *parent_id_2,
            )
            .await
            .expect("Should remove second member");

        // Verify no active members remain
        let all_final_members = datastore
            .multicast_group_members_list(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
            .await
            .expect("Should list final members");

        // Filter for active members (non-"Left" state)
        let active_final_members: Vec<_> = all_final_members
            .into_iter()
            .filter(|m| m.state != MulticastGroupMemberState::Left)
            .collect();

        assert_eq!(active_final_members.len(), 0);

        // Add a member with the third parent_id to verify different parent
        // types work
        let member3 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_3),
            )
            .await
            .expect("Should add third member with different parent_id");

        assert_eq!(member3.external_group_id, group.id());
        assert_eq!(member3.parent_id, *parent_id_3);

        // Verify generation continues to increment properly
        let gen3 = member3.version_added;
        assert!(
            gen3 > gen2,
            "Third member should have higher generation than second"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_duplicate_prevention() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_duplicate_prevention",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Set up multicast IP pool and group
        let pool_identity = IdentityMetadataCreateParams {
            name: "duplicate-test-pool".parse().unwrap(),
            description: "Pool for duplicate testing".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            external::LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 3, 1, 1),
                Ipv4Addr::new(224, 3, 1, 10),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        // Create test project, sled and instance for duplicate testing
        let (authz_project, _project) =
            helpers::create_project(&opctx, &datastore, "dup-test-proj").await;
        let sled_id = create_test_sled(&datastore).await;
        let instance_record = helpers::create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "dup-test-instance",
        )
        .await;
        let parent_id = instance_record.as_untyped_uuid();

        // Create VMM and associate instance with sled (required for multicast membership)
        let vmm_id = PropolisUuid::new_v4();
        let vmm = crate::db::model::Vmm::new(
            vmm_id,
            InstanceUuid::from_untyped_uuid(*parent_id),
            sled_id,
            "127.0.0.1".parse().unwrap(),
            12400,
            crate::db::model::VmmCpuPlatform::SledDefault,
        );
        datastore.vmm_insert(&opctx, vmm).await.expect("Should create VMM");

        // Update instance to point to the VMM (increment generation for update to succeed)
        let instance = datastore
            .instance_refetch(
                &opctx,
                &authz::Instance::new(
                    authz_project.clone(),
                    instance_record.into_untyped_uuid(),
                    LookupType::by_id(instance_record),
                ),
            )
            .await
            .expect("Should fetch instance");
        datastore
            .instance_update_runtime(
                &instance_record,
                &InstanceRuntimeState {
                    nexus_state: crate::db::model::InstanceState::Vmm,
                    propolis_id: Some(vmm_id.into_untyped_uuid()),
                    dst_propolis_id: None,
                    migration_id: None,
                    gen: Generation::from(instance.runtime().gen.next()),
                    time_updated: Utc::now(),
                    time_last_auto_restarted: None,
                },
            )
            .await
            .expect("Should set instance runtime state");

        let params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "duplicate-test-group".parse().unwrap(),
                description: "Group for duplicate testing".to_string(),
            },
            multicast_ip: Some("224.3.1.5".parse().unwrap()),
            source_ips: None,
            pool: Some(NameOrId::Name("duplicate-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(), // rack_id
                &params,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create multicast group");

        // Transition group to "Active" state before adding members
        datastore
            .multicast_group_set_state(
                &opctx,
                group.id(),
                MulticastGroupState::Active,
            )
            .await
            .expect("Should transition group to 'Active' state");

        // Add member first time - should succeed
        let member1 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id),
            )
            .await
            .expect("Should add member first time");

        // Try to add same parent_id again - this should either:
        // 1. Fail with a conflict error, or
        // 2. Succeed if the system allows multiple entries (which we can test)
        let result2 = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id),
            )
            .await;

        // Second attempt should succeed idempotently (return existing member)
        let member2 =
            result2.expect("Should handle duplicate add idempotently");

        // Should return the same member (idempotent)
        assert_eq!(member2.id, member1.id);
        assert_eq!(member2.parent_id, *parent_id);

        // Verify only one member exists
        let pagparams = &DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };

        let members = datastore
            .multicast_group_members_list(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
            .await
            .expect("Should list members");

        assert_eq!(members.len(), 1);
        assert_eq!(members[0].parent_id, *parent_id);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_member_state_transitions_datastore() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_member_state_transitions_datastore",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Set up multicast IP pool and group
        let pool_identity = IdentityMetadataCreateParams {
            name: "state-test-pool".parse().unwrap(),
            description: "Pool for state transition testing".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 4, 1, 1),
                Ipv4Addr::new(224, 4, 1, 10),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link pool to silo");

        // Create multicast group (datastore-only; not exercising reconciler)
        let group_params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "state-test-group".parse().unwrap(),
                description: "Group for testing member state transitions"
                    .to_string(),
            },
            multicast_ip: None, // Let it allocate from pool
            source_ips: None,
            pool: Some(NameOrId::Name("state-test-pool".parse().unwrap())),
            mvlan: None,
        };
        let group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &group_params,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create multicast group");

        // Create test project and instance (datastore-only)
        let (authz_project, _project) =
            helpers::create_project(&opctx, &datastore, "state-test-proj")
                .await;
        let sled_id = create_test_sled(&datastore).await;
        let (instance, _vmm) = helpers::create_instance_with_vmm(
            &opctx,
            &datastore,
            &authz_project,
            "state-test-instance",
            sled_id,
        )
        .await;
        let test_instance_id = instance.into_untyped_uuid();

        // Transition group to "Active" state before adding members
        datastore
            .multicast_group_set_state(
                &opctx,
                group.id(),
                MulticastGroupState::Active,
            )
            .await
            .expect("Should transition group to 'Active' state");

        // Create member record in "Joining" state using datastore API
        let member = datastore
            .multicast_group_member_add(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
            )
            .await
            .expect("Should create member record");

        assert_eq!(member.state, MulticastGroupMemberState::Joining);
        assert_eq!(member.parent_id, test_instance_id);

        // Test: Transition from "Joining" → "Joined" (simulating what the reconciler would do)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                group.id(),
                test_instance_id,
                MulticastGroupMemberState::Joined,
            )
            .await
            .expect("Should transition to 'Joined'");

        // Verify member is now "Active"
        let pagparams = &DataPageParams {
            marker: None,
            limit: std::num::NonZeroU32::new(100).unwrap(),
            direction: dropshot::PaginationOrder::Ascending,
        };

        let members = datastore
            .multicast_group_members_list(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
            .await
            .expect("Should list members");

        assert_eq!(members.len(), 1);
        assert_eq!(members[0].state, MulticastGroupMemberState::Joined);

        // Test: Transition member to "Left" state (without permanent deletion)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                group.id(),
                test_instance_id,
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should transition to 'Left' state");

        // Verify member is now in "Left" state (use _all_states to see Left members)
        let all_members = datastore
            .multicast_group_members_list_all(&opctx, group.id(), pagparams)
            .await
            .expect("Should list all members");

        assert_eq!(all_members.len(), 1);

        // Verify only "Active" members are shown (filter out Left members)
        let all_members = datastore
            .multicast_group_members_list(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
            .await
            .expect("Should list all members");

        // Filter for "Active" members (non-"Left" state)
        let active_members: Vec<_> = all_members
            .into_iter()
            .filter(|m| m.state != MulticastGroupMemberState::Left)
            .collect();

        assert_eq!(
            active_members.len(),
            0,
            "Active member list should filter out Left members"
        );

        // Complete removal (→ "Left")
        datastore
            .multicast_group_member_set_state(
                &opctx,
                group.id(),
                test_instance_id,
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should transition to Deleted");

        // Member should still exist in database but marked as "Deleted"
        let members = datastore
            .multicast_group_members_list_all(&opctx, group.id(), pagparams)
            .await
            .expect("Should list members");

        assert_eq!(members.len(), 1);
        assert_eq!(members[0].state, MulticastGroupMemberState::Left);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_ip_reuse_after_deletion() {
        let logctx =
            dev::test_setup_log("test_multicast_group_ip_reuse_after_deletion");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Set up multicast IP pool
        let pool_identity = IdentityMetadataCreateParams {
            name: "reuse-test-pool".parse().unwrap(),
            description: "Pool for IP reuse testing".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            external::LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 10, 1, 100),
                Ipv4Addr::new(224, 10, 1, 102), // Only 3 addresses
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link pool to silo");

        // Create group with specific IP
        let target_ip = "224.10.1.101".parse().unwrap();
        let params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "reuse-test".parse().unwrap(),
                description: "Group for IP reuse test".to_string(),
            },
            multicast_ip: Some(target_ip),
            source_ips: None,
            pool: Some(NameOrId::Name("reuse-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group1 = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create first group");
        assert_eq!(group1.multicast_ip.ip(), target_ip);

        // Delete the group completely (time_deleted set)
        let deleted = datastore
            .deallocate_external_multicast_group(&opctx, group1.id())
            .await
            .expect("Should deallocate group");
        assert_eq!(deleted, true, "Should successfully deallocate the group");

        // Create another group with the same IP - should succeed due to time_deleted filtering
        let params2 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "reuse-test-2".parse().unwrap(),
                description: "Second group reusing same IP".to_string(),
            },
            multicast_ip: Some(target_ip),
            source_ips: None,
            pool: Some(NameOrId::Name("reuse-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group2 = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params2,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create second group with same IP after first was deleted");
        assert_eq!(group2.multicast_ip.ip(), target_ip);
        assert_ne!(
            group1.id(),
            group2.id(),
            "Should be different group instances"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_pool_exhaustion_delete_create_cycle() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_pool_exhaustion_delete_create_cycle",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Set up small pool (only 1 address)
        let pool_identity = IdentityMetadataCreateParams {
            name: "cycle-test-pool".parse().unwrap(),
            description: "Pool for exhaustion-delete-create cycle testing"
                .to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            external::LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 20, 1, 50), // Only 1 address
                Ipv4Addr::new(224, 20, 1, 50),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link pool to silo");

        // Exhaust the pool
        let params1 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "cycle-test-1".parse().unwrap(),
                description: "First group to exhaust pool".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name("cycle-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group1 = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params1,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create first group");
        let allocated_ip = group1.multicast_ip.ip();

        // Try to create another group - should fail due to exhaustion
        let params2 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "cycle-test-2".parse().unwrap(),
                description: "Second group should fail".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name("cycle-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let result2 = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params2,
                Some(authz_pool.clone()),
            )
            .await;
        assert!(
            result2.is_err(),
            "Second group creation should fail due to pool exhaustion"
        );

        // Delete the first group to free up the IP
        let deleted = datastore
            .deallocate_external_multicast_group(&opctx, group1.id())
            .await
            .expect("Should deallocate first group");
        assert_eq!(deleted, true, "Should successfully deallocate the group");

        // Now creating a new group should succeed
        let params3 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "cycle-test-3".parse().unwrap(),
                description: "Third group should succeed after deletion"
                    .to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name("cycle-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group3 = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params3,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create third group after first was deleted");

        // Should reuse the same IP address
        assert_eq!(
            group3.multicast_ip.ip(),
            allocated_ip,
            "Should reuse the same IP address"
        );
        assert_ne!(
            group1.id(),
            group3.id(),
            "Should be different group instances"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_deallocation_return_values() {
        let logctx = dev::test_setup_log(
            "test_multicast_group_deallocation_return_values",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Set up multicast IP pool
        let pool_identity = IdentityMetadataCreateParams {
            name: "dealloc-test-pool".parse().unwrap(),
            description: "Pool for deallocation testing".to_string(),
        };
        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            external::LookupType::ById(ip_pool.id()),
        );
        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 30, 1, 1),
                Ipv4Addr::new(224, 30, 1, 5),
            )
            .unwrap(),
        );
        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add multicast range to pool");

        let silo_id = opctx.authn.silo_required().unwrap().id();
        let link = IpPoolResource {
            ip_pool_id: ip_pool.id(),
            resource_type: IpPoolResourceType::Silo,
            resource_id: silo_id,
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link pool to silo");

        // Create a group
        let params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "dealloc-test".parse().unwrap(),
                description: "Group for deallocation testing".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            pool: Some(NameOrId::Name("dealloc-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create multicast group");

        // Deallocate existing group - should return true
        let result1 = datastore
            .deallocate_external_multicast_group(&opctx, group.id())
            .await
            .expect("Deallocation should succeed");
        assert_eq!(
            result1, true,
            "Deallocating existing group should return true"
        );

        // Deallocate the same group again - should return false (already deleted)
        let result2 = datastore
            .deallocate_external_multicast_group(&opctx, group.id())
            .await
            .expect("Second deallocation should succeed but return false");
        assert_eq!(
            result2, false,
            "Deallocating already-deleted group should return false"
        );

        // Try to deallocate non-existent group - should return error
        let fake_id = Uuid::new_v4();
        let result3 = datastore
            .deallocate_external_multicast_group(&opctx, fake_id)
            .await;
        assert!(
            result3.is_err(),
            "Deallocating non-existent group should return an error"
        );

        // Verify it's the expected NotFound error
        match result3.unwrap_err() {
            external::Error::ObjectNotFound { .. } => {
                // This is expected
            }
            other => panic!("Expected ObjectNotFound error, got: {:?}", other),
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_create_and_fetch() {
        let logctx =
            dev::test_setup_log("test_multicast_group_create_and_fetch");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create project for multicast groups

        // Create IP pool
        let pool_identity = IdentityMetadataCreateParams {
            name: "fetch-test-pool".parse().unwrap(),
            description: "Test pool for fetch operations".to_string(),
        };

        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            LookupType::ById(ip_pool.id()),
        );

        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 100, 10, 1),
                Ipv4Addr::new(224, 100, 10, 100),
            )
            .unwrap(),
        );

        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add range to pool");

        let link = IpPoolResource {
            resource_id: opctx.authn.silo_required().unwrap().id(),
            resource_type: IpPoolResourceType::Silo,
            ip_pool_id: ip_pool.id(),
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        // Test creating a multicast group
        let params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fetch-test-group".parse().unwrap(),
                description: "Test group for fetch operations".to_string(),
            },
            multicast_ip: Some("224.100.10.5".parse().unwrap()),
            source_ips: Some(vec![
                "10.0.0.1".parse().unwrap(),
                "10.0.0.2".parse().unwrap(),
            ]),
            pool: Some(NameOrId::Name("fetch-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params,
                Some(authz_pool),
            )
            .await
            .expect("Should create multicast group");

        // Test fetching the created group
        let fetched_group = datastore
            .multicast_group_fetch(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .expect("Should fetch created group");

        assert_eq!(group.id(), fetched_group.id());
        assert_eq!(group.name(), fetched_group.name());
        assert_eq!(group.description(), fetched_group.description());
        assert_eq!(group.multicast_ip, fetched_group.multicast_ip);
        assert_eq!(group.source_ips, fetched_group.source_ips);
        assert_eq!(group.state, MulticastGroupState::Creating);

        // Test fetching non-existent group
        let fake_id = Uuid::new_v4();
        let result = datastore
            .multicast_group_fetch(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_id),
            )
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            external::Error::ObjectNotFound { .. } => {
                // Expected
            }
            other => panic!("Expected ObjectNotFound, got: {:?}", other),
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_list_fleet_wide() {
        let logctx =
            dev::test_setup_log("test_multicast_group_list_fleet_wide");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create IP pool
        let pool_identity = IdentityMetadataCreateParams {
            name: "list-test-pool".parse().unwrap(),
            description: "Test pool for list operations".to_string(),
        };

        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            LookupType::ById(ip_pool.id()),
        );

        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 100, 20, 1),
                Ipv4Addr::new(224, 100, 20, 100),
            )
            .unwrap(),
        );

        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add range to pool");

        let link = IpPoolResource {
            resource_id: opctx.authn.silo_required().unwrap().id(),
            resource_type: IpPoolResourceType::Silo,
            ip_pool_id: ip_pool.id(),
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        // Create fleet-wide multicast groups
        let params_1 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fleet-group-1".parse().unwrap(),
                description: "Fleet-wide group 1".to_string(),
            },
            multicast_ip: Some("224.100.20.10".parse().unwrap()),
            source_ips: None,
            pool: Some(NameOrId::Name("list-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let params_2 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fleet-group-2".parse().unwrap(),
                description: "Fleet-wide group 2".to_string(),
            },
            multicast_ip: Some("224.100.20.11".parse().unwrap()),
            source_ips: None,
            pool: Some(NameOrId::Name("list-test-pool".parse().unwrap())),
            mvlan: None,
        };

        let params_3 = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fleet-group-3".parse().unwrap(),
                description: "Fleet-wide group 3".to_string(),
            },
            multicast_ip: Some("224.100.20.12".parse().unwrap()),
            source_ips: None,
            pool: Some(NameOrId::Name("list-test-pool".parse().unwrap())),
            mvlan: None,
        };

        // Create groups (all are fleet-wide)
        datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params_1,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create fleet-group-1");

        datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params_2,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create fleet-group-2");

        datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params_3,
                Some(authz_pool),
            )
            .await
            .expect("Should create fleet-group-3");

        // List all groups fleet-wide - should get 3 groups
        let pagparams = DataPageParams {
            marker: None,
            direction: external::PaginationOrder::Ascending,
            limit: std::num::NonZeroU32::new(10).unwrap(),
        };

        let paginated_by =
            external::http_pagination::PaginatedBy::Id(pagparams);
        let groups = datastore
            .multicast_groups_list(&opctx, &paginated_by)
            .await
            .expect("Should list all fleet-wide groups");

        assert_eq!(groups.len(), 3, "Should have 3 fleet-wide groups");

        // Verify the groups have the correct names
        let group_names: Vec<_> =
            groups.iter().map(|g| g.name().to_string()).collect();
        assert!(group_names.contains(&"fleet-group-1".to_string()));
        assert!(group_names.contains(&"fleet-group-2".to_string()));
        assert!(group_names.contains(&"fleet-group-3".to_string()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_state_transitions() {
        let logctx =
            dev::test_setup_log("test_multicast_group_state_transitions");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create IP pool
        let pool_identity = IdentityMetadataCreateParams {
            name: "state-test-pool".parse().unwrap(),
            description: "Test pool for state transitions".to_string(),
        };

        let ip_pool = datastore
            .ip_pool_create(
                &opctx,
                IpPool::new_multicast(&pool_identity, IpVersion::V4),
            )
            .await
            .expect("Should create multicast IP pool");

        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            ip_pool.id(),
            LookupType::ById(ip_pool.id()),
        );

        let range = IpRange::V4(
            Ipv4Range::new(
                Ipv4Addr::new(224, 100, 30, 1),
                Ipv4Addr::new(224, 100, 30, 100),
            )
            .unwrap(),
        );

        datastore
            .ip_pool_add_range(&opctx, &authz_pool, &ip_pool, &range)
            .await
            .expect("Should add range to pool");

        let link = IpPoolResource {
            resource_id: opctx.authn.silo_required().unwrap().id(),
            resource_type: IpPoolResourceType::Silo,
            ip_pool_id: ip_pool.id(),
            is_default: false,
        };
        datastore
            .ip_pool_link_silo(&opctx, link)
            .await
            .expect("Should link multicast pool to silo");

        let params = params::MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "state-test-group".parse().unwrap(),
                description: "Test group for state transitions".to_string(),
            },
            multicast_ip: Some("224.100.30.5".parse().unwrap()),
            source_ips: None,
            pool: Some(NameOrId::Name("state-test-pool".parse().unwrap())),
            mvlan: None,
        };

        // Create group - starts in "Creating" state
        let group = datastore
            .multicast_group_create(
                &opctx,
                Uuid::new_v4(),
                &params,
                Some(authz_pool),
            )
            .await
            .expect("Should create multicast group");

        assert_eq!(group.state, MulticastGroupState::Creating);

        // Test transition to "Active"
        datastore
            .multicast_group_set_state(
                &opctx,
                group.id(),
                MulticastGroupState::Active,
            )
            .await
            .expect("Should transition to 'Active'");

        let updated_group = datastore
            .multicast_group_fetch(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .expect("Should fetch updated group");

        assert_eq!(updated_group.state, MulticastGroupState::Active);

        // Test transition to "Deleting"
        datastore
            .multicast_group_set_state(
                &opctx,
                group.id(),
                MulticastGroupState::Deleting,
            )
            .await
            .expect("Should transition to 'Deleting'");

        let deleting_group = datastore
            .multicast_group_fetch(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .expect("Should fetch deleting group");

        assert_eq!(deleting_group.state, MulticastGroupState::Deleting);

        // Test trying to update non-existent group
        let fake_id = Uuid::new_v4();
        let result = datastore
            .multicast_group_set_state(
                &opctx,
                fake_id,
                MulticastGroupState::Active,
            )
            .await;
        assert!(result.is_err());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_lookup_by_ip() {
        let logctx = dev::test_setup_log("test_multicast_group_lookup_by_ip");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create test setup
        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "test-pool",
            "test-project",
        )
        .await;

        // Create first multicast group with IP 224.10.1.100
        let group1 = multicast::create_test_group(
            &opctx,
            &datastore,
            &setup,
            "group1",
            "224.10.1.100",
        )
        .await;

        // Create second multicast group with IP 224.10.1.101
        let group2 = multicast::create_test_group(
            &opctx,
            &datastore,
            &setup,
            "group2",
            "224.10.1.101",
        )
        .await;

        // Test successful lookup for first group
        let found_group1 = datastore
            .multicast_group_lookup_by_ip(
                &opctx,
                "224.10.1.100".parse().unwrap(),
            )
            .await
            .expect("Should find group by IP");

        assert_eq!(found_group1.id(), group1.id());
        assert_eq!(
            found_group1.multicast_ip.ip(),
            "224.10.1.100".parse::<IpAddr>().unwrap()
        );

        // Test successful lookup for second group
        let found_group2 = datastore
            .multicast_group_lookup_by_ip(
                &opctx,
                "224.10.1.101".parse().unwrap(),
            )
            .await
            .expect("Should find group by IP");

        assert_eq!(found_group2.id(), group2.id());
        assert_eq!(
            found_group2.multicast_ip.ip(),
            "224.10.1.101".parse::<IpAddr>().unwrap()
        );

        // Test lookup for nonexistent IP - should fail
        let not_found_result = datastore
            .multicast_group_lookup_by_ip(
                &opctx,
                "224.10.1.199".parse().unwrap(),
            )
            .await;

        assert!(not_found_result.is_err());
        match not_found_result.err().unwrap() {
            Error::ObjectNotFound { .. } => {
                // Expected error type for missing multicast group
            }
            other => panic!("Expected ObjectNotFound error, got: {:?}", other),
        }

        // Test that soft-deleted groups are not returned
        // Soft-delete group1 (sets time_deleted)
        datastore
            .deallocate_external_multicast_group(&opctx, group1.id())
            .await
            .expect("Should soft-delete group");

        // Now lookup should fail for deleted group
        let deleted_lookup_result = datastore
            .multicast_group_lookup_by_ip(
                &opctx,
                "224.10.1.100".parse().unwrap(),
            )
            .await;

        assert!(deleted_lookup_result.is_err());
        match deleted_lookup_result.err().unwrap() {
            Error::ObjectNotFound { .. } => {
                // Expected - deleted groups should not be found
            }
            other => panic!(
                "Expected ObjectNotFound error for deleted group, got: {:?}",
                other
            ),
        }

        // Second group should still be findable
        let still_found_group2 = datastore
            .multicast_group_lookup_by_ip(
                &opctx,
                "224.10.1.101".parse().unwrap(),
            )
            .await
            .expect("Should still find non-deleted group");

        assert_eq!(still_found_group2.id(), group2.id());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_multicast_group_update() {
        let logctx = dev::test_setup_log("test_multicast_group_update");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create test setup
        let setup = multicast::create_test_setup(
            &opctx,
            &datastore,
            "test-pool",
            "test-project",
        )
        .await;

        // Create initial multicast group
        let group = multicast::create_test_group(
            &opctx,
            &datastore,
            &setup,
            "original-group",
            "224.10.1.100",
        )
        .await;

        // Verify original values
        assert_eq!(group.name().as_str(), "original-group");
        assert_eq!(group.description(), "Test group: original-group");
        assert_eq!(group.source_ips.len(), 0); // Empty array initially

        // Test updating name and description
        let update_params = params::MulticastGroupUpdate {
            identity: IdentityMetadataUpdateParams {
                name: Some("updated-group".parse().unwrap()),
                description: Some("Updated group description".to_string()),
            },
            source_ips: None,
            mvlan: None,
        };

        let updated_group = datastore
            .multicast_group_update(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                &update_params,
            )
            .await
            .expect("Should update multicast group");

        // Verify updated identity fields
        assert_eq!(updated_group.name().as_str(), "updated-group");
        assert_eq!(updated_group.description(), "Updated group description");
        assert_eq!(updated_group.id(), group.id()); // ID should not change
        assert_eq!(updated_group.multicast_ip, group.multicast_ip); // IP should not change
        assert!(updated_group.time_modified() > group.time_modified()); // Modified time should advance

        // Test updating source IPs (Source-Specific Multicast)
        let source_ip_update = params::MulticastGroupUpdate {
            identity: IdentityMetadataUpdateParams {
                name: None,
                description: None,
            },
            source_ips: Some(vec![
                "10.1.1.10".parse().unwrap(),
                "10.1.1.20".parse().unwrap(),
            ]),
            mvlan: None,
        };

        let group_with_sources = datastore
            .multicast_group_update(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(updated_group.id()),
                &source_ip_update,
            )
            .await
            .expect("Should update source IPs");

        // Verify source IPs were updated
        assert_eq!(group_with_sources.source_ips.len(), 2);
        let source_addrs: Vec<_> =
            group_with_sources.source_ips.iter().map(|ip| ip.ip()).collect();
        assert!(source_addrs.contains(&"10.1.1.10".parse().unwrap()));
        assert!(source_addrs.contains(&"10.1.1.20".parse().unwrap()));

        // Test updating all fields at once
        let complete_update = params::MulticastGroupUpdate {
            identity: IdentityMetadataUpdateParams {
                name: Some("final-group".parse().unwrap()),
                description: Some("Final group description".to_string()),
            },
            source_ips: Some(vec!["192.168.1.1".parse().unwrap()]),
            mvlan: None,
        };

        let final_group = datastore
            .multicast_group_update(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group_with_sources.id()),
                &complete_update,
            )
            .await
            .expect("Should update all fields");

        assert_eq!(final_group.name().as_str(), "final-group");
        assert_eq!(final_group.description(), "Final group description");
        assert_eq!(final_group.source_ips.len(), 1);
        assert_eq!(
            final_group.source_ips[0].ip(),
            "192.168.1.1".parse::<IpAddr>().unwrap()
        );

        // Test updating nonexistent group - should fail
        let nonexistent_id = MulticastGroupUuid::new_v4();
        let failed_update = datastore
            .multicast_group_update(&opctx, nonexistent_id, &update_params)
            .await;

        assert!(failed_update.is_err());
        match failed_update.err().unwrap() {
            Error::ObjectNotFound { .. } => {
                // Expected error for nonexistent group
            }
            other => panic!("Expected ObjectNotFound error, got: {:?}", other),
        }

        // Test updating deleted group - should fail
        // First soft-delete the group (sets time_deleted)
        datastore
            .deallocate_external_multicast_group(&opctx, final_group.id())
            .await
            .expect("Should soft-delete group");

        let deleted_update = datastore
            .multicast_group_update(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(final_group.id()),
                &update_params,
            )
            .await;

        assert!(deleted_update.is_err());
        match deleted_update.err().unwrap() {
            Error::ObjectNotFound { .. } => {
                // Expected - soft-deleted groups should not be updatable
            }
            other => panic!(
                "Expected ObjectNotFound error for deleted group, got: {:?}",
                other
            ),
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
