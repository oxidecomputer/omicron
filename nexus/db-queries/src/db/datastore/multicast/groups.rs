// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multicast group management and IP allocation.
//!
//! Database operations for multicast groups following the bifurcated design
//! from [RFD 488](https://rfd.shared.oxide.computer/rfd/488):
//!
//! - External groups: customer-facing, allocated from IP pools
//! - Underlay groups: system-generated admin-scoped IPv6 multicast groups

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
use slog::{debug, error, info};
use uuid::Uuid;

use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_lookup::DbConnection;
use nexus_types::identity::Resource;
use nexus_types::multicast::MulticastGroupCreate;
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
    ExternalMulticastGroup, IncompleteExternalMulticastGroup,
    IncompleteExternalMulticastGroupParams, IpPoolType, MulticastGroup,
    MulticastGroupState, Name, UnderlayMulticastGroup, Vni,
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
    ///
    /// Used by RPW reconciler. For "Deleting" state, this includes groups with
    /// `time_deleted` set so the RPW can clean them up.
    pub async fn multicast_groups_list_by_state(
        &self,
        opctx: &OpContext,
        state: MulticastGroupState,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

        let mut query = paginated(dsl::multicast_group, dsl::id, pagparams)
            .filter(dsl::state.eq(state));

        if state != MulticastGroupState::Deleting {
            query = query.filter(dsl::time_deleted.is_null());
        }

        query
            .select(MulticastGroup::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List multicast groups matching any of the provided states.
    ///
    /// Used by RPW reconciler. For "Deleting" state, includes groups with
    /// `time_deleted` set so the RPW can clean them up.
    pub async fn multicast_groups_list_by_states(
        &self,
        opctx: &OpContext,
        states: &[MulticastGroupState],
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<MulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

        let mut query = paginated(dsl::multicast_group, dsl::id, pagparams)
            .filter(dsl::state.eq_any(states.to_vec()));

        if !states.contains(&MulticastGroupState::Deleting) {
            query = query.filter(dsl::time_deleted.is_null());
        }

        query
            .select(MulticastGroup::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Transition multicast group to "Active" state.
    ///
    /// This is used after successfully programming the dataplane (DPD) to mark
    /// the group as fully operational.
    ///
    /// Note: this is the only valid state transition via this API. To delete a
    /// group, use [`Self::mark_multicast_group_for_removal_if_no_members`] which
    /// handles the "Deleting" state transition along with setting `time_deleted`.
    pub async fn multicast_group_set_active(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> UpdateResult<()> {
        use nexus_db_schema::schema::multicast_group::dsl;

        let rows_updated = diesel::update(dsl::multicast_group)
            .filter(dsl::id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .set((
                dsl::state.eq(MulticastGroupState::Active),
                dsl::time_modified.eq(diesel::dsl::now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if rows_updated == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroup,
                &group_id.into_untyped_uuid(),
            ));
        }

        Ok(())
    }

    /// Allocate a new external multicast group.
    ///
    /// The external multicast IP is allocated from the specified pool or the
    /// default multicast pool.
    pub async fn multicast_group_create(
        &self,
        opctx: &OpContext,
        params: &MulticastGroupCreate,
        authz_pool: Option<authz::IpPool>,
    ) -> CreateResult<ExternalMulticastGroup> {
        self.allocate_external_multicast_group(
            opctx,
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
    ///
    /// See [`Self::multicast_group_fetch_on_conn`] for the connection-reusing
    /// variant.
    pub async fn multicast_group_fetch(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> LookupResult<ExternalMulticastGroup> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.multicast_group_fetch_on_conn(&conn, group_id.into_untyped_uuid())
            .await
    }

    /// Fetch an external multicast group using provided connection.
    pub async fn multicast_group_fetch_on_conn(
        &self,
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

    /// List multicast groups (fleet-scoped for visibility).
    pub async fn multicast_groups_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<ExternalMulticastGroup> {
        use nexus_db_schema::schema::multicast_group::dsl;

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

    /// Mark a multicast group for deletion, but only if it has no active members.
    ///
    /// This is a safe implicit deletion method. It atomically checks that no members
    /// exist before marking the group as "Deleting". This prevents race conditions
    /// where a concurrent join could create a member between a "list members"
    /// check and the mark-for-removal call.
    ///
    /// Returns:
    /// - `Ok(true)` if the group was marked for deletion (no members existed)
    /// - `Ok(false)` if the group still has members (not marked)
    /// - `Err` on database errors
    pub async fn mark_multicast_group_for_removal_if_no_members(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> Result<bool, external::Error> {
        use nexus_db_schema::schema::multicast_group;
        use nexus_db_schema::schema::multicast_group_member;
        let now = Utc::now();

        // Atomic: only mark `Deleting` if no active members exist.
        let rows = diesel::update(multicast_group::table)
            .filter(multicast_group::id.eq(group_id.into_untyped_uuid()))
            .filter(
                multicast_group::state
                    .eq(MulticastGroupState::Active)
                    .or(multicast_group::state
                        .eq(MulticastGroupState::Creating)),
            )
            .filter(multicast_group::time_deleted.is_null())
            .filter(diesel::dsl::not(diesel::dsl::exists(
                multicast_group_member::table
                    .filter(
                        multicast_group_member::external_group_id
                            .eq(group_id.into_untyped_uuid()),
                    )
                    .filter(multicast_group_member::time_deleted.is_null()),
            )))
            .set((
                multicast_group::state.eq(MulticastGroupState::Deleting),
                multicast_group::time_deleted.eq(now),
                multicast_group::time_modified.eq(now),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(rows > 0)
    }

    /// Delete a multicast group permanently.
    ///
    /// This should only be called by the RPW reconciler after DPD cleanup.
    /// Requires both `state=Deleting` and `time_deleted IS NOT NULL` as a
    /// safety check.
    pub async fn multicast_group_delete(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::multicast_group::dsl;

        let deleted_rows = diesel::delete(dsl::multicast_group)
            .filter(dsl::id.eq(group_id.into_untyped_uuid()))
            .filter(dsl::state.eq(MulticastGroupState::Deleting))
            .filter(dsl::time_deleted.is_not_null())
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if deleted_rows == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroup,
                &group_id.into_untyped_uuid(),
            ));
        }

        Ok(())
    }

    /// Allocate an external multicast group from an IP Pool.
    ///
    /// See [`Self::allocate_external_multicast_group_on_conn`] for the
    /// connection-reusing variant.
    pub(crate) async fn allocate_external_multicast_group(
        &self,
        opctx: &OpContext,
        params: MulticastGroupAllocationParams,
    ) -> CreateResult<ExternalMulticastGroup> {
        let group_id = Uuid::new_v4();

        // Determine if this is an SSM request (source_ips provided) or an
        // implicit ASM request (no sources, no explicit pool/IP)
        let sources_empty =
            params.source_ips.as_ref().map(|v| v.is_empty()).unwrap_or(true);
        let needs_ssm_pool =
            !sources_empty && params.pool.is_none() && params.ip.is_none();
        let needs_asm_pool =
            sources_empty && params.pool.is_none() && params.ip.is_none();

        // Select the appropriate pool:
        // - If `source_ips` provided without explicit pool/IP, find an SSM pool.
        // - If no `source_ips` and no explicit pool/IP, find an ASM pool.
        // - Otherwise (explicit pool or explicit IP provided), fall back to
        //   generic resolution via `resolve_pool_for_allocation` which validates
        //   linkage and type. ASM/SSM semantics are still enforced below.
        let authz_pool = if needs_ssm_pool {
            let (authz_pool, _) = self
                .ip_pools_fetch_ssm_multicast(opctx)
                .await
                .map_err(|_| {
                    external::Error::invalid_request(concat!(
                        "No SSM multicast pool linked to your silo. ",
                        "Create a multicast pool with SSM ranges ",
                        "(IPv4 232/8, IPv6 ff3x::/32) and link it to ",
                        "your silo, or provide an explicit SSM address.",
                    ))
                })?;
            opctx.authorize(authz::Action::CreateChild, &authz_pool).await?;
            authz_pool
        } else if needs_asm_pool {
            let (authz_pool, _) = self
                .ip_pools_fetch_asm_multicast(opctx)
                .await
                .map_err(|_| {
                    external::Error::invalid_request(concat!(
                        "No ASM multicast pool linked to your silo. ",
                        "Create a multicast pool with ASM ranges ",
                        "(IPv4 224/4 excluding 232/8, or IPv6 ffxx::/16 ",
                        "excluding ff3x::/32) and link it to your silo, ",
                        "or provide an explicit ASM address.",
                    ))
                })?;
            opctx.authorize(authz::Action::CreateChild, &authz_pool).await?;
            authz_pool
        } else {
            self.resolve_pool_for_allocation(
                opctx,
                params.pool,
                IpPoolType::Multicast,
            )
            .await?
        };

        debug!(
            opctx.log,
            "multicast group allocation";
            "pool_selection" => if needs_ssm_pool { "ssm" } else if needs_asm_pool { "asm" } else { "explicit" },
            "pool_id" => %authz_pool.id(),
        );

        // Enforce ASM/SSM semantics when allocating from a pool:
        // - If sources are provided without an explicit IP (implicit allocation),
        //   the pool must be SSM so we allocate an SSM address.
        // - If the pool is SSM and sources are empty/missing, reject.
        let pool_is_ssm =
            self.multicast_pool_is_ssm(opctx, authz_pool.id()).await?;

        // Note: When needs_ssm_pool was true, we already fetched an SSM pool,
        // so this check only triggers for explicitly-provided pools.
        if !sources_empty && params.ip.is_none() && !pool_is_ssm {
            let pool_id = authz_pool.id();
            return Err(external::Error::invalid_request(&format!(
                concat!(
                    "Cannot allocate SSM multicast group from ASM pool {}. ",
                    "Choose a multicast pool with SSM ranges ",
                    "(IPv4 232/8, IPv6 ff3x::/32) or provide an explicit ",
                    "SSM address."
                ),
                pool_id
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
        // groups are fleet-scoped and can span multiple projects/VPCs.
        let vni = Vni(external::Vni::DEFAULT_MULTICAST_VNI);

        // Create the incomplete group
        let data = IncompleteExternalMulticastGroup::new(
            IncompleteExternalMulticastGroupParams {
                id: group_id,
                name: Name(params.identity.name.clone()),
                description: params.identity.description.clone(),
                ip_pool_id: authz_pool.id(),
                explicit_address: params.ip,
                source_ips: source_ip_networks,
                mvlan: params.mvlan.map(|vlan_id| u16::from(vlan_id) as i16),
                vni,
                // Set DPD tag to the group UUID to ensure uniqueness across lifecycle.
                // This prevents tag collision when group names are reused.
                tag: Some(group_id.to_string()),
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

    /// Deallocate an external multicast group address for IP pool cleanup.
    ///
    /// This marks the group's IP address as deallocated by setting `time_deleted`,
    /// releasing it back to the pool. This is not the user-initiated deletion path.
    ///
    /// User-initiated deletion uses `mark_multicast_group_for_removal` which
    /// transitions to "Deleting" state for RPW cleanup before row removal.
    ///
    /// Returns `Ok(true)` if the group was deallocated, `Ok(false)` if it was
    /// already deleted (i.e., `time_deleted` was already set), `Err(_)` for any
    /// other condition including non-existent record.
    pub async fn deallocate_external_multicast_group(
        &self,
        opctx: &OpContext,
        group_id: MulticastGroupUuid,
    ) -> Result<bool, external::Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        self.deallocate_external_multicast_group_on_conn(
            &conn,
            group_id.into_untyped_uuid(),
        )
        .await
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
                    "multicast_ip" => %multicast_ip
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
        self.underlay_multicast_group_fetch_on_conn(
            &*self.pool_connection_authorized(opctx).await?,
            group_id,
        )
        .await
    }

    /// Fetch underlay multicast group using provided connection.
    pub async fn underlay_multicast_group_fetch_on_conn(
        &self,
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
    /// This should only be called by the RPW reconciler after DPD cleanup.
    /// Underlay groups don't have independent lifecycle, i.e. they're always
    /// deleted as part of cleaning up their parent external group.
    pub async fn underlay_multicast_group_delete(
        &self,
        opctx: &OpContext,
        group_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::underlay_multicast_group::dsl;

        let deleted_rows = diesel::delete(dsl::underlay_multicast_group)
            .filter(dsl::id.eq(group_id))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if deleted_rows == 0 {
            return Err(external::Error::not_found_by_id(
                ResourceType::MulticastGroup,
                &group_id,
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::Ipv4Addr;

    use nexus_types::identity::Resource;
    use omicron_common::address::{IpRange, Ipv4Range};
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{
        GenericUuid, InstanceUuid, PropolisUuid, SledUuid,
    };

    use crate::db::datastore::Error;
    use crate::db::datastore::LookupType;
    use crate::db::model::IpPool;
    use crate::db::model::{
        Generation, InstanceRuntimeState, IpPoolReservationType,
        IpPoolResource, IpPoolResourceType, IpVersion,
        MulticastGroupMemberState,
    };
    use crate::db::pub_test_utils::helpers::{
        SledUpdateBuilder, create_instance_with_vmm, create_project,
        create_stopped_instance_record,
    };
    use crate::db::pub_test_utils::{TestDatabase, multicast};

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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params1 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "first-group".parse().unwrap(),
                description: "First group".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };
        datastore
            .multicast_group_create(&opctx, &params1, Some(authz_pool.clone()))
            .await
            .expect("Should create first group");

        // Allocate second address
        let params2 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "second-group".parse().unwrap(),
                description: "Second group".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };
        datastore
            .multicast_group_create(&opctx, &params2, Some(authz_pool.clone()))
            .await
            .expect("Should create second group");

        // Third allocation should fail due to exhaustion
        let params3 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "third-group".parse().unwrap(),
                description: "Should fail".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };
        let result3 = datastore
            .multicast_group_create(&opctx, &params3, Some(authz_pool.clone()))
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params_default = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "auto-alloc-group".parse().unwrap(),
                description: "Group using default pool".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };

        let group_default = datastore
            .multicast_group_create(&opctx, &params_default, None)
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
        let params_explicit = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "explicit-alloc-group".parse().unwrap(),
                description: "Group with explicit pool".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };
        let group_explicit = datastore
            .multicast_group_create(&opctx, &params_explicit, None)
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
            .multicast_group_set_active(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group_default.id()),
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-group".parse().unwrap(),
                description: "Comprehensive test group".to_string(),
            },
            multicast_ip: Some("224.1.3.3".parse().unwrap()),
            source_ips: None,
            mvlan: None,
        };

        let external_group = datastore
            .multicast_group_create(&opctx, &params, Some(authz_pool.clone()))
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
            )
            .await
            .expect("Should create underlay group");

        // Verify underlay group properties
        assert!(underlay_group.multicast_ip.ip().is_ipv6());

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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "parent-id-test-group".parse().unwrap(),
                description: "Group for parent_id testing".to_string(),
            },
            multicast_ip: Some("224.3.1.5".parse().unwrap()),
            source_ips: None,
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(&opctx, &params, Some(authz_pool.clone()))
            .await
            .expect("Should create multicast group");

        // Create test sled and instances
        let sled_id = create_test_sled(&datastore).await;
        let instance_record_1 = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "test-instance-1",
        )
        .await;
        let parent_id_1 = instance_record_1.as_untyped_uuid();
        let instance_record_2 = create_stopped_instance_record(
            &opctx,
            &datastore,
            &authz_project,
            "test-instance-2",
        )
        .await;
        let parent_id_2 = instance_record_2.as_untyped_uuid();
        let instance_record_3 = create_stopped_instance_record(
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
                    generation: Generation::from(
                        instance1.runtime().generation.next(),
                    ),
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
                    generation: Generation::from(
                        instance2.runtime().generation.next(),
                    ),
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
                    generation: Generation::from(
                        instance3.runtime().generation.next(),
                    ),
                    time_updated: Utc::now(),
                    time_last_auto_restarted: None,
                },
            )
            .await
            .expect("Should set instance3 runtime state");

        // Transition group to "Active" state before adding members
        datastore
            .multicast_group_set_active(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
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
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_1),
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
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_1),
            )
            .await
            .expect("Should remove first member again");

        datastore
            .multicast_group_member_detach_by_group_and_instance(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(*parent_id_2),
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
            create_project(&opctx, &datastore, "dup-test-proj").await;
        let sled_id = create_test_sled(&datastore).await;
        let instance_record = create_stopped_instance_record(
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
                    generation: Generation::from(
                        instance.runtime().generation.next(),
                    ),
                    time_updated: Utc::now(),
                    time_last_auto_restarted: None,
                },
            )
            .await
            .expect("Should set instance runtime state");

        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "duplicate-test-group".parse().unwrap(),
                description: "Group for duplicate testing".to_string(),
            },
            multicast_ip: Some("224.3.1.5".parse().unwrap()),
            source_ips: None,
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(&opctx, &params, Some(authz_pool.clone()))
            .await
            .expect("Should create multicast group");

        // Transition group to "Active" state before adding members
        datastore
            .multicast_group_set_active(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let group_params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "state-test-group".parse().unwrap(),
                description: "Group for testing member state transitions"
                    .to_string(),
            },
            multicast_ip: None, // Let it allocate from pool
            source_ips: None,
            mvlan: None,
        };
        let group = datastore
            .multicast_group_create(
                &opctx,
                &group_params,
                Some(authz_pool.clone()),
            )
            .await
            .expect("Should create multicast group");

        // Create test project and instance (datastore-only)
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "state-test-proj").await;
        let sled_id = create_test_sled(&datastore).await;
        let (instance, _vmm) = create_instance_with_vmm(
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
            .multicast_group_set_active(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
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

        // Case: Transition from "Joining" → "Joined" (simulating what the reconciler would do)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
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

        // Case: Transition member to "Left" state (without permanent deletion)
        datastore
            .multicast_group_member_set_state(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should transition to 'Left' state");

        // Verify member is now in "Left" state
        let all_members = datastore
            .multicast_group_members_list_by_id(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
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
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                InstanceUuid::from_untyped_uuid(test_instance_id),
                MulticastGroupMemberState::Left,
            )
            .await
            .expect("Should transition to Left");

        // Member should still exist in database and be in "Left" state
        let members = datastore
            .multicast_group_members_list_by_id(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
                pagparams,
            )
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "reuse-test".parse().unwrap(),
                description: "Group for IP reuse test".to_string(),
            },
            multicast_ip: Some(target_ip),
            source_ips: None,
            mvlan: None,
        };

        let group1 = datastore
            .multicast_group_create(&opctx, &params, Some(authz_pool.clone()))
            .await
            .expect("Should create first group");
        assert_eq!(group1.multicast_ip.ip(), target_ip);

        // Delete the group completely (time_deleted set)
        let deleted = datastore
            .deallocate_external_multicast_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
            )
            .await
            .expect("Should deallocate group");
        assert_eq!(deleted, true, "Should successfully deallocate the group");

        // Create another group with the same IP - should succeed due to time_deleted filtering
        let params2 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "reuse-test-2".parse().unwrap(),
                description: "Second group reusing same IP".to_string(),
            },
            multicast_ip: Some(target_ip),
            source_ips: None,
            mvlan: None,
        };

        let group2 = datastore
            .multicast_group_create(
                &opctx,
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params1 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "cycle-test-1".parse().unwrap(),
                description: "First group to exhaust pool".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };

        let group1 = datastore
            .multicast_group_create(&opctx, &params1, Some(authz_pool.clone()))
            .await
            .expect("Should create first group");
        let allocated_ip = group1.multicast_ip.ip();

        // Try to create another group - should fail due to exhaustion
        let params2 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "cycle-test-2".parse().unwrap(),
                description: "Second group should fail".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };

        let result2 = datastore
            .multicast_group_create(&opctx, &params2, Some(authz_pool.clone()))
            .await;
        assert!(
            result2.is_err(),
            "Second group creation should fail due to pool exhaustion"
        );

        // Delete the first group to free up the IP
        let deleted = datastore
            .deallocate_external_multicast_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
            )
            .await
            .expect("Should deallocate first group");
        assert_eq!(deleted, true, "Should successfully deallocate the group");

        // Now creating a new group should succeed
        let params3 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "cycle-test-3".parse().unwrap(),
                description: "Third group should succeed after deletion"
                    .to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };

        let group3 = datastore
            .multicast_group_create(&opctx, &params3, Some(authz_pool.clone()))
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "dealloc-test".parse().unwrap(),
                description: "Group for deallocation testing".to_string(),
            },
            multicast_ip: None,
            source_ips: None,
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(&opctx, &params, Some(authz_pool.clone()))
            .await
            .expect("Should create multicast group");

        // Deallocate existing group - should return true
        let result1 = datastore
            .deallocate_external_multicast_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .expect("Deallocation should succeed");
        assert_eq!(
            result1, true,
            "Deallocating existing group should return true"
        );

        // Deallocate the same group again - should return false (already deleted)
        let result2 = datastore
            .deallocate_external_multicast_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .expect("Second deallocation should succeed but return false");
        assert_eq!(
            result2, false,
            "Deallocating already-deleted group should return false"
        );

        // Try to deallocate non-existent group - should return error
        let fake_id = Uuid::new_v4();
        let result3 = datastore
            .deallocate_external_multicast_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_id),
            )
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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
        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fetch-test-group".parse().unwrap(),
                description: "Test group for fetch operations".to_string(),
            },
            multicast_ip: Some("224.100.10.5".parse().unwrap()),
            source_ips: Some(vec![
                "10.0.0.1".parse().unwrap(),
                "10.0.0.2".parse().unwrap(),
            ]),
            mvlan: None,
        };

        let group = datastore
            .multicast_group_create(&opctx, &params, Some(authz_pool))
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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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

        // Create fleet-scoped multicast groups
        let params_1 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fleet-group-1".parse().unwrap(),
                description: "Fleet-wide group 1".to_string(),
            },
            multicast_ip: Some("224.100.20.10".parse().unwrap()),
            source_ips: None,
            mvlan: None,
        };

        let params_2 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fleet-group-2".parse().unwrap(),
                description: "Fleet-wide group 2".to_string(),
            },
            multicast_ip: Some("224.100.20.11".parse().unwrap()),
            source_ips: None,
            mvlan: None,
        };

        let params_3 = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "fleet-group-3".parse().unwrap(),
                description: "Fleet-wide group 3".to_string(),
            },
            multicast_ip: Some("224.100.20.12".parse().unwrap()),
            source_ips: None,
            mvlan: None,
        };

        // Create groups (all are fleet-scoped)
        datastore
            .multicast_group_create(&opctx, &params_1, Some(authz_pool.clone()))
            .await
            .expect("Should create fleet-group-1");

        datastore
            .multicast_group_create(&opctx, &params_2, Some(authz_pool.clone()))
            .await
            .expect("Should create fleet-group-2");

        datastore
            .multicast_group_create(&opctx, &params_3, Some(authz_pool))
            .await
            .expect("Should create fleet-group-3");

        // List all groups (fleet-scoped) - should get 3 groups
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
            .expect("Should list all fleet-scoped groups");

        assert_eq!(groups.len(), 3, "Should have 3 fleet-scoped groups");

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
                IpPool::new_multicast(
                    &pool_identity,
                    IpVersion::V4,
                    IpPoolReservationType::ExternalSilos,
                ),
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

        let params = MulticastGroupCreate {
            identity: IdentityMetadataCreateParams {
                name: "state-test-group".parse().unwrap(),
                description: "Test group for state transitions".to_string(),
            },
            multicast_ip: Some("224.100.30.5".parse().unwrap()),
            source_ips: None,
            mvlan: None,
        };

        // Create group - starts in "Creating" state
        let group = datastore
            .multicast_group_create(&opctx, &params, Some(authz_pool))
            .await
            .expect("Should create multicast group");

        assert_eq!(group.state, MulticastGroupState::Creating);

        // Test transition to "Active"
        datastore
            .multicast_group_set_active(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
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
        // Since this group has no members, it should be marked for deletion
        let marked = datastore
            .mark_multicast_group_for_removal_if_no_members(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group.id()),
            )
            .await
            .expect("Should transition to 'Deleting'");
        assert!(marked, "Group with no members should be marked for deletion");

        // Note: After marking for removal, group has `time_deleted` set,
        // so it won't show up in regular fetch (which filters `time_deleted IS NULL`).
        // We can verify by listing groups in Deleting state which includes deleted groups.
        let deleting_groups = datastore
            .multicast_groups_list_by_state(
                &opctx,
                MulticastGroupState::Deleting,
                &DataPageParams::max_page(),
            )
            .await
            .expect("Should list deleting groups");

        assert_eq!(deleting_groups.len(), 1);
        assert_eq!(deleting_groups[0].id(), group.id());
        assert_eq!(deleting_groups[0].state, MulticastGroupState::Deleting);
        assert!(deleting_groups[0].time_deleted().is_some());

        // Test trying to update non-existent group
        let fake_id = Uuid::new_v4();
        let result = datastore
            .multicast_group_set_active(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(fake_id),
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
            .deallocate_external_multicast_group(
                &opctx,
                MulticastGroupUuid::from_untyped_uuid(group1.id()),
            )
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
}
