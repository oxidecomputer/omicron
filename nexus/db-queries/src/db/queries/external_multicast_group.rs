// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries for allocating external, customer-facing  multicast groups from IP
//! pools.
//!
//! Based on [`super::external_ip`] allocation code, adapted for multicast
//! group semantics.

use chrono::{DateTime, Utc};
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::{Column, QueryResult, RunQueryDsl, sql_types};
use ipnetwork::IpNetwork;
use uuid::Uuid;

use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema;

use crate::db::model::{
    ExternalMulticastGroup, Generation, IncompleteExternalMulticastGroup,
    MulticastGroupState, Name, Vni,
};
use crate::db::true_or_cast_error::matches_sentinel;

const REALLOCATION_WITH_DIFFERENT_MULTICAST_GROUP_SENTINEL: &'static str =
    "Reallocation of multicast group with different configuration";

/// Converts multicast group allocation errors to external errors.
pub fn from_diesel(
    e: diesel::result::Error,
) -> omicron_common::api::external::Error {
    let sentinels = [REALLOCATION_WITH_DIFFERENT_MULTICAST_GROUP_SENTINEL];
    if let Some(sentinel) = matches_sentinel(&e, &sentinels) {
        match sentinel {
            REALLOCATION_WITH_DIFFERENT_MULTICAST_GROUP_SENTINEL => {
                return omicron_common::api::external::Error::invalid_request(
                    "Re-allocating multicast group with different configuration",
                );
            }
            // Fall-through to the generic error conversion.
            _ => {}
        }
    }

    nexus_db_errors::public_error_from_diesel(
        e,
        nexus_db_errors::ErrorHandler::Server,
    )
}

/// Query to allocate next available external multicast group address from IP pools.
///
/// Similar pattern to [`super::external_ip::NextExternalIp`] but for multicast
/// addresses. Handles pool-based allocation, explicit address requests, and
/// idempotency.
pub struct NextExternalMulticastGroup {
    group: IncompleteExternalMulticastGroup,
    now: DateTime<Utc>,
}

impl NextExternalMulticastGroup {
    pub fn new(group: IncompleteExternalMulticastGroup) -> Self {
        let now = Utc::now();
        Self { group, now }
    }

    fn push_next_multicast_ip_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql("SELECT ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.group.id)?;
        out.push_sql(" AS id, ");

        out.push_bind_param::<sql_types::Text, Name>(&self.group.name)?;
        out.push_sql(" AS name, ");
        out.push_bind_param::<sql_types::Text, String>(
            &self.group.description,
        )?;
        out.push_sql(" AS description, ");

        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.now,
        )?;
        out.push_sql(" AS time_created, ");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.now,
        )?;
        out.push_sql(" AS time_modified, ");

        out.push_bind_param::<sql_types::Nullable<sql_types::Timestamptz>, Option<DateTime<Utc>>>(&None)?;
        out.push_sql(" AS time_deleted, ");

        // Pool ID from the candidates subquery (like external IP)
        out.push_sql("ip_pool_id, ");

        // Pool range ID from the candidates subquery
        out.push_sql("ip_pool_range_id, ");

        // VNI
        out.push_bind_param::<sql_types::Int4, Vni>(&self.group.vni)?;
        out.push_sql(" AS vni, ");

        // The multicast IP comes from the candidates subquery
        out.push_sql("candidate_ip AS multicast_ip, ");

        // Handle source IPs array
        out.push_sql("ARRAY[");
        for (i, source_ip) in self.group.source_ips.iter().enumerate() {
            if i > 0 {
                out.push_sql(", ");
            }
            out.push_bind_param::<sql_types::Inet, ipnetwork::IpNetwork>(
                source_ip,
            )?;
        }
        out.push_sql("]::inet[] AS source_ips, ");

        // MVLAN for external uplink forwarding
        out.push_bind_param::<sql_types::Nullable<sql_types::Int2>, Option<i16>>(&self.group.mvlan)?;
        out.push_sql(" AS mvlan, ");

        out.push_bind_param::<sql_types::Nullable<sql_types::Uuid>, Option<Uuid>>(&None)?;
        out.push_sql(" AS underlay_group_id, ");

        out.push_bind_param::<sql_types::Nullable<sql_types::Text>, Option<String>>(&self.group.tag)?;
        out.push_sql(" AS tag, ");

        // New multicast groups start in "Creating" state (RPW pattern)
        out.push_bind_param::<nexus_db_schema::enums::MulticastGroupStateEnum, MulticastGroupState>(&MulticastGroupState::Creating)?;
        out.push_sql(" AS state, ");

        out.push_sql("nextval('omicron.public.multicast_group_version') AS version_added, ");
        out.push_bind_param::<sql_types::Nullable<sql_types::Int8>, Option<Generation>>(&None)?;
        out.push_sql(" AS version_removed");

        // FROM the candidates subquery with LEFT JOIN (like external IP)
        out.push_sql(" FROM (");
        self.push_address_candidates_subquery(out.reborrow())?;
        out.push_sql(") LEFT OUTER JOIN ");
        schema::multicast_group::table.walk_ast(out.reborrow())?;
        out.push_sql(
            " ON (multicast_ip = candidate_ip AND time_deleted IS NULL)",
        );
        out.push_sql(
            " WHERE candidate_ip IS NOT NULL AND multicast_ip IS NULL LIMIT 1",
        );

        Ok(())
    }

    fn push_address_candidates_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::ip_pool_range::dsl;

        out.push_sql("SELECT ");
        out.push_identifier(dsl::ip_pool_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" AS ip_pool_range_id, ");

        // Handle explicit address vs automatic allocation
        if let Some(explicit_addr) = &self.group.explicit_address {
            out.push_sql("CASE ");
            out.push_identifier(dsl::first_address::NAME)?;
            out.push_sql(" <= ");
            out.push_bind_param::<sql_types::Inet, IpNetwork>(explicit_addr)?;
            out.push_sql(" AND ");
            out.push_bind_param::<sql_types::Inet, IpNetwork>(explicit_addr)?;
            out.push_sql(" <= ");
            out.push_identifier(dsl::last_address::NAME)?;
            out.push_sql(" WHEN TRUE THEN ");
            out.push_bind_param::<sql_types::Inet, IpNetwork>(explicit_addr)?;
            out.push_sql(" ELSE NULL END");
        } else {
            // Generate series of candidate IPs (like external IP does)
            out.push_identifier(dsl::first_address::NAME)?;
            out.push_sql(" + generate_series(0, ");
            out.push_identifier(dsl::last_address::NAME)?;
            out.push_sql(" - ");
            out.push_identifier(dsl::first_address::NAME)?;
            out.push_sql(")");
        }

        out.push_sql(" AS candidate_ip FROM ");
        schema::ip_pool_range::table.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(dsl::ip_pool_id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.group.ip_pool_id)?;
        out.push_sql(" AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL");
        // Filter for multicast address ranges (224.0.0.0/4 for IPv4,
        // ff00::/8 for IPv6)
        out.push_sql(" AND (");
        out.push_identifier(dsl::first_address::NAME)?;
        out.push_sql(" << '224.0.0.0/4'::inet OR ");
        out.push_identifier(dsl::first_address::NAME)?;
        out.push_sql(" << 'ff00::/8'::inet)");

        Ok(())
    }

    fn push_prior_allocation_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        out.push_sql("SELECT * FROM ");
        schema::multicast_group::table.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE id = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.group.id)?;
        out.push_sql(" AND time_deleted IS NULL");
        Ok(())
    }

    /// Push a subquery to update the `ip_pool_range` table's rcgen, if we've
    /// successfully allocated a multicast group from that range.
    ///
    /// This prevents race conditions where a range is deleted while a
    /// multicast group allocation is in progress. The range deletion checks
    /// the rcgen value, and this increment ensures it changes when an
    /// allocation occurs.
    fn push_update_ip_pool_range_subquery<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> QueryResult<()> {
        use schema::ip_pool_range::dsl;
        out.push_sql("UPDATE ");
        schema::ip_pool_range::table.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.now,
        )?;
        out.push_sql(", ");
        out.push_identifier(dsl::rcgen::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(dsl::rcgen::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(" = (SELECT ip_pool_range_id FROM next_external_multicast_group) AND ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(" IS NULL RETURNING ");
        out.push_identifier(dsl::id::NAME)?;
        Ok(())
    }
}

impl QueryFragment<Pg> for NextExternalMulticastGroup {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Create CTE for candidate multicast group
        out.push_sql("WITH next_external_multicast_group AS (");
        self.push_next_multicast_ip_subquery(out.reborrow())?;
        out.push_sql("), ");

        // Check for existing allocation (idempotency)
        out.push_sql("previously_allocated_group AS (");
        self.push_prior_allocation_subquery(out.reborrow())?;
        out.push_sql("), ");

        // Insert new record or return existing one
        out.push_sql("multicast_group AS (");
        out.push_sql("INSERT INTO ");
        schema::multicast_group::table.walk_ast(out.reborrow())?;
        out.push_sql(
            " (id, name, description, time_created, time_modified, time_deleted, ip_pool_id, ip_pool_range_id, vni, multicast_ip, source_ips, mvlan, underlay_group_id, tag, state, version_added, version_removed)
             SELECT id, name, description, time_created, time_modified, time_deleted, ip_pool_id, ip_pool_range_id, vni, multicast_ip, source_ips, mvlan, underlay_group_id, tag, state, version_added, version_removed FROM next_external_multicast_group
                WHERE NOT EXISTS (SELECT 1 FROM previously_allocated_group)
                RETURNING id, name, description, time_created, time_modified, time_deleted, ip_pool_id, ip_pool_range_id, vni, multicast_ip, source_ips, mvlan, underlay_group_id, tag, state, version_added, version_removed",
        );
        out.push_sql("), ");

        // Update the IP pool range's rcgen to prevent race conditions with range deletion
        out.push_sql("updated_pool_range AS (");
        self.push_update_ip_pool_range_subquery(out.reborrow())?;
        out.push_sql(") ");

        // Return either the newly inserted or previously allocated group
        out.push_sql(
            "SELECT id, name, description, time_created, time_modified, time_deleted, ip_pool_id, ip_pool_range_id, vni, multicast_ip, source_ips, mvlan, underlay_group_id, tag, state, version_added, version_removed FROM previously_allocated_group
            UNION ALL
            SELECT id, name, description, time_created, time_modified, time_deleted, ip_pool_id, ip_pool_range_id, vni, multicast_ip, source_ips, mvlan, underlay_group_id, tag, state, version_added, version_removed FROM multicast_group",
        );

        Ok(())
    }
}

impl QueryId for NextExternalMulticastGroup {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Query for NextExternalMulticastGroup {
    type SqlType = <<ExternalMulticastGroup as
        diesel::Selectable<Pg>>::SelectExpression as diesel::Expression>::SqlType;
}

impl RunQueryDsl<DbConnection> for NextExternalMulticastGroup {}
