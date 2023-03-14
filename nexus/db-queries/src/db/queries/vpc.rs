// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries for inserting a candidate, incomplete VPC

use crate::db::model::Generation;
use crate::db::model::IncompleteVpc;
use crate::db::model::Name;
use crate::db::model::Vni;
use crate::db::queries::next_item::DefaultShiftGenerator;
use crate::db::queries::next_item::NextItem;
use crate::db::schema::vpc;
use crate::db::schema::vpc::dsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::Column;
use diesel::Insertable;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use uuid::Uuid;

/// A query used to insert a candidate VPC into the database.
///
/// This query mostly just inserts the data provided in the `IncompleteVpc`
/// argument. However, it selects a random, available VNI for the VPC, should
/// one be available.
#[derive(Debug)]
pub struct InsertVpcQuery {
    vpc: IncompleteVpc,
    vni_subquery: NextVni,
}

impl InsertVpcQuery {
    pub fn new(vpc: IncompleteVpc) -> Self {
        let vni_subquery = NextVni::new(vpc.vni);
        Self { vpc, vni_subquery }
    }
}

impl QueryId for InsertVpcQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Insertable<vpc::table> for InsertVpcQuery {
    type Values = InsertVpcQueryValues;

    fn values(self) -> Self::Values {
        InsertVpcQueryValues(self)
    }
}

impl QueryFragment<Pg> for InsertVpcQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.push_sql("SELECT ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.vpc.identity.id)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Text, Name>(&self.vpc.identity.name)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Text, String>(
            &self.vpc.identity.description,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.vpc.identity.time_created,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.vpc.identity.time_modified,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<
            sql_types::Nullable<sql_types::Timestamptz>,
            Option<DateTime<Utc>>
        >(&self.vpc.identity.time_deleted)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.vpc.project_id)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::project_id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Uuid, Uuid>(
            &self.vpc.system_router_id,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::system_router_id::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Text, Name>(&self.vpc.dns_name)?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::dns_name::NAME)?;
        out.push_sql(", ");

        out.push_sql("(");
        self.vni_subquery.walk_ast(out.reborrow())?;
        out.push_sql(") AS ");
        out.push_identifier(dsl::vni::NAME)?;
        out.push_sql(", ");

        // TODO-performance: It might be possible to replace this with a
        // NextItem query, but Cockroach doesn't currently support the 128-bit
        // integers we'd need to create `/48` prefixes by adding a random offset
        // to `fd00::/8`. For now, we do that with a retry-loop in the
        // application.
        out.push_bind_param::<sql_types::Inet, IpNetwork>(
            &self.vpc.ipv6_prefix,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::ipv6_prefix::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Int8, Generation>(
            &self.vpc.firewall_gen,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::firewall_gen::NAME)?;
        out.push_sql(", ");

        out.push_bind_param::<sql_types::Int8, Generation>(
            &self.vpc.subnet_gen,
        )?;
        out.push_sql(" AS ");
        out.push_identifier(dsl::subnet_gen::NAME)?;

        Ok(())
    }
}

pub struct InsertVpcQueryValues(InsertVpcQuery);

impl QueryId for InsertVpcQueryValues {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl diesel::insertable::CanInsertInSingleQuery<Pg> for InsertVpcQueryValues {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(1)
    }
}

impl QueryFragment<Pg> for InsertVpcQueryValues {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.push_sql("(");
        out.push_identifier(dsl::id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::name::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::description::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_created::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_modified::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::time_deleted::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::project_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::system_router_id::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::dns_name::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::vni::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::ipv6_prefix::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::firewall_gen::NAME)?;
        out.push_sql(", ");
        out.push_identifier(dsl::subnet_gen::NAME)?;
        out.push_sql(")");
        self.0.walk_ast(out)
    }
}

/// A `NextItem` query to select a Geneve Virtual Network Identifier (VNI) for a
/// new VPC.
#[derive(Debug, Clone, Copy)]
struct NextVni {
    inner: NextItem<vpc::table, Vni, dsl::vni>,
}

impl NextVni {
    fn new(vni: Vni) -> Self {
        let base_u32 = u32::from(vni.0);
        // The valid range is [0, 1 << 24], so the maximum shift is whatever
        // gets us to 1 << 24, and the minimum is whatever gets us back to the
        // minimum guest VNI.
        let max_shift = i64::from(external::Vni::MAX_VNI - base_u32);
        let min_shift = i64::from(
            -i32::try_from(base_u32 - external::Vni::MIN_GUEST_VNI)
                .expect("Expected a valid VNI at this point"),
        );
        let generator =
            DefaultShiftGenerator { base: vni, max_shift, min_shift };
        let inner = NextItem::new_unscoped(generator);
        Self { inner }
    }
}

delegate_query_fragment_impl!(NextVni);
