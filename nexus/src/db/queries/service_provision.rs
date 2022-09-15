// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning services.

use crate::db::model::Name;
use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::model::Sled;
use crate::db::pool::DbConnection;
use crate::db::schema;
use chrono::DateTime;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::Column;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::QueryResult;
use diesel::RunQueryDsl;
use diesel::SelectableHelper;
use uuid::Uuid;

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type ServiceFromClause = FromClause<schema::service::table>;
const SERVICE_FROM_CLAUSE: ServiceFromClause = ServiceFromClause::new();

//trait Queryable: Query + QueryFragment<Pg> {}
//impl<T: Query + QueryFragment<Pg>> Queryable for T {}

trait Queryable: Query + QueryFragment<Pg> {}

impl<T> Queryable for T
where
    T: Query + QueryFragment<Pg>
{}

/*
trait QueryableClone<ST> {
    fn clone_box(&self) -> Box<dyn Queryable<SqlType = ST>>;
}

impl<T, ST> QueryableClone<ST> for T
where
    T: 'static + Queryable<SqlType = ST> + Clone,
{
    fn clone_box(&self) -> Box<dyn Queryable<SqlType = ST>> {
        Box::new(self.clone())
    }
}

impl<ST> Clone for Box<dyn Queryable<SqlType = ST>> {
    fn clone(&self) -> Box<dyn Queryable<SqlType = ST>> {
        self.clone_box()
    }
}
*/

/// Represents a sub-query within a CTE.
///
/// For an expression like:
///
/// ```sql
/// WITH
///     foo as ...,
///     bar as ...,
/// SELECT * FROM bar;
/// ```
///
/// This trait represents one of the sub-query arms, such as "foo as ..." or
/// "bar as ...".
trait SubQuery {
    // TODO: This query should have an associated SQL type!

    // TODO: Could be associated constant, maybe?
    fn name(&self) -> &'static str;
    fn query(&self) -> &dyn QueryFragment<Pg>;
}

/// A thin wrapper around a [`SubQuery`].
///
/// Used to avoid orphan rules while creating blanket implementations.
struct CteSubquery(Box<dyn SubQuery>);

impl QueryId for CteSubquery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for CteSubquery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql(self.0.name());
        out.push_sql(" AS (");
        self.0.query().walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

struct CteBuilder {
    subqueries: Vec<CteSubquery>,
}

impl CteBuilder {
    fn new() -> Self {
        Self {
            subqueries: vec![],
        }
    }

    fn add_subquery<Q: SubQuery + 'static>(mut self, subquery: Q) -> Self {
        self.subqueries.push(
            CteSubquery(Box::new(subquery))
        );
        self
    }

    fn build(mut self, statement: Box<dyn QueryFragment<Pg>>) -> Cte {
        Cte {
            subqueries: self.subqueries,
            statement
        }
    }
}

struct Cte {
    subqueries: Vec<CteSubquery>,
    statement: Box<dyn QueryFragment<Pg>>,
}

impl QueryFragment<Pg> for Cte {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql("WITH ");
        for (pos, query) in self.subqueries.iter().enumerate() {
            query.walk_ast(out.reborrow())?;
            if pos == self.subqueries.len() - 1 {
                out.push_sql(" ");
            } else {
                out.push_sql(", ");
            }
        }
        self.statement.walk_ast(out.reborrow())?;
        Ok(())
    }
}

// ----------------------------- //

// TODO: I want this to be as lightweight to make as possible!
struct SledAllocationPoolSubquery {
    query: Box<dyn Queryable<SqlType = sql_types::Uuid>>,
}

impl SledAllocationPoolSubquery {
    fn new() -> Self {
        use crate::db::schema::sled::dsl;
        Self {
            query: Box::new(
                dsl::sled
                    .filter(dsl::time_deleted.is_null())
                    // TODO: Filter by rack?
                    .select(dsl::id)
            )
        }
    }
}

impl SubQuery for SledAllocationPoolSubquery {
    fn name(&self) -> &'static str {
        "sled_allocation_pool"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

struct PreviouslyAllocatedServices {
    query: Box<dyn Queryable<SqlType = crate::db::schema::service::SqlType>>,
}

impl PreviouslyAllocatedServices {
    fn new(allocation_pool: &SledAllocationPoolSubquery) -> Self {
        use crate::db::schema::service::dsl;
        Self {
            query: Box::new(
                dsl::service
                    .filter(dsl::kind.eq(ServiceKind::Nexus))
//                    .filter(dsl::sled_id.eq_any(allocation_pool))
            )
        }
    }
}

impl SubQuery for PreviouslyAllocatedServices {
    fn name(&self) -> &'static str {
        "previously_allocated_services"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

/// Provision services of a particular type within a rack.
///
/// TODO: Document
pub struct ServiceProvision {
    now: DateTime<Utc>,

    cte: Cte,
}

impl ServiceProvision {
    pub fn new() -> Self {
        let now = Utc::now();
        let sled_allocation_pool = SledAllocationPoolSubquery::new();
        let previously_allocated_services = PreviouslyAllocatedServices::new(&sled_allocation_pool);

        // TODO: Reference prior subquery?
        use crate::db::schema::sled::dsl;
        let final_select = Box::new(dsl::sled.filter(dsl::time_deleted.is_null()));

        let cte = CteBuilder::new()
            .add_subquery(sled_allocation_pool)
            .add_subquery(previously_allocated_services)
            .build(final_select);

        Self {
            now,
            cte,
        }
    }
}


impl QueryId for ServiceProvision {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for ServiceProvision {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        self.cte.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl Query for ServiceProvision {
    type SqlType = <<Service as
        diesel::Selectable<Pg>>::SelectExpression as diesel::Expression>::SqlType;
}

impl RunQueryDsl<DbConnection> for ServiceProvision {}

#[cfg(test)]
mod tests {
    use crate::context::OpContext;
    use crate::db::datastore::DataStore;
    use crate::db::identity::Resource;
    use crate::db::model::IpKind;
    use crate::db::model::IpPool;
    use crate::db::model::IpPoolRange;
    use crate::db::model::Name;
    use crate::external_api::shared::IpRange;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use dropshot::test_util::LogContext;
    use nexus_test_utils::db::test_setup_database;
    use nexus_test_utils::RACK_UUID;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::db::CockroachInstance;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use uuid::Uuid;

    struct TestContext {
        logctx: LogContext,
        opctx: OpContext,
        db: CockroachInstance,
        db_datastore: Arc<DataStore>,
    }

    impl TestContext {
        async fn new(test_name: &str) -> Self {
            let logctx = dev::test_setup_log(test_name);
            let log = logctx.log.new(o!());
            let db = test_setup_database(&log).await;
            crate::db::datastore::datastore_test(&logctx, &db).await;
            let cfg = crate::db::Config { url: db.pg_config().clone() };
            let pool = Arc::new(crate::db::Pool::new(&cfg));
            let db_datastore =
                Arc::new(crate::db::DataStore::new(Arc::clone(&pool)));
            let opctx =
                OpContext::for_tests(log.new(o!()), db_datastore.clone());
            Self { logctx, opctx, db, db_datastore }
        }

        async fn create_ip_pool_internal(
            &self,
            name: &str,
            range: IpRange,
            project_id: Option<Uuid>,
            rack_id: Option<Uuid>,
        ) {
            let pool = IpPool::new(
                &IdentityMetadataCreateParams {
                    name: String::from(name).parse().unwrap(),
                    description: format!("ip pool {}", name),
                },
                project_id,
                rack_id,
            );

            diesel::insert_into(crate::db::schema::ip_pool::dsl::ip_pool)
                .values(pool.clone())
                .execute_async(
                    self.db_datastore
                        .pool_authorized(&self.opctx)
                        .await
                        .unwrap(),
                )
                .await
                .expect("Failed to create IP Pool");

            let pool_range = IpPoolRange::new(&range, pool.id(), project_id);
            diesel::insert_into(
                crate::db::schema::ip_pool_range::dsl::ip_pool_range,
            )
            .values(pool_range)
            .execute_async(
                self.db_datastore.pool_authorized(&self.opctx).await.unwrap(),
            )
            .await
            .expect("Failed to create IP Pool range");
        }

        async fn create_rack_ip_pool(
            &self,
            name: &str,
            range: IpRange,
            rack_id: Uuid,
        ) {
            self.create_ip_pool_internal(
                name,
                range,
                /* project_id= */ None,
                Some(rack_id),
            )
            .await;
        }

        async fn create_ip_pool(
            &self,
            name: &str,
            range: IpRange,
            project_id: Option<Uuid>,
        ) {
            self.create_ip_pool_internal(
                name, range, project_id, /* rack_id= */ None,
            )
            .await;
        }

        async fn success(mut self) {
            self.db.cleanup().await.unwrap();
            self.logctx.cleanup_successful();
        }
    }

    #[tokio::test]
    async fn test_next_external_ip_allocation_and_exhaustion() {
        let context =
            TestContext::new("test_next_external_ip_allocation_and_exhaustion")
                .await;
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 1),
        ))
        .unwrap();
        context.create_ip_pool("p0", range, None).await;
        let project_id = Uuid::new_v4();
        for first_port in
            (0..super::MAX_PORT).step_by(super::NUM_SOURCE_NAT_PORTS)
        {
            let id = Uuid::new_v4();
            let instance_id = Uuid::new_v4();
            let ip = context
                .db_datastore
                .allocate_instance_snat_ip(
                    &context.opctx,
                    id,
                    project_id,
                    instance_id,
                )
                .await
                .expect("Failed to allocate instance external IP address");
            assert_eq!(ip.ip.ip(), range.first_address());
            assert_eq!(ip.first_port.0, first_port as u16);
            assert_eq!(
                ip.last_port.0,
                (first_port + (super::NUM_SOURCE_NAT_PORTS - 1) as i32) as u16
            );
        }

        // The next allocation should fail, due to IP exhaustion
        let instance_id = Uuid::new_v4();
        let err = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
            )
            .await
            .expect_err(
                "An error should be received when the IP pools are exhausted",
            );
        assert_eq!(
            err,
            Error::InvalidRequest {
                message: String::from("No external IP addresses available"),
            }
        );
        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_out_of_order_allocation_ok() {
        let context = TestContext::new(
            "test_next_external_ip_out_of_order_allocation_ok",
        )
        .await;
        // Need a larger range, since we're currently limited to the whole port
        // range for each external IP.
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", range, None).await;

        // TODO-completess: Implementing Iterator for IpRange would be nice.
        let addresses = [
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(10, 0, 0, 3),
        ];
        let ports = (0..super::MAX_PORT).step_by(super::NUM_SOURCE_NAT_PORTS);
        let mut external_ips = itertools::iproduct!(addresses, ports);

        // Allocate two addresses
        let mut ips = Vec::with_capacity(2);
        let project_id = Uuid::new_v4();
        for (expected_ip, expected_first_port) in external_ips.clone().take(2) {
            let instance_id = Uuid::new_v4();
            let ip = context
                .db_datastore
                .allocate_instance_snat_ip(
                    &context.opctx,
                    Uuid::new_v4(),
                    project_id,
                    instance_id,
                )
                .await
                .expect("Failed to allocate instance external IP address");
            assert_eq!(ip.ip.ip(), expected_ip);
            assert_eq!(ip.first_port.0, expected_first_port as u16);
            let expected_last_port = (expected_first_port
                + (super::NUM_SOURCE_NAT_PORTS - 1) as i32)
                as u16;
            assert_eq!(ip.last_port.0, expected_last_port);
            ips.push(ip);
        }

        // Release the first
        context
            .db_datastore
            .deallocate_external_ip(&context.opctx, ips[0].id)
            .await
            .expect("Failed to release the first external IP address");

        // Allocate a new one, ensure it's the same as the first one we
        // released.
        let instance_id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
            )
            .await
            .expect("Failed to allocate instance external IP address");
        println!("{:?}\n{:?}", ip, ips[0]);
        assert_eq!(
            ip.ip, ips[0].ip,
            "Expected to reallocate external IPs sequentially"
        );
        assert_eq!(
            ip.first_port, ips[0].first_port,
            "Expected to reallocate external IPs sequentially"
        );
        assert_eq!(
            ip.last_port, ips[0].last_port,
            "Expected to reallocate external IPs sequentially"
        );

        // Allocate one more, ensure it's the next chunk after the second one
        // from the original loop.
        let instance_id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
            )
            .await
            .expect("Failed to allocate instance external IP address");
        let (expected_ip, expected_first_port) = external_ips.nth(2).unwrap();
        assert_eq!(ip.ip.ip(), std::net::IpAddr::from(expected_ip));
        assert_eq!(ip.first_port.0, expected_first_port as u16);
        let expected_last_port = (expected_first_port
            + (super::NUM_SOURCE_NAT_PORTS - 1) as i32)
            as u16;
        assert_eq!(ip.last_port.0, expected_last_port);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_with_ephemeral_takes_whole_port_range() {
        let context = TestContext::new(
            "test_next_external_ip_with_ephemeral_takes_whole_port_range",
        )
        .await;
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", range, None).await;

        let instance_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = None;

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_is_restricted_to_projects() {
        let context =
            TestContext::new("test_next_external_ip_is_restricted_to_projects")
                .await;

        // Create one pool restricted to a project, and one not.
        let project_id = Uuid::new_v4();
        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", first_range, Some(project_id)).await;

        let second_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 4),
            Ipv4Addr::new(10, 0, 0, 6),
        ))
        .unwrap();
        context.create_ip_pool("p1", second_range, None).await;

        // Allocating an address on an instance in a _different_ project should
        // get an address from the second pool.
        let instance_id = Uuid::new_v4();
        let instance_project_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = None;

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                instance_project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), second_range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert_eq!(ip.project_id.unwrap(), instance_project_id);

        // Allocating an address on an instance in the same project should get
        // an address from the first pool.
        let instance_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = None;

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), first_range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert_eq!(ip.project_id.unwrap(), project_id);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_for_service() {
        let context =
            TestContext::new("test_next_external_ip_for_service").await;

        // Create an IP pool without an associated project.
        let rack_id = Uuid::parse_str(RACK_UUID).unwrap();
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
        ))
        .unwrap();
        context.create_rack_ip_pool("p0", ip_range, rack_id).await;

        // Allocate an IP address as we would for an external, rack-associated
        // service.
        let id1 = Uuid::new_v4();
        let ip1 = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id1, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip1.kind, IpKind::Service);
        assert_eq!(ip1.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(ip1.first_port.0, 0);
        assert_eq!(ip1.last_port.0, u16::MAX);
        assert!(ip1.instance_id.is_none());
        assert!(ip1.project_id.is_none());

        // Allocate the next (last) IP address
        let id2 = Uuid::new_v4();
        let ip2 = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id2, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip2.kind, IpKind::Service);
        assert_eq!(ip2.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        assert_eq!(ip2.first_port.0, 0);
        assert_eq!(ip2.last_port.0, u16::MAX);
        assert!(ip2.instance_id.is_none());
        assert!(ip2.project_id.is_none());

        // Once we're out of IP addresses, test that we see the right error.
        let id3 = Uuid::new_v4();
        let err = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id3, rack_id)
            .await
            .expect_err("Should have failed to allocate after pool exhausted");
        assert_eq!(
            err,
            Error::InvalidRequest {
                message: String::from("No external IP addresses available"),
            }
        );

        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_external_ip_for_service_is_idempoent() {
        let context = TestContext::new(
            "test_insert_external_ip_for_service_is_idempotent",
        )
        .await;

        // Create an IP pool without an associated project.
        let rack_id = Uuid::parse_str(RACK_UUID).unwrap();
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 2),
        ))
        .unwrap();
        context.create_rack_ip_pool("p0", ip_range, rack_id).await;

        // Allocate an IP address as we would for an external, rack-associated
        // service.
        let id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip.kind, IpKind::Service);
        assert_eq!(ip.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert!(ip.instance_id.is_none());
        assert!(ip.project_id.is_none());

        let ip_again = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");

        assert_eq!(ip.id, ip_again.id);
        assert_eq!(ip.ip.ip(), ip_again.ip.ip());

        context.success().await;
    }

    // This test is identical to "test_insert_external_ip_is_idempotent",
    // but tries to make an idempotent allocation after all addresses in the
    // pool have been allocated.
    #[tokio::test]
    async fn test_insert_external_ip_for_service_is_idempotent_even_when_full()
    {
        let context = TestContext::new(
            "test_insert_external_ip_is_idempotent_even_when_full",
        )
        .await;

        // Create an IP pool without an associated project.
        let rack_id = Uuid::parse_str(RACK_UUID).unwrap();
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 1),
        ))
        .unwrap();
        context.create_rack_ip_pool("p0", ip_range, rack_id).await;

        // Allocate an IP address as we would for an external, rack-associated
        // service.
        let id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");
        assert_eq!(ip.kind, IpKind::Service);
        assert_eq!(ip.ip.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert!(ip.instance_id.is_none());
        assert!(ip.project_id.is_none());

        let ip_again = context
            .db_datastore
            .allocate_service_ip(&context.opctx, id, rack_id)
            .await
            .expect("Failed to allocate service IP address");

        assert_eq!(ip.id, ip_again.id);
        assert_eq!(ip.ip.ip(), ip_again.ip.ip());

        context.success().await;
    }

    #[tokio::test]
    async fn test_insert_external_ip_is_idempotent() {
        let context =
            TestContext::new("test_insert_external_ip_is_idempotent").await;

        // Create an IP pool
        let range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", range, None).await;

        // Create one SNAT IP address.
        let instance_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
            )
            .await
            .expect("Failed to allocate instance SNAT IP address");
        assert_eq!(ip.kind, IpKind::SNat);
        assert_eq!(ip.ip.ip(), range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(
            usize::from(ip.last_port.0),
            super::NUM_SOURCE_NAT_PORTS - 1
        );
        assert_eq!(ip.project_id.unwrap(), project_id);

        // Create a new IP, with the _same_ ID, and ensure we get back the same
        // value.
        let new_ip = context
            .db_datastore
            .allocate_instance_snat_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
            )
            .await
            .expect("Failed to allocate instance SNAT IP address");

        // Check identity, not equality. The timestamps will be updated.
        assert_eq!(ip.id, new_ip.id);
        assert_eq!(ip.name, new_ip.name);
        assert_eq!(ip.description, new_ip.description);
        assert!(ip.time_created <= new_ip.time_created);
        assert!(ip.time_modified <= new_ip.time_modified);
        assert_eq!(ip.time_deleted, new_ip.time_deleted);
        assert_eq!(ip.ip_pool_id, new_ip.ip_pool_id);
        assert_eq!(ip.ip_pool_range_id, new_ip.ip_pool_range_id);
        assert_eq!(ip.kind, new_ip.kind);
        assert_eq!(ip.ip, new_ip.ip);
        assert_eq!(ip.first_port, new_ip.first_port);
        assert_eq!(ip.last_port, new_ip.last_port);

        context.success().await;
    }

    #[tokio::test]
    async fn test_next_external_ip_is_restricted_to_pools() {
        let context =
            TestContext::new("test_next_external_ip_is_restricted_to_pools")
                .await;

        // Create two pools, neither project-restricted.
        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", first_range, None).await;
        let second_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 4),
            Ipv4Addr::new(10, 0, 0, 6),
        ))
        .unwrap();
        context.create_ip_pool("p1", second_range, None).await;

        // Allocating an address on an instance in the second pool should be
        // respected, even though there are IPs available in the first.
        let instance_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let id = Uuid::new_v4();
        let pool_name = Some(Name("p1".parse().unwrap()));

        let ip = context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                id,
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect("Failed to allocate instance ephemeral IP address");
        assert_eq!(ip.kind, IpKind::Ephemeral);
        assert_eq!(ip.ip.ip(), second_range.first_address());
        assert_eq!(ip.first_port.0, 0);
        assert_eq!(ip.last_port.0, u16::MAX);
        assert_eq!(ip.project_id.unwrap(), project_id);

        context.success().await;
    }

    #[tokio::test]
    async fn test_ensure_pool_exhaustion_does_not_use_other_pool() {
        let context = TestContext::new(
            "test_ensure_pool_exhaustion_does_not_use_other_pool",
        )
        .await;

        // Create two pools, neither project-restricted.
        let first_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 3),
        ))
        .unwrap();
        context.create_ip_pool("p0", first_range, None).await;
        let first_address = Ipv4Addr::new(10, 0, 0, 4);
        let last_address = Ipv4Addr::new(10, 0, 0, 6);
        let second_range =
            IpRange::try_from((first_address, last_address)).unwrap();
        context.create_ip_pool("p1", second_range, None).await;

        // Allocate all available addresses in the second pool.
        let instance_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let pool_name = Some(Name("p1".parse().unwrap()));
        let first_octet = first_address.octets()[3];
        let last_octet = last_address.octets()[3];
        for octet in first_octet..=last_octet {
            let ip = context
                .db_datastore
                .allocate_instance_ephemeral_ip(
                    &context.opctx,
                    Uuid::new_v4(),
                    project_id,
                    instance_id,
                    pool_name.clone(),
                )
                .await
                .expect("Failed to allocate instance ephemeral IP address");
            println!("{ip:#?}");
            if let IpAddr::V4(addr) = ip.ip.ip() {
                assert_eq!(addr.octets()[3], octet);
            } else {
                panic!("Expected an IPv4 address");
            }
        }

        // Allocating another address should _fail_, and not use the first pool.
        context
            .db_datastore
            .allocate_instance_ephemeral_ip(
                &context.opctx,
                Uuid::new_v4(),
                project_id,
                instance_id,
                pool_name,
            )
            .await
            .expect_err("Should not use IP addresses from a different pool");

        context.success().await;
    }
}
