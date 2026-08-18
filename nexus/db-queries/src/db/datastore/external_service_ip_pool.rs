// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for assigning IP pools to external services.
//!
//! These are the internal persistence methods behind the (not-yet-built)
//! per-service networking configuration API. The assignment is operator intent
//! consumed by Reconfigurator, but nothing reads this table yet.

use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::DataStore;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::TypedSqlQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::ExternalServiceIpPool;
use nexus_db_model::ExternalServiceKind;
use nexus_db_model::IpPoolAssignment;
use nexus_db_schema::enums::ExternalServiceKindEnum;
use nexus_db_schema::enums::IpPoolAssignmentEnum;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

// Bool-cast sentinel used when a target IP Pool exists, but isn't assigned for
// `system_services` at all.
const NOT_SYSTEM_SERVICES_POOL_SENTINEL: &str = "not-a-system-services-pool";

// Bool-cast sentinel used when we try to unassign the last pool for a specific
// service.
const UNASSIGN_LAST_POOL_SENTINEL: &str = "unassign-last-pool";
const UNASSIGN_LAST_POOL_ERROR: &str =
    "cannot remove the last IP pool assigned to a service";

impl DataStore {
    /// Assign an IP pool to an external service.
    ///
    /// This fails if the IP Pool doesn't exist or exists but isn't a service IP
    /// Pool. Assigning the same pool multiple times is a no-op.
    pub async fn external_service_ip_pool_assign(
        &self,
        opctx: &OpContext,
        service: ExternalServiceKind,
        authz_pool: &authz::IpPool,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        let pool_id = authz_pool.id();
        let conn = self.pool_connection_authorized(opctx).await?;

        match assign_pool_to_service_query(service, pool_id)
            .execute_async(&*conn)
            .await
        {
            // No rows means there is no such pool at all.
            Ok(0) => {
                Err(Error::not_found_by_id(ResourceType::IpPool, &pool_id))
            }
            Ok(_) => Ok(()),
            // Already exists -> no-op.
            Err(DieselError::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                _,
            )) => Ok(()),
            // Pool exists, but is a silo pool.
            Err(DieselError::DatabaseError(
                DatabaseErrorKind::Unknown,
                ref info,
            )) if info.message().ends_with("invalid bool value") => {
                Err(Error::invalid_request(
                    "only IP pools assigned for system services may be \
                    assigned to a service",
                ))
            }
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Unassign an IP pool from an external service.
    ///
    /// This fails if the pool is the last one assigned to the specific service.
    /// Unassigning a pool multiple times is a no-op.
    pub async fn external_service_ip_pool_unassign(
        &self,
        opctx: &OpContext,
        service: ExternalServiceKind,
        authz_pool: &authz::IpPool,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, authz_pool).await?;
        let pool_id = authz_pool.id();
        let conn = self.pool_connection_authorized(opctx).await?;

        match unassign_pool_from_service_query(service, pool_id)
            .execute_async(&*conn)
            .await
        {
            // A row was removed, or the pair wasn't assigned; either is success.
            Ok(_) => Ok(()),
            // The bool-cast sentinel fired: this was the service's last pool.
            Err(DieselError::DatabaseError(
                DatabaseErrorKind::Unknown,
                ref info,
            )) if info.message().ends_with("invalid bool value") => {
                Err(Error::invalid_request(UNASSIGN_LAST_POOL_ERROR))
            }
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// List assignments between pools and services.
    pub async fn external_service_ip_pool_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (ExternalServiceKind, Uuid)>,
    ) -> ListResultVec<ExternalServiceIpPool> {
        use nexus_db_schema::schema::external_service_ip_pool;
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        paginated_multicolumn(
            external_service_ip_pool::table,
            (
                external_service_ip_pool::service,
                external_service_ip_pool::ip_pool_id,
            ),
            pagparams,
        )
        .select(ExternalServiceIpPool::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List IP pools assigned to a given external service.
    pub async fn external_service_ip_pool_pools_for_service(
        &self,
        opctx: &OpContext,
        service: ExternalServiceKind,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Uuid> {
        use nexus_db_schema::schema::external_service_ip_pool;
        opctx
            .authorize(authz::Action::ListChildren, &authz::IP_POOL_LIST)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        paginated(
            external_service_ip_pool::table,
            external_service_ip_pool::ip_pool_id,
            pagparams,
        )
        .filter(external_service_ip_pool::service.eq(service))
        .select(external_service_ip_pool::ip_pool_id)
        .load_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

// Query to assign a pool to a service.
//
// This emits a custom query which:
//
// - Checks that the pool exists, isn't deleted, and is a service IP Pool.
// - Increments the `ip_pool.rcgen` column, to detect concurrent changes to the
//   pool and this assignment.
//
// The return value helps disambiguate the failure modes:
//
// - If the pool doesn't exist, Ok(0) is returned (no rows affected)
// - If the pool isn't a service pool, a bool-cast error is generated
// - If the assignment already existed, a PK violation is generated.
// - Otherwise, the assignment succeeds and the row count is returned.
fn assign_pool_to_service_query(
    service: ExternalServiceKind,
    pool_id: Uuid,
) -> TypedSqlQuery<()> {
    let mut builder = QueryBuilder::new();
    builder
        .sql("WITH bumped AS (UPDATE ip_pool SET rcgen = rcgen + 1 WHERE id = ")
        .param()
        .bind::<sql_types::Uuid, _>(pool_id)
        .sql(" AND time_deleted IS NULL AND CAST(IF(assignment = ")
        .param()
        .bind::<IpPoolAssignmentEnum, _>(IpPoolAssignment::SystemServices)
        .sql(", 'true', '")
        .sql(NOT_SYSTEM_SERVICES_POOL_SENTINEL)
        .sql(
            "') AS BOOL) RETURNING id) \
            INSERT INTO external_service_ip_pool (service, ip_pool_id) SELECT ",
        )
        .param()
        .bind::<ExternalServiceKindEnum, _>(service)
        .sql(", id FROM bumped");
    builder.query()
}

// Query to unassign a pool from a service.
//
// This generates a query which:
//
// - Hard-deletes the pool <-> service assignment
// - Increments the `ip_pool.rcgen` generation number to detect concurrent
//   operations on the pool and this association.
//
// The return values disambiguates the failure modes:
//
// - If there isn't at least 1 other pool assigned to the same service, a
//   bool-cast error is generated.
// - If the pool isn't currently assigned, Ok(0) is returned (no rows affected).
// - Otherwise, the unassignment succeeds and the row count is returned.
fn unassign_pool_from_service_query(
    service: ExternalServiceKind,
    pool_id: Uuid,
) -> TypedSqlQuery<()> {
    let mut builder = QueryBuilder::new();
    builder
        .sql(
            "WITH deleted AS (\
                DELETE FROM external_service_ip_pool WHERE service = ",
        )
        .param()
        .bind::<ExternalServiceKindEnum, _>(service)
        .sql(" AND ip_pool_id = ")
        .param()
        .bind::<sql_types::Uuid, _>(pool_id)
        .sql(
            " AND CAST(IF(\
                (SELECT COUNT(1) FROM external_service_ip_pool WHERE service = ",
        )
        .param()
        .bind::<ExternalServiceKindEnum, _>(service)
        .sql(") >= 2, 'true', '")
        .sql(UNASSIGN_LAST_POOL_SENTINEL)
        .sql(
            "') AS BOOL) RETURNING ip_pool_id) \
            UPDATE ip_pool SET rcgen = rcgen + 1 WHERE id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(pool_id)
        .sql(" AND EXISTS (SELECT 1 FROM deleted)");
    builder.query()
}

#[cfg(test)]
mod test {
    use super::assign_pool_to_service_query;
    use super::unassign_pool_from_service_query;
    use crate::authz;
    use crate::db::datastore::DataStore;
    use crate::db::datastore::SQL_BATCH_SIZE;
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::create_service_ip_pool;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use assert_matches::assert_matches;
    use async_bb8_diesel::AsyncRunQueryDsl as _;
    use diesel::ExpressionMethods as _;
    use diesel::QueryDsl as _;
    use dropshot::PaginationOrder;
    use nexus_db_model::ExternalServiceIpPool;
    use nexus_db_model::ExternalServiceKind;
    use nexus_db_model::IpPool;
    use nexus_db_model::IpPoolAssignment;
    use nexus_types::identity::Resource as _;
    use omicron_common::api::external::DataPageParams;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::IpVersion;
    use omicron_common::api::external::LookupType;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    // Read a pool's current `rcgen`, to check that mutations bump it.
    async fn pool_rcgen(datastore: &DataStore, pool_id: Uuid) -> i64 {
        use nexus_db_schema::schema::ip_pool::dsl;
        let conn = datastore
            .pool_connection_for_tests()
            .await
            .expect("got a connection");
        dsl::ip_pool
            .filter(dsl::id.eq(pool_id))
            .select(dsl::rcgen)
            .first_async::<i64>(&*conn)
            .await
            .expect("read the pool's rcgen")
    }

    // A page big enough to hold all rows any of these tests create.
    fn all<T>() -> DataPageParams<'static, T> {
        DataPageParams {
            marker: None,
            limit: NonZeroU32::new(100).unwrap(),
            direction: PaginationOrder::Ascending,
        }
    }

    #[tokio::test]
    async fn test_assign_pool_to_service() {
        let logctx = dev::test_setup_log("test_assign_pool_to_service");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // A system-services IP pool, and its `rcgen` before we touch it.
        let pool = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-v4",
            IpVersion::V4,
        )
        .await;
        let pool_id = pool.authz_pool.id();
        let rcgen_before = pool_rcgen(datastore, pool_id).await;

        // Assign it to Nexus.
        datastore
            .external_service_ip_pool_assign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool.authz_pool,
            )
            .await
            .expect("assigning a system-services pool to Nexus should succeed");

        // Shows up in the Nexus-specific list, and the pool's `rcgen` was
        // bumped.
        let pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::Nexus,
                &all(),
            )
            .await
            .expect("listing Nexus's pools should succeed");
        assert_eq!(pools, vec![pool_id]);
        assert_eq!(pool_rcgen(datastore, pool_id).await, rcgen_before + 1);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_assign_is_idempotent() {
        let logctx = dev::test_setup_log("test_assign_is_idempotent");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-v4",
            IpVersion::V4,
        )
        .await;
        let pool_id = pool.authz_pool.id();

        // First assignment.
        datastore
            .external_service_ip_pool_assign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool.authz_pool,
            )
            .await
            .expect("first assignment should succeed");
        let rcgen_after_first = pool_rcgen(datastore, pool_id).await;

        // Re-assigning the same pair is a no-op.
        datastore
            .external_service_ip_pool_assign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool.authz_pool,
            )
            .await
            .expect("re-assigning the same pair should succeed");

        let pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::Nexus,
                &all(),
            )
            .await
            .expect("listing Nexus's pools should succeed");
        assert_eq!(pools, vec![pool_id]);
        assert_eq!(pool_rcgen(datastore, pool_id).await, rcgen_after_first);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_pool_shared_by_two_services() {
        let logctx = dev::test_setup_log("test_pool_shared_by_two_services");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-v4",
            IpVersion::V4,
        )
        .await;
        let pool_id = pool.authz_pool.id();
        let rcgen_before = pool_rcgen(datastore, pool_id).await;

        // The same pool can back more than one service.
        for service in
            [ExternalServiceKind::Nexus, ExternalServiceKind::BoundaryNtp]
        {
            datastore
                .external_service_ip_pool_assign(
                    opctx,
                    service,
                    &pool.authz_pool,
                )
                .await
                .expect("assigning the pool to a service should succeed");
        }

        // Shows up in the list for each service.
        for service in
            [ExternalServiceKind::Nexus, ExternalServiceKind::BoundaryNtp]
        {
            let pools = datastore
                .external_service_ip_pool_pools_for_service(
                    opctx,
                    service,
                    &all(),
                )
                .await
                .expect("listing a service's pools should succeed");
            assert_eq!(pools, vec![pool_id]);
        }

        // Both rows show up in the full list, for any service.
        let all_rows = datastore
            .external_service_ip_pool_list(opctx, &all())
            .await
            .expect("listing all assignments should succeed");
        let got: Vec<(ExternalServiceKind, Uuid)> =
            all_rows.iter().map(|r| (r.service, r.ip_pool_id)).collect();
        assert_eq!(got.len(), 2);
        assert!(got.contains(&(ExternalServiceKind::Nexus, pool_id)));
        assert!(got.contains(&(ExternalServiceKind::BoundaryNtp, pool_id)));

        // Two assignments, two rcgen bumps.
        assert_eq!(pool_rcgen(datastore, pool_id).await, rcgen_before + 2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_assign_silo_pool_is_rejected() {
        let logctx = dev::test_setup_log("test_assign_silo_pool_is_rejected");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // A silo pool, not a system-services pool.
        let db_pool = datastore
            .ip_pool_create(
                opctx,
                IpPool::new(
                    &IdentityMetadataCreateParams {
                        name: "silo-pool".parse().unwrap(),
                        description: String::new(),
                    },
                    nexus_db_model::IpVersion::V4,
                    IpPoolAssignment::Silos,
                ),
            )
            .await
            .expect("creating a silo pool should succeed");
        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            db_pool.id(),
            LookupType::ById(db_pool.id()),
        );

        // Assigning a silo pool to a service is a 400.
        let err = datastore
            .external_service_ip_pool_assign(
                opctx,
                ExternalServiceKind::Nexus,
                &authz_pool,
            )
            .await
            .expect_err("assigning a silo pool to a service should fail");
        assert_matches!(err, Error::InvalidRequest { .. });

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_assign_nonexistent_pool_is_not_found() {
        let logctx =
            dev::test_setup_log("test_assign_nonexistent_pool_is_not_found");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // An authz handle for a pool that was never created.
        let missing = uuid::uuid!("289b779c-f8ec-4b04-97fd-39e70905d924");
        let authz_pool = authz::IpPool::new(
            authz::FLEET,
            missing,
            LookupType::ById(missing),
        );

        let err = datastore
            .external_service_ip_pool_assign(
                opctx,
                ExternalServiceKind::Nexus,
                &authz_pool,
            )
            .await
            .expect_err("assigning a non-existent pool should fail");
        assert_matches!(err, Error::ObjectNotFound { .. });

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_unassign_pool() {
        let logctx = dev::test_setup_log("test_unassign_pool");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool1 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-1",
            IpVersion::V4,
        )
        .await;
        let pool2 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-2",
            IpVersion::V4,
        )
        .await;
        let id1 = pool1.authz_pool.id();
        let id2 = pool2.authz_pool.id();

        // Nexus gets both pools.
        for pool in [&pool1, &pool2] {
            datastore
                .external_service_ip_pool_assign(
                    opctx,
                    ExternalServiceKind::Nexus,
                    &pool.authz_pool,
                )
                .await
                .expect("assigning should succeed");
        }
        let rcgen1_before = pool_rcgen(datastore, id1).await;

        // Removing pool1 succeeds because pool2 still remains.
        datastore
            .external_service_ip_pool_unassign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool1.authz_pool,
            )
            .await
            .expect("unassigning should succeed while another pool remains");

        // Only pool2 is left, and pool1's rcgen was bumped by the removal.
        let pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::Nexus,
                &all(),
            )
            .await
            .expect("listing Nexus's pools should succeed");
        assert_eq!(pools, vec![id2]);
        assert_eq!(pool_rcgen(datastore, id1).await, rcgen1_before + 1);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_unassign_last_pool_is_rejected() {
        let logctx = dev::test_setup_log("test_unassign_last_pool_is_rejected");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-v4",
            IpVersion::V4,
        )
        .await;
        let pool_id = pool.authz_pool.id();
        datastore
            .external_service_ip_pool_assign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool.authz_pool,
            )
            .await
            .expect("assigning should succeed");
        let rcgen_before = pool_rcgen(datastore, pool_id).await;

        // It's Nexus's only pool, so removing it is rejected as a 400.
        let err = datastore
            .external_service_ip_pool_unassign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool.authz_pool,
            )
            .await
            .expect_err("removing a service's last pool should fail");
        assert_matches!(err, Error::InvalidRequest { .. });

        // Still assigned, and rcgen not changed.
        let pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::Nexus,
                &all(),
            )
            .await
            .expect("listing Nexus's pools should succeed");
        assert_eq!(pools, vec![pool_id]);
        assert_eq!(pool_rcgen(datastore, pool_id).await, rcgen_before);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_unassign_not_assigned_is_noop() {
        let logctx = dev::test_setup_log("test_unassign_not_assigned_is_noop");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-v4",
            IpVersion::V4,
        )
        .await;
        let pool_id = pool.authz_pool.id();
        let rcgen_before = pool_rcgen(datastore, pool_id).await;

        // The pool was never assigned to Nexus, removing is a no-op.
        datastore
            .external_service_ip_pool_unassign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool.authz_pool,
            )
            .await
            .expect("unassigning a not-assigned pair should be a no-op");

        // Still doesn't show up in the list for Nexus.
        let pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::Nexus,
                &all(),
            )
            .await
            .expect("listing Nexus's pools should succeed");
        assert!(pools.is_empty());
        assert_eq!(pool_rcgen(datastore, pool_id).await, rcgen_before);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_list_and_pagination() {
        let logctx = dev::test_setup_log("test_list_and_pagination");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let pool1 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-1",
            IpVersion::V4,
        )
        .await;
        let pool2 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-2",
            IpVersion::V4,
        )
        .await;
        let pool3 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-3",
            IpVersion::V4,
        )
        .await;
        let pool4 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-4",
            IpVersion::V4,
        )
        .await;
        let id1 = pool1.authz_pool.id();
        let id2 = pool2.authz_pool.id();
        let id3 = pool3.authz_pool.id();
        let id4 = pool4.authz_pool.id();

        // Nexus: {pool1, pool2}
        // boundary NTP: {pool2, pool3} (pool2 shared)
        // External DNS: {pool4}
        let assignments = [
            (ExternalServiceKind::Nexus, &pool1),
            (ExternalServiceKind::Nexus, &pool2),
            (ExternalServiceKind::BoundaryNtp, &pool2),
            (ExternalServiceKind::BoundaryNtp, &pool3),
            (ExternalServiceKind::ExternalDns, &pool4),
        ];
        for (service, pool) in assignments {
            datastore
                .external_service_ip_pool_assign(
                    opctx,
                    service,
                    &pool.authz_pool,
                )
                .await
                .expect("assigning should succeed");
        }

        // List for each service, ensure it's sorted by ID.
        let mut nexus_pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::Nexus,
                &all(),
            )
            .await
            .expect("listing Nexus's pools should succeed");
        nexus_pools.sort();
        let mut expected_nexus_pools = vec![id1, id2];
        expected_nexus_pools.sort();
        assert_eq!(nexus_pools, expected_nexus_pools);

        let mut ntp_pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::BoundaryNtp,
                &all(),
            )
            .await
            .expect("listing boundary NTP's pools should succeed");
        ntp_pools.sort();
        let mut expected_ntp_pools = vec![id2, id3];
        expected_ntp_pools.sort();
        assert_eq!(ntp_pools, expected_ntp_pools);

        // DNS only got one pool, no sorting needed.
        let dns_pools = datastore
            .external_service_ip_pool_pools_for_service(
                opctx,
                ExternalServiceKind::ExternalDns,
                &all(),
            )
            .await
            .expect("listing external DNS's pools should succeed");
        assert_eq!(dns_pools, &[id4]);

        // Walk the full list in pages of 2 to exercise pagination.
        let mut seen: Vec<(ExternalServiceKind, Uuid)> = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = datastore
                .external_service_ip_pool_list(opctx, &p.current_pagparams())
                .await
                .expect("listing a page of assignments should succeed");
            paginator = p
                .found_batch(&batch, &|row: &ExternalServiceIpPool| {
                    (row.service, row.ip_pool_id)
                });
            seen.extend(
                batch.into_iter().map(|row| (row.service, row.ip_pool_id)),
            );
        }
        assert_eq!(seen.len(), 5);
        for expected in [
            (ExternalServiceKind::Nexus, id1),
            (ExternalServiceKind::Nexus, id2),
            (ExternalServiceKind::BoundaryNtp, id2),
            (ExternalServiceKind::BoundaryNtp, id3),
            (ExternalServiceKind::ExternalDns, id4),
        ] {
            assert!(seen.contains(&expected), "list is missing {expected:?}");
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_cannot_delete_pool_assigned_to_service() {
        let logctx =
            dev::test_setup_log("test_cannot_delete_pool_assigned_to_service");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create two pools for services, so we don't hit the "at least one
        // pool" check in the `ip_pool_delete()` method.
        let pool1 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-1",
            IpVersion::V4,
        )
        .await;
        let _pool2 = create_service_ip_pool(
            opctx,
            datastore,
            "svc-pool-2",
            IpVersion::V4,
        )
        .await;

        datastore
            .external_service_ip_pool_assign(
                opctx,
                ExternalServiceKind::Nexus,
                &pool1.authz_pool,
            )
            .await
            .expect("assigning should succeed");

        // Still assigned to Nexus, can't delete the pool at all.
        let err = datastore
            .ip_pool_delete(opctx, &pool1.authz_pool, &pool1.db_pool)
            .await
            .expect_err("deleting a pool assigned to a service should fail");
        assert_matches!(err, Error::InvalidRequest { .. });

        db.terminate().await;
        logctx.cleanup_successful();
    }

    const POOL_ID: Uuid = uuid::uuid!("bce2bca9-2183-4625-9560-4dfab8378c1e");

    #[tokio::test]
    async fn expectorate_assign_pool_to_service_query() {
        let query =
            assign_pool_to_service_query(ExternalServiceKind::Nexus, POOL_ID);
        expectorate_query_contents(
            &query,
            "tests/output/external_service_ip_pool_assign.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_unassign_pool_from_service_query() {
        let query = unassign_pool_from_service_query(
            ExternalServiceKind::Nexus,
            POOL_ID,
        );
        expectorate_query_contents(
            &query,
            "tests/output/external_service_ip_pool_unassign.sql",
        )
        .await;
    }
}
