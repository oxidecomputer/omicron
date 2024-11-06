// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/5.0/.

// Copyright 2024 Oxide Computer Company

//! Database queries for tracking Tofino ASIC table utilization

use super::DataStore;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::raw_query_builder::QueryBuilder;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::data_types::PgNumeric;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use diesel::ExpressionMethods as _;
use diesel::QueryDsl as _;
use nexus_auth::context::OpContext;
use nexus_db_model::schema::asic_table_utilization;
use nexus_db_model::AsicTable;
use nexus_db_model::AsicTableEnum;
use nexus_db_model::AsicTableUtilization;
use nexus_db_model::SqlU32;
use omicron_common::api::external::Error;
use omicron_common::api::external::MessagePair;
use omicron_common::api::internal::nexus::AsicTable as AsicTableInternal;
use uuid::Uuid;

/// Return the allocatable capacity for the named ASIC table.
fn n_allocatable_for_table(table: AsicTableInternal) -> u32 {
    let cap = match table {
        AsicTableInternal::Ipv4Routing => nexus_defaults::IPV4_ROUTING_TABLE,
        AsicTableInternal::Ipv6Routing => nexus_defaults::IPV6_ROUTING_TABLE,
        AsicTableInternal::Ipv4Addresses => nexus_defaults::IPV4_ADDRESS_TABLE,
        AsicTableInternal::Ipv6Addresses => nexus_defaults::IPV6_ADDRESS_TABLE,
        AsicTableInternal::Ipv4Nat => nexus_defaults::IPV4_NAT_TABLE,
        AsicTableInternal::Ipv6Nat => nexus_defaults::IPV6_NAT_TABLE,
        AsicTableInternal::Ipv4Arp => nexus_defaults::IPV4_ARP_TABLE,
        AsicTableInternal::Ipv6Neighbor => nexus_defaults::IPV6_NEIGHBOR_TABLE,
    };
    cap.n_allocatable()
}

// Name of the CHECK constraint that will be violated if there is insufficient
// space in the ASIC table.
const CONSTRAINT_NAME: &str = "nonzero_utilization";

impl DataStore {
    /// Add an entry in the ASIC utilization table.
    ///
    /// This adds an entry in the database table that tracks space on the ASIC
    /// table consumed by an API resource.
    pub async fn insert_asic_utilization_entry(
        &self,
        opctx: &OpContext,
        entry: &AsicTableUtilization,
    ) -> Result<(), Error> {
        let n_allocatable = n_allocatable_for_table(entry.asic_table.0);
        let query = QueryBuilder::new()
            .sql("INSERT INTO asic_table_utilization VALUES (")
            .param()
            .bind::<sql_types::Uuid, _>(entry.id)
            .sql(", ")
            .param()
            .bind::<AsicTableEnum, _>(entry.asic_table)
            .sql(", IF((SELECT IFNULL(SUM(n_entries), 0) + ")
            .param()
            .bind::<sql_types::Int8, _>(entry.n_entries)
            .sql(" <= ")
            .param()
            .bind::<sql_types::Int8, _>(SqlU32::new(n_allocatable))
            .sql(" FROM asic_table_utilization WHERE asic_table = ")
            .param()
            .bind::<AsicTableEnum, _>(entry.asic_table)
            .sql(" AND time_deleted IS NULL), ")
            .param()
            .bind::<sql_types::Int8, _>(entry.n_entries)
            .sql(", 0), ")
            .param()
            .bind::<sql_types::Timestamptz, _>(entry.time_created)
            .sql(", ")
            .param()
            .bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(
                entry.time_deleted,
            )
            .sql(")")
            .query::<sql_types::Int8>();
        let conn = self.pool_connection_authorized(opctx).await?;
        match query.execute_async(&*conn).await {
            Ok(_count) => Ok(()),
            Err(DieselError::DatabaseError(
                DatabaseErrorKind::CheckViolation,
                info,
            )) if info
                .constraint_name()
                .map(|c| c == CONSTRAINT_NAME)
                .unwrap_or(false) =>
            {
                let message = MessagePair::new_full(
                    String::from(
                        "There is insufficient space on the switch ASIC \
                        for the requested networking resource",
                    ),
                    format!(
                        "There is insufficient space on the '{}' ASIC table",
                        entry.asic_table.0,
                    ),
                );
                Err(Error::InsufficientCapacity { message })
            }
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Delete an entry from the ASIC utilization table by ID.
    pub async fn delete_asic_utilization_entry(
        &self,
        opctx: &OpContext,
        id: &Uuid,
    ) -> Result<(), Error> {
        let res = diesel::update(
            asic_table_utilization::dsl::asic_table_utilization.find(*id),
        )
        .set(asic_table_utilization::dsl::time_deleted.eq(Utc::now()))
        .execute_async(&*self.pool_connection_authorized(opctx).await?)
        .await;
        match res {
            Ok(_) | Err(DieselError::NotFound) => Ok(()),
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    /// Return the current utilization of the named ASIC table.
    pub async fn asic_utilization(
        &self,
        opctx: &OpContext,
        table: AsicTable,
    ) -> Result<u32, Error> {
        let conn = self.pool_connection_authorized(opctx).await;

        // We're using a u32 to represent the available and utilized capacity of
        // these ASIC tables. That's stored in the DB as an `Int8`, since we're
        // using the unsigned `SqlU32` wrapper type.
        //
        // CockroachDB performs this sum as a decimal / numeric type, which is
        // arbitrary precision. Extract the query result as this type, and then
        // convert that to an actual `u32` using its base-10000 representation
        // (yes, seriously)
        match asic_table_utilization::dsl::asic_table_utilization
            .select(diesel::dsl::sum(asic_table_utilization::dsl::n_entries))
            .filter(asic_table_utilization::dsl::asic_table.eq(table))
            .filter(asic_table_utilization::dsl::time_deleted.is_null())
            .nullable()
            .get_result_async::<Option<PgNumeric>>(&*conn?)
            .await
        {
            Ok(Some(utilization)) => u32_from_numeric(utilization, table),
            Ok(None) => Ok(0),
            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }
}

fn u32_from_numeric(num: PgNumeric, table: AsicTable) -> Result<u32, Error> {
    let PgNumeric::Positive { scale, digits, .. } = num else {
        return Err(Error::internal_error(
            "Found impossible negative or NaN numeric deserializing ASIC \
            table utilization from the database",
        ));
    };
    if scale != 0 {
        return Err(Error::internal_error(
            "Found impossible non-integer sum deserializing ASIC \
            table utilization from the database",
        ));
    }
    let mut base: i64 = 1;
    let mut sum: i64 = 0;
    for digit in digits.into_iter() {
        sum += base * i64::from(digit);
        base *= 10_000;
    }
    u32::try_from(sum).map_err(|_| {
        let msg = format!(
            "Overflowed a u32 computing the sum of ASIC table \
                utilization. Count is {} for {} table.",
            sum, table,
        );
        Error::internal_error(msg.as_str())
    })
}

#[cfg(test)]
mod tests {
    use crate::db::pub_test_utils::TestDatabase;
    use chrono::Utc;
    use nexus_db_model::AsicTable;
    use nexus_db_model::AsicTableUtilization;
    use nexus_db_model::SqlU32;
    use nexus_defaults::IPV4_NAT_TABLE;
    use omicron_common::api::internal::nexus::AsicTable as AsicTableInternal;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    #[tokio::test]
    async fn insert_delete_single_asic_utilization_entry() {
        let logctx =
            dev::test_setup_log("insert_delete_single_asic_utilization_entry");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let entry = AsicTableUtilization {
            id: Uuid::new_v4(),
            asic_table: AsicTable(AsicTableInternal::Ipv4Nat),
            n_entries: SqlU32(2),
            time_created: Utc::now(),
            time_deleted: None,
        };
        datastore.insert_asic_utilization_entry(opctx, &entry).await.expect(
            "Should have been able to insert single ASIC utilization entry",
        );
        let util = datastore
            .asic_utilization(opctx, entry.asic_table)
            .await
            .expect("Failed to compute current utilization");
        assert_eq!(util, 2, "Computed incorrect ASIC table utilization");

        datastore
            .delete_asic_utilization_entry(opctx, &entry.id)
            .await
            .expect("Failed to delete utilization entry by ID");
        let util = datastore
            .asic_utilization(opctx, entry.asic_table)
            .await
            .expect("Failed to compute current utilization");
        assert_eq!(
            util, 0,
            "ASIC utilization should not include soft-deleted entries"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn handle_utilization_of_empty_table() {
        let logctx = dev::test_setup_log("handle_utilization_of_empty_table");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let util = datastore
            .asic_utilization(opctx, AsicTable(AsicTableInternal::Ipv4Nat))
            .await
            .expect("Failed to compute current utilization");
        assert_eq!(util, 0, "Utilization of an empty table should be 0");
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn fail_insert_when_out_of_table_space() {
        let logctx = dev::test_setup_log("fail_insert_when_out_of_table_space");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let entry = AsicTableUtilization {
            id: Uuid::new_v4(),
            asic_table: AsicTable(AsicTableInternal::Ipv4Nat),
            n_entries: SqlU32(IPV4_NAT_TABLE.n_allocatable()),
            time_created: Utc::now(),
            time_deleted: None,
        };
        datastore.insert_asic_utilization_entry(opctx, &entry).await.expect(
            "Should have been able to insert single ASIC utilization entry",
        );
        let util = datastore
            .asic_utilization(opctx, entry.asic_table)
            .await
            .expect("Failed to compute current utilization");
        assert_eq!(
            util,
            IPV4_NAT_TABLE.n_allocatable(),
            "Computed incorrect ASIC table utilization"
        );

        // Trying to insert more should fail.
        let entry = AsicTableUtilization {
            id: Uuid::new_v4(),
            asic_table: AsicTable(AsicTableInternal::Ipv4Nat),
            n_entries: SqlU32(1),
            time_created: Utc::now(),
            time_deleted: None,
        };
        let err = datastore
            .insert_asic_utilization_entry(opctx, &entry)
            .await
            .expect_err(
                "Should have failed to insert ASIC utilization \
                entry when the table is completely full",
            );
        assert!(
            matches!(
                err,
                omicron_common::api::external::Error::InsufficientCapacity { .. }
            ),
            "Should have failed with insufficient capacity when \
            the ASIC table is completely full"
        );
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
