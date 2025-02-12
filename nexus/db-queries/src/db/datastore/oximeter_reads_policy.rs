// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries related to the oximeter_reads policy

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::dsl::sql_query;
use diesel::expression::SelectableHelper;
use diesel::sql_types;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use nexus_db_model::DbOximeterReadsMode;
use nexus_db_model::OximeterReadsModeEnum;
use nexus_db_model::OximeterReadsPolicy as DbOximeterReadsPolicy;
use nexus_db_model::SqlU32;
use nexus_types::deployment::OximeterReadsPolicy;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;

impl DataStore {
    /// Return a list of all oximeter_reads policies
    pub async fn oximeter_reads_policy_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, SqlU32>,
    ) -> ListResultVec<OximeterReadsPolicy> {
        use db::schema::oximeter_reads_policy;

        opctx
            .authorize(authz::Action::ListChildren, &authz::BLUEPRINT_CONFIG)
            .await?;

        let policies = paginated(
            oximeter_reads_policy::table,
            oximeter_reads_policy::version,
            pagparams,
        )
        .select(DbOximeterReadsPolicy::as_select())
        .get_results_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(policies.into_iter().map(OximeterReadsPolicy::from).collect())
    }

    /// Return the clickhouse policy with the highest version
    pub async fn oximeter_reads_policy_get_latest(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<OximeterReadsPolicy>, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        use db::schema::oximeter_reads_policy::dsl;

        let latest_policy = dsl::oximeter_reads_policy
            .order_by(dsl::version.desc())
            .first_async::<DbOximeterReadsPolicy>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest_policy.map(Into::into))
    }

    /// Insert the current version of the policy in the database
    ///
    /// Only succeeds if the prior version is the latest version currently
    /// in the `oximeter_reads_policy` table. If there are no versions currently
    /// in the table, then the current policy must be at version 1.
    pub async fn oximeter_reads_policy_insert_latest_version(
        &self,
        opctx: &OpContext,
        policy: &OximeterReadsPolicy,
    ) -> Result<(), Error> {
        if policy.version < 1 {
            return Err(Error::invalid_request(
                "policy version must be greater than 0",
            ));
        }
        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        let num_inserted = if policy.version == 1 {
            self.oximeter_reads_policy_insert_first_policy(opctx, &policy)
                .await?
        } else {
            self.oximeter_reads_policy_insert_next_policy(opctx, &policy)
                .await?
        };

        match num_inserted {
            0 => Err(Error::invalid_request(format!(
                "policy version {} is not the most recent",
                policy.version
            ))),
            1 => Ok(()),
            // This is impossible because we are explicitly inserting only one
            // row with a unique primary key.
            _ => unreachable!("query inserted more than one row"),
        }
    }

    /// Insert the next version of the policy in the database
    ///
    /// Only succeeds if the prior version is the latest version currently
    /// in the `oximeter_reads_policy` table.
    ///
    /// Panics if `policy.version <= 1`;
    async fn oximeter_reads_policy_insert_next_policy(
        &self,
        opctx: &OpContext,
        policy: &OximeterReadsPolicy,
    ) -> Result<usize, Error> {
        assert!(policy.version > 1);
        let prev_version = policy.version - 1;

        sql_query(
                r"INSERT INTO oximeter_reads_policy
                     (version, oximeter_reads_mode, time_created)
                     SELECT $1, $2, $3
                      FROM oximeter_reads_policy WHERE version = $4 AND version IN
                       (SELECT version FROM oximeter_reads_policy
                        ORDER BY version DESC LIMIT 1)",
            )
            .bind::<sql_types::BigInt, SqlU32>(policy.version.into())
            .bind::<OximeterReadsModeEnum, DbOximeterReadsMode>((&policy.mode).into())
            .bind::<sql_types::Timestamptz, _>(policy.time_created)
            .bind::<sql_types::BigInt, SqlU32>(prev_version.into())
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert the first clickhouse policy in the database at version 1.
    ///
    /// Only insert this policy if no other policy exists yet.
    ///
    /// Return the number of inserted rows or an error.
    async fn oximeter_reads_policy_insert_first_policy(
        &self,
        opctx: &OpContext,
        policy: &OximeterReadsPolicy,
    ) -> Result<usize, Error> {
        sql_query(
            r"INSERT INTO oximeter_reads_policy
                  (version, oximeter_reads_mode, time_created)
                 SELECT $1, $2, $3
                 WHERE NOT EXISTS (SELECT * FROM oximeter_reads_policy)",
        )
        .bind::<sql_types::BigInt, SqlU32>(policy.version.into())
        .bind::<OximeterReadsModeEnum, DbOximeterReadsMode>(
            (&policy.mode).into(),
        )
        .bind::<sql_types::Timestamptz, _>(policy.time_created)
        .execute_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
