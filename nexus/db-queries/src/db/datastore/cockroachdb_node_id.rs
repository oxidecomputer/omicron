// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Datastore methods involving CockroachDB node IDs.

use super::DataStore;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_model::CockroachZoneIdToNodeId;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::OmicronZoneUuid;

impl DataStore {
    /// Get the CockroachDB node ID of a given Omicron zone ID.
    ///
    /// Returns `Ok(None)` if no node ID is known. This can occur if the
    /// requested zone ID isn't a CockroachDB zone, or if it is a CockroachDB
    /// zone but the background task responsible for collecting its node ID has
    /// not yet successfully done so.
    pub async fn cockroachdb_node_id(
        &self,
        opctx: &OpContext,
        omicron_zone_id: OmicronZoneUuid,
    ) -> LookupResult<Option<String>> {
        use db::schema::cockroachdb_zone_id_to_node_id::dsl;

        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        dsl::cockroachdb_zone_id_to_node_id
            .select(dsl::crdb_node_id)
            .filter(dsl::omicron_zone_id.eq(to_db_typed_uuid(omicron_zone_id)))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Record the CockroachDB node ID of a given Omicron zone ID.
    ///
    /// This function must only be called with valid CockroachDB zone IDs. It
    /// will return an error if the given `omicron_zone_id` already has an entry
    /// in this table that does not exactly match `crdb_node_id`.
    pub async fn set_cockroachdb_node_id(
        &self,
        opctx: &OpContext,
        omicron_zone_id: OmicronZoneUuid,
        crdb_node_id: String,
    ) -> Result<(), Error> {
        use db::schema::cockroachdb_zone_id_to_node_id::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let row = CockroachZoneIdToNodeId {
            omicron_zone_id: omicron_zone_id.into(),
            crdb_node_id,
        };

        let _nrows = diesel::insert_into(dsl::cockroachdb_zone_id_to_node_id)
            .values(row)
            .on_conflict((dsl::omicron_zone_id, dsl::crdb_node_id))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_cockroachdb_node_id() {
        let logctx =
            dev::test_setup_log("test_service_network_interfaces_list");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Make up a CRDB zone id.
        let crdb_zone_id = OmicronZoneUuid::new_v4();

        // We shouldn't have a mapping for it yet.
        let node_id = datastore
            .cockroachdb_node_id(&opctx, crdb_zone_id)
            .await
            .expect("looked up node ID");
        assert_eq!(node_id, None);

        // We can assign a mapping.
        let fake_node_id = "test-node";
        datastore
            .set_cockroachdb_node_id(
                &opctx,
                crdb_zone_id,
                fake_node_id.to_string(),
            )
            .await
            .expect("set node ID");

        // We can look up the mapping we created.
        let node_id = datastore
            .cockroachdb_node_id(&opctx, crdb_zone_id)
            .await
            .expect("looked up node ID");
        assert_eq!(node_id.as_deref(), Some(fake_node_id));

        // We can't assign a different node ID to this same zone.
        let different_node_id = "test-node-2";
        datastore
            .set_cockroachdb_node_id(
                &opctx,
                crdb_zone_id,
                different_node_id.to_string(),
            )
            .await
            .expect_err("failed to set node ID");

        // We can't assign the same node ID to a different zone, either.
        let different_zone_id = OmicronZoneUuid::new_v4();
        datastore
            .set_cockroachdb_node_id(
                &opctx,
                different_zone_id,
                fake_node_id.to_string(),
            )
            .await
            .expect_err("failed to set node ID");

        // We can reassign the same node ID (i.e., setting is idempotent).
        datastore
            .set_cockroachdb_node_id(
                &opctx,
                crdb_zone_id,
                fake_node_id.to_string(),
            )
            .await
            .expect("set node ID is idempotent");

        // The mapping should not have changed.
        let node_id = datastore
            .cockroachdb_node_id(&opctx, crdb_zone_id)
            .await
            .expect("looked up node ID");
        assert_eq!(node_id.as_deref(), Some(fake_node_id));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
