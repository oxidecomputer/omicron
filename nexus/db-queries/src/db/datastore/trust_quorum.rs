// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum related queries

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::CollectorReassignment;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::TrustQuorumConfiguration as DbTrustQuorumConfiguration;
use nexus_db_model::TrustQuorumMember as DbTrustQuorumMember;
use nexus_types::trust_quorum::{
    TrustQuorumConfig, TrustQuorumConfigState, TrustQuorumMemberData,
    TrustQuorumMemberState,
};
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::OptionalLookupResult;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use std::collections::{BTreeMap, BTreeSet};
use trust_quorum_protocol::{
    BaseboardId, EncryptedRackSecrets, Epoch, Salt, Sha3_256Digest, Threshold,
};

impl DataStore {
    /// Return all `HwBaseboardId`s for a given rack that has run LRTQ
    ///
    /// No need for pagination, as there at most 32 member sleds per rack
    pub async fn lrtq_members(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> ListResultVec<HwBaseboardId> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::hw_baseboard_id::dsl as hw_baseboard_id_dsl;
        use nexus_db_schema::schema::lrtq_member::dsl as lrtq_member_dsl;

        lrtq_member_dsl::lrtq_member
            .filter(lrtq_member_dsl::rack_id.eq(rack_id.into_untyped_uuid()))
            .inner_join(hw_baseboard_id_dsl::hw_baseboard_id.on(
                hw_baseboard_id_dsl::id.eq(lrtq_member_dsl::hw_baseboard_id),
            ))
            .select(HwBaseboardId::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    // TODO: Probably abstract out some of this error handling.
    pub async fn tq_latest_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> OptionalLookupResult<TrustQuorumConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::hw_baseboard_id::dsl as hw_baseboard_id_dsl;
        use nexus_db_schema::schema::trust_quorum_configuration::dsl as tq_config_dsl;
        use nexus_db_schema::schema::trust_quorum_member::dsl as tq_member_dsl;

        // First, retrieve our configuration if there is one.
        let Some(latest) = tq_config_dsl::trust_quorum_configuration
            .filter(tq_config_dsl::rack_id.eq(rack_id.into_untyped_uuid()))
            .order_by(tq_config_dsl::epoch.desc())
            .first_async::<DbTrustQuorumConfiguration>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
        else {
            return Ok(None);
        };

        // Then get any members associated with the configuration
        let members: Vec<(DbTrustQuorumMember, HwBaseboardId)> =
            tq_member_dsl::trust_quorum_member
                .filter(tq_member_dsl::rack_id.eq(rack_id.into_untyped_uuid()))
                .filter(tq_member_dsl::epoch.eq(latest.epoch))
                .inner_join(hw_baseboard_id_dsl::hw_baseboard_id.on(
                    hw_baseboard_id_dsl::id.eq(tq_member_dsl::hw_baseboard_id),
                ))
                .select((
                    DbTrustQuorumMember::as_select(),
                    HwBaseboardId::as_select(),
                ))
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

        let mut tq_members: BTreeMap<BaseboardId, TrustQuorumMemberData> =
            BTreeMap::new();
        let mut coordinator: Option<BaseboardId> = None;
        for (member, hw_baseboard_id) in members {
            let digest = if let Some(digest_str) = member.share_digest {
                let mut data = [0u8; 32];
                hex::decode_to_slice(&digest_str, &mut data).map_err(|e| {
                    Error::InternalError {
                        internal_message: format!(
                            "failed to decode share digest for {}:{} : {e}",
                            hw_baseboard_id.part_number,
                            hw_baseboard_id.serial_number
                        ),
                    }
                })?;
                Some(Sha3_256Digest(data))
            } else {
                None
            };
            // The coordinator is always a member of the group
            // We pull out it's BaseboardId here.
            if latest.coordinator == hw_baseboard_id.id {
                coordinator = Some(hw_baseboard_id.clone().into());
            }
            tq_members.insert(
                hw_baseboard_id.into(),
                TrustQuorumMemberData { state: member.state.into(), digest },
            );
        }

        let salt = if let Some(salt_str) = latest.encrypted_rack_secrets_salt {
            let mut data = [0u8; 32];
            hex::decode_to_slice(&salt_str, &mut data).map_err(|e| {
                Error::InternalError {
                    internal_message: format!(
                        "failed to decode salt for TQ: \
                        rack_id: {}, epoch: {}: {e}",
                        latest.rack_id, latest.epoch
                    ),
                }
            })?;
            Some(Salt(data))
        } else {
            None
        };

        let encrypted_rack_secrets = if salt.is_some() {
            let Some(secrets) = latest.encrypted_rack_secrets else {
                // This should never happend due to constraint checks
                return Err(Error::InternalError {
                    internal_message: format!(
                        "salt exists, but secrets do not for TQ:\
                             rack_id: {}, epoch: {}",
                        latest.rack_id, latest.epoch
                    ),
                });
            };
            Some(EncryptedRackSecrets::new(
                salt.unwrap(),
                secrets.into_boxed_slice(),
            ))
        } else {
            None
        };

        let Some(coordinator) = coordinator else {
            return Err(Error::InternalError {
                internal_message: format!(
                    "Failed to find coordinator for id: {}",
                    latest.coordinator
                ),
            });
        };

        Ok(Some(TrustQuorumConfig {
            rack_id: latest.rack_id.into(),
            epoch: Epoch(latest.epoch.try_into().unwrap()),
            state: latest.state.into(),
            threshold: Threshold(latest.threshold.into()),
            commit_crash_tolerance: latest.commit_crash_tolerance.into(),
            coordinator,
            encrypted_rack_secrets,
            members: tq_members,
        }))
    }

    /// Insert a new trust quorum configuration, but only if it is equivalent
    /// to the highest epoch of the last configuration + 1.
    pub async fn tq_insert_latest_config(
        &self,
        opctx: &OpContext,
        config: TrustQuorumConfig,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_insert_latest_config")
            .transaction(&conn, |c| {
                let err = err.clone();
                let config = config.clone();

                async move {
                    let current = self
                        .tq_get_latest_epoch_in_txn(opctx, &c, config.rack_id)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let is_insertable = if let Some(epoch) = current.clone() {
                        // Only insert if what is in the DB is immediately prior to
                        // this configuration.
                        Some(epoch) == config.epoch.previous()
                    } else {
                        // Unconditional update is fine here, since a config doesn't
                        // exist TODO: Should we ensure that epoch == 1 || epoch ==
                        // 2 ?
                        true
                    };

                    if !is_insertable {
                        return Err(err.bail(TransactionError::CustomError(
                            Error::conflict(format!(
                                "expected current TQ epoch for rack_id \
                            {} to be {:?}, found {:?}",
                                config.rack_id,
                                config.epoch.previous(),
                                current
                            )),
                        )));
                    }

                    self.insert_tq_config_in_txn(opctx, conn, config)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    // Unconditional insert that should only run inside a transaction
    async fn insert_tq_config_in_txn(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        config: TrustQuorumConfig,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let members = self
            .lookup_hw_baseboard_ids_conn(
                opctx,
                conn,
                config.members.keys().cloned(),
            )
            .await?;

        let (salt, secrets) =
            config.encrypted_rack_secrets.map_or((None, None), |s| {
                (Some(hex::encode(s.salt.0)), Some(s.data.into()))
            });

        // Max of 32 members to search. We could use binary search if we sorted
        // the output with an `order_by` in the DB query, or speed up search
        // if converted to a map. Neither seems necessary for such a rare
        // operation.
        let coordinator_id = members.iter().find(|m| {
            m.part_number == config.coordinator.part_number
                && m.serial_number == config.coordinator.serial_number
        });
        bail_unless!(
            coordinator_id.is_some(),
            "Coordinator: {} is not a member of the trust quorum",
            config.coordinator
        );
        let coordinator_id = coordinator_id.unwrap().id;

        // Insert the configuration
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        diesel::insert_into(dsl::trust_quorum_configuration)
            .values(DbTrustQuorumConfiguration {
                rack_id: config.rack_id.into(),
                epoch: config.epoch.0.try_into().unwrap(),
                state: config.state.into(),
                threshold: config.threshold.0.into(),
                commit_crash_tolerance: config.commit_crash_tolerance.into(),
                coordinator: coordinator_id,
                encrypted_rack_secrets_salt: salt,
                encrypted_rack_secrets: secrets,
            })
            .execute_async(conn)
            .await?;

        // Insert the members
        let members: Vec<_> = members
            .into_iter()
            .map(|m| DbTrustQuorumMember {
                rack_id: config.rack_id.into(),
                epoch: config.epoch.0.try_into().unwrap(),
                hw_baseboard_id: m.id,
                state: nexus_db_model::DbTrustQuorumMemberState::Unacked,
                share_digest: None,
            })
            .collect();

        use nexus_db_schema::schema::trust_quorum_member::dsl as members_dsl;
        diesel::insert_into(members_dsl::trust_quorum_member)
            .values(members)
            .execute_async(conn)
            .await?;

        Ok(())
    }

    async fn lookup_hw_baseboard_ids_conn(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        members: impl Iterator<Item = BaseboardId>,
    ) -> ListResultVec<HwBaseboardId> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::hw_baseboard_id::dsl;

        let (parts, serials): (Vec<_>, Vec<_>) = members
            .into_iter()
            .map(|m| (m.part_number, m.serial_number))
            .collect();

        dsl::hw_baseboard_id
            .filter(dsl::part_number.eq_any(parts))
            .filter(dsl::serial_number.eq_any(serials))
            .select(HwBaseboardId::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    async fn tq_get_latest_epoch_in_txn(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<Option<Epoch>, TransactionError<Error>> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        let latest_epoch = dsl::trust_quorum_configuration
            .filter(dsl::rack_id.eq(rack_id.into_untyped_uuid()))
            .order_by(dsl::epoch.desc())
            .select(dsl::epoch)
            .first_async::<i64>(conn)
            .await
            .optional()?
            .map(|epoch| Epoch(epoch.try_into().unwrap()));
        Ok(latest_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::{HwBaseboardId, LrtqMember};
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::RackUuid;
    use uuid::Uuid;

    async fn insert_hw_baseboard_ids(db: &TestDatabase) -> Vec<HwBaseboardId> {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::hw_baseboard_id::dsl;
        let hw_baseboard_ids: Vec<_> = (0..10)
            .map(|i| HwBaseboardId {
                id: Uuid::new_v4(),
                part_number: "test-part".to_string(),
                serial_number: i.to_string(),
            })
            .collect();

        diesel::insert_into(dsl::hw_baseboard_id)
            .values(hw_baseboard_ids.clone())
            .execute_async(&*conn)
            .await
            .unwrap();

        hw_baseboard_ids
    }

    async fn insert_lrtq_members(
        db: &TestDatabase,
        rack_id1: RackUuid,
        rack_id2: RackUuid,
        hw_ids: Vec<HwBaseboardId>,
    ) {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::lrtq_member::dsl;
        for (i, hw_baseboard_id) in hw_ids.into_iter().enumerate() {
            let rack_id = if i < 5 { rack_id1.into() } else { rack_id2.into() };
            diesel::insert_into(dsl::lrtq_member)
                .values(LrtqMember {
                    rack_id,
                    hw_baseboard_id: hw_baseboard_id.id,
                })
                .execute_async(&*conn)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_lrtq_members() {
        let logctx = test_setup_log("test_lrtq_members");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let rack_id1 = RackUuid::new_v4();
        let rack_id2 = RackUuid::new_v4();

        // Listing lrtq members should return an empty vec
        assert!(
            datastore.lrtq_members(opctx, rack_id1).await.unwrap().is_empty()
        );

        // Insert some data
        let hw_ids = insert_hw_baseboard_ids(&db).await;
        insert_lrtq_members(&db, rack_id1, rack_id2, hw_ids.clone()).await;

        let hw_baseboard_ids1 =
            datastore.lrtq_members(opctx, rack_id1).await.unwrap();
        println!("{:?}", hw_baseboard_ids1);
        assert_eq!(hw_baseboard_ids1.len(), 5);
        let hw_baseboard_ids2 =
            datastore.lrtq_members(opctx, rack_id2).await.unwrap();
        assert_eq!(hw_baseboard_ids2.len(), 5);
        assert_ne!(hw_baseboard_ids1, hw_baseboard_ids2);
    }

    #[tokio::test]
    async fn test_insert_latest_tq_round_trip() {
        let logctx = test_setup_log("test_insert_latest_tq_round_trip");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;

        let rack_id = RackUuid::new_v4();

        // Create an initial config
        let config = TrustQuorumConfig {
            rack_id,
            epoch: Epoch(1),
            state: TrustQuorumConfigState::Preparing,
            threshold: Threshold((hw_ids.len() / 2 + 1) as u8),
            commit_crash_tolerance: 2,
            coordinator: hw_ids.first().unwrap().clone().into(),
            encrypted_rack_secrets: None,
            members: hw_ids
                .clone()
                .into_iter()
                .map(|m| (m.into(), TrustQuorumMemberData::new()))
                .collect(),
        };

        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        let config2 = datastore
            .tq_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(config, config2);
    }
}
