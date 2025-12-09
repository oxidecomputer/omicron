// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum related queries

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::TrustQuorumConfiguration as DbTrustQuorumConfiguration;
use nexus_db_model::TrustQuorumMember as DbTrustQuorumMember;
use nexus_types::trust_quorum::BaseboardId;
use nexus_types::trust_quorum::{
    TrustQuorumConfig, TrustQuorumConfigState, TrustQuorumMemberData,
    TrustQuorumMemberState,
};
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::OptionalLookupResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use std::collections::{BTreeMap, BTreeSet};
use trust_quorum_protocol::EncryptedRackSecrets;
use trust_quorum_protocol::Salt;
use trust_quorum_protocol::Sha3_256Digest;

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

    pub async fn tq_latest_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> OptionalLookupResult<TrustQuorumConfig> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
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
            let secrets =
                latest.encrypted_rack_secrets.unwrap_or_else(|| {
                    // This should never happend due to constraint checks
                    Error::InternalError {
                        internal_message: format!(
                            "salt exists, but secrets do not for TQ:\
                             rack_id: {}, epoch: {}"
                            latest.rack_id, latest.epoch
                        ),
                    }
                })?;
            Some(EncryptedRackSecrets::new(salt, secrets))
        } else {
            None
        };

        let coordinator = coordinator.unwrap_or_else(|| {
            return Err(Error::InternalError {
                internal_message: format!(
                    "Failed to find coordinator for id: {}",
                    latest.coordinator
                ),
            });
        });

        Ok(Some(TrustQuorumConfig {
            rack_id: latest.rack_id.into(),
            epoch: latest.epoch.try_into().unwrap(),
            state: latest.state.into(),
            threshold: Threshold(latest.threshold.into()),
            commit_crash_tolerance: latest.commit_crash_tolerance.into(),
            coordinator,
            encrypted_rack_secrets,
            members: tq_members,
        }))
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

    async fn insert_hw_baseboard_ids(db: &TestDatabase) -> Vec<Uuid> {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::hw_baseboard_id::dsl;
        let mut uuids = Vec::new();
        for i in 0..10 {
            let uuid = Uuid::new_v4();
            uuids.push(uuid);
            diesel::insert_into(dsl::hw_baseboard_id)
                .values(HwBaseboardId {
                    id: uuid,
                    part_number: "test-part".to_string(),
                    serial_number: i.to_string(),
                })
                .execute_async(&*conn)
                .await
                .unwrap();
        }
        uuids
    }

    async fn insert_lrtq_members(
        db: &TestDatabase,
        rack_id1: RackUuid,
        rack_id2: RackUuid,
        hw_ids: Vec<Uuid>,
    ) {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::lrtq_member::dsl;
        for (i, &hw_baseboard_id) in hw_ids.iter().enumerate() {
            let rack_id = if i < 5 { rack_id1.into() } else { rack_id2.into() };
            diesel::insert_into(dsl::lrtq_member)
                .values(LrtqMember { rack_id, hw_baseboard_id })
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
}
