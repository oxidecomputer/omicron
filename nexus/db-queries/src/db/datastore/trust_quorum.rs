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
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::DbTrustQuorumConfigurationState;
use nexus_db_model::DbTrustQuorumMemberState;
use nexus_db_model::DbTypedUuid;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::TrustQuorumConfiguration as DbTrustQuorumConfiguration;
use nexus_db_model::TrustQuorumMember as DbTrustQuorumMember;
use nexus_types::trust_quorum::{TrustQuorumConfig, TrustQuorumMemberData};
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::OptionalLookupResult;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackKind;
use omicron_uuid_kinds::RackUuid;
use sled_agent_types::sled::BaseboardId;
use std::collections::{BTreeMap, BTreeSet};
use trust_quorum_protocol::{
    EncryptedRackSecrets, Epoch, Salt, Sha3_256Digest, Threshold,
};

macro_rules! bail_txn {
    ($err:ident, $($arg:tt),*) => {
        return Err($err.bail(
            omicron_common::api::external::Error::internal_error(&format!(
                $($arg),*
            ))
            .into()
        ));
    }
}

fn i64_to_epoch(val: i64) -> Result<Epoch, Error> {
    let Ok(epoch) = val.try_into() else {
        return Err(Error::internal_error(&format!(
            "Failed to convert i64 from database: {val} \
                into trust quroum epoch",
        )));
    };
    Ok(Epoch(epoch))
}

fn epoch_to_i64(epoch: Epoch) -> Result<i64, Error> {
    epoch.0.try_into().map_err(|_| {
        Error::internal_error(&format!(
            "Failed to convert trust quorum epoch to i64 in attempt to insert \
            into database: {epoch}"
        ))
    })
}

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
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Get the latest trust quorum configuration from the database
    pub async fn tq_get_latest_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> OptionalLookupResult<TrustQuorumConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        // First, retrieve our configuration if there is one.
        let Some(latest) =
            Self::tq_get_latest_config_conn(opctx, conn, rack_id)
                .await
                .map_err(|err| err.into_public_ignore_retries())?
        else {
            return Ok(None);
        };

        // Then get any members associated with the configuration
        let members =
            Self::tq_get_members_conn(opctx, conn, rack_id, latest.epoch)
                .await
                .map_err(|err| err.into_public_ignore_retries())?;

        let mut tq_members: BTreeMap<BaseboardId, TrustQuorumMemberData> =
            BTreeMap::new();
        let mut coordinator: Option<BaseboardId> = None;
        for (member, hw_baseboard_id) in members {
            let digest = if let Some(digest_str) = member.share_digest {
                let mut data = [0u8; 32];
                hex::decode_to_slice(&digest_str, &mut data).map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to decode share digest for trust quorum member \
                        {}:{} : {e}",
                        hw_baseboard_id.part_number,
                        hw_baseboard_id.serial_number
                    ))
                })?;
                Some(Sha3_256Digest(data))
            } else {
                None
            };

            // The coordinator is always a member of the group
            // We pull out its `BaseboardId` here.
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
                Error::internal_error(&format!(
                    "Failed to decode salt for trust quorum config: \
                    rack_id: {}, epoch: {}: {e}",
                    latest.rack_id, latest.epoch
                ))
            })?;
            Some(Salt(data))
        } else {
            None
        };

        let encrypted_rack_secrets = if let Some(salt) = salt {
            let Some(secrets) = latest.encrypted_rack_secrets else {
                // This should never happend due to constraint checks
                return Err(Error::internal_error(&format!(
                    "Salt exists, but secrets do not for trust quorum config: \
                    rack_id: {}, epoch: {}",
                    latest.rack_id, latest.epoch
                )));
            };
            Some(EncryptedRackSecrets::new(salt, secrets.into_boxed_slice()))
        } else {
            None
        };

        let Some(coordinator) = coordinator else {
            return Err(Error::internal_error(&format!(
                "Failed to find coordinator for hw_baseboard_id: {} \
                in trust quorum config.",
                latest.coordinator
            )));
        };

        Ok(Some(TrustQuorumConfig {
            rack_id: latest.rack_id.into(),
            epoch: i64_to_epoch(latest.epoch)?,
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
                    let current = Self::tq_get_latest_epoch_in_txn(
                        opctx,
                        &c,
                        config.rack_id,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let is_insertable = if let Some(epoch) = current {
                        // Only insert if what is in the DB is immediately prior to
                        // this configuration.
                        Some(epoch) == config.epoch.previous()
                    } else {
                        // Unconditional update is fine here, since a config
                        // doesn't exist TODO: Should we ensure that epoch == 1
                        // || epoch == 2 ?
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

                    Self::insert_tq_config_in_txn(opctx, conn, config)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// If this configuration is in the `Preparing` state, then update any
    /// members to acknowledge the prepare.
    ///
    /// Also, update any digests or encrypted rack secrets if necessary.
    /// Lastly, if enough members have acked prepares then commit the configuration.
    pub async fn tq_update_prepare_status(
        &self,
        opctx: &OpContext,
        config: trust_quorum_protocol::Configuration,
        acked_prepares: BTreeSet<BaseboardId>,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let epoch = epoch_to_i64(config.epoch)?;
        let rack_id = config.rack_id;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_update_prepare_status")
            .transaction(&conn, |c| {
                let err = err.clone();
                let config = config.clone();
                let acked_prepares = acked_prepares.clone();
                async move {
                    // First, retrieve our configuration if there is one.
                    let latest = Self::tq_get_latest_config_conn(opctx, &c, rack_id)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let Some(db_config) = latest else {
                        bail_txn!(
                            err,
                            "No trust quorum config for rack_id {} at epoch {}",
                            rack_id,
                            epoch
                        );
                    };

                    if db_config.epoch != epoch {
                        let actual = db_config.epoch;
                        bail_txn!(
                            err,
                            "Cannot update trust quorum config. \
                             Latest epoch does not match. Expected {}, Got {}",
                            epoch,
                            actual
                        );
                    }

                    // If we aren't preparing, then ignore this call. Multiple
                    // Nexuses race to completion and we don't want to worry
                    // about overwriting commits with prepares in the `state`
                    // field of each member.
                    if db_config.state
                        != DbTrustQuorumConfigurationState::Preparing
                    {
                        let state = db_config.state;
                        bail_txn!(
                            err,
                            "Ignoring stale update of trust quorum prepare \
                            status. Expected state = preparing, Got {:?}",
                            state
                        );
                    }

                    // Then get any members associated with the configuration
                    let db_members = Self::tq_get_members_conn(
                            opctx,
                            &c,
                            rack_id,
                            db_config.epoch,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    // We only update the configuration in the database if:
                    //  1. This is the first time we have seen encrypted rack secrets
                    //  2. We are transitioning from preparing to committed state.
                    let should_write_secrets =
                        db_config.encrypted_rack_secrets_salt.is_none()
                            && config.encrypted_rack_secrets.is_some();

                    let mut total_acks = 0;
                    for (mut member, hw_id) in db_members {
                        let mut update_share_digest = false;
                        let mut update_prepared = false;

                        let baseboard_id: BaseboardId = hw_id.into();

                        // Set the share digest for the member if we just learned it
                        if member.share_digest.is_none() {
                            let Some(digest) =
                                config.members.get(&baseboard_id)
                            else {
                                bail_txn!(
                                    err,
                                    "Cannot update share digest for {}. Not a \
                                    member of the trust quorum configuration.",
                                    baseboard_id
                                );
                            };
                           member.share_digest = Some(hex::encode(digest.0));
                           update_share_digest = true;
                        }

                        // Set the state of this member
                        if acked_prepares.contains(&baseboard_id)
                            && member.state == DbTrustQuorumMemberState::Unacked
                        {
                            update_prepared = true;
                            total_acks += 1;
                        }

                        if member.state == DbTrustQuorumMemberState::Prepared {
                            total_acks += 1;
                        }

                        // Write each member that has been modified
                        match (update_share_digest, update_prepared) {
                            (true, true) => {
                                Self::update_tq_member_share_digest_and_state_prepared_in_txn(
                                    opctx,
                                    conn,
                                    member
                                )
                                .await
                                .map_err(|txn_error| txn_error.into_diesel(&err))?;
                            }
                            (true, false) => {
                                Self::update_tq_member_share_digest_in_txn(
                                    opctx,
                                    conn,
                                    member
                                )
                                .await
                                .map_err(|txn_error| txn_error.into_diesel(&err))?;
                            }
                            (false, true) => {
                                Self::update_tq_member_state_prepared_in_txn(
                                    opctx,
                                    conn,
                                    member
                                )
                                .await
                                .map_err(|txn_error| txn_error.into_diesel(&err))?;
                            }
                            (false, false) => {
                                // Nothing to do
                            }
                        }
                    }


                    // Do we have enough acks to commit?
                    let should_commit = total_acks
                        >= (db_config.threshold.0
                            + db_config.commit_crash_tolerance.0)
                            as usize;

                    match (should_write_secrets, should_commit) {
                        (true, true) => {
                            Self::update_tq_encrypted_rack_secrets_and_commit_in_txn(
                                opctx,
                                conn,
                                db_config.rack_id,
                                db_config.epoch,
                                config.encrypted_rack_secrets.unwrap(),
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }
                        (true, false) => {
                            Self::update_tq_encrypted_rack_secrets_in_txn(
                                opctx,
                                conn,
                                db_config.rack_id,
                                db_config.epoch,
                                config.encrypted_rack_secrets.unwrap(),
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }
                        (false, true) => {
                            Self::update_tq_commit_state_in_txn(
                                opctx,
                                conn,
                                db_config.rack_id,
                                db_config.epoch)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }
                        (false, false) => {
                            // Nothing to do
                        }
                    }

                    Ok(())
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// If this configuration is in the `Committed` state, then update any
    /// members to acknowledge their commit acknowledgements.
    pub async fn tq_update_commit_status(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        epoch: Epoch,
        acked_commits: BTreeSet<BaseboardId>,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let epoch = epoch_to_i64(epoch)?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_update_commit_status")
            .transaction(&conn, |c| {
                let err = err.clone();
                let acked_commits = acked_commits.clone();
                async move {
                    // First, retrieve our configuration if there is one.
                    let latest =
                        Self::tq_get_latest_config_conn(opctx, &c, rack_id)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let Some(db_config) = latest else {
                        bail_txn!(
                            err,
                            "No trust quorum config for rack_id {} at epoch {}",
                            rack_id,
                            epoch
                        );
                    };

                    if db_config.epoch != epoch {
                        let actual = db_config.epoch;
                        bail_txn!(
                            err,
                            "Cannot update trust quorum config. \
                             Latest epoch does not match. Expected {}, Got {}",
                            epoch,
                            actual
                        );
                    }

                    // Nexus should not be retrieving committed acks if the
                    // configuration is `Preparing` or `Aborted`.
                    if db_config.state
                        != DbTrustQuorumConfigurationState::Committed
                    {
                        let state = db_config.state;
                        bail_txn!(
                            err,
                            "Invalid update of trust quorum commit status. \
                            Expected `Committed`, got {:?}",
                            state
                        );
                    }

                    Self::update_tq_members_state_commit_in_txn(
                        opctx,
                        conn,
                        rack_id.into(),
                        epoch,
                        acked_commits,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// Abort the configuration for a trust quorum if `epoch` is the latest per `rack_id`
    /// and the configuration has not been committed.
    ///
    /// This operation returns `Ok(())` if the configuration has already been
    /// aborted and it is still the latest configuration.
    pub async fn tq_abort_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let epoch = epoch_to_i64(epoch)?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_abort_config")
            .transaction(&conn, |c| {
                let err = err.clone();
                async move {
                    // First, retrieve our configuration if there is one.
                    let latest =
                        Self::tq_get_latest_config_conn(opctx, &c, rack_id)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let Some(db_config) = latest else {
                        bail_txn!(
                            err,
                            "No trust quorum config for rack_id {} at epoch {}",
                            rack_id,
                            epoch
                        );
                    };

                    if db_config.epoch != epoch {
                        let actual = db_config.epoch;
                        bail_txn!(
                            err,
                            "Cannot abort trust quorum config. \
                             Latest epoch does not match. Expected {}, Got {}",
                            epoch,
                            actual
                        );
                    }

                    if db_config.state
                        == DbTrustQuorumConfigurationState::Aborted
                    {
                        // Abort is idempotent
                        return Ok(());
                    }

                    // If we've already committed, we can't abort
                    if db_config.state
                        == DbTrustQuorumConfigurationState::Committed
                    {
                        bail_txn!(
                            err,
                            "Invalid update of trust quorum abort status. \
                            Expected `Preparing`, got `Committed`"
                        );
                    }

                    Self::update_tq_abort_state_in_txn(
                        opctx,
                        conn,
                        db_config.rack_id,
                        db_config.epoch,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    // Unconditional insert that should only run inside a transaction
    async fn insert_tq_config_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        config: TrustQuorumConfig,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let members = Self::lookup_hw_baseboard_ids_conn(
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

        let epoch = epoch_to_i64(config.epoch)
            .map_err(|e| TransactionError::from(e))?;

        // Insert the configuration
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        diesel::insert_into(dsl::trust_quorum_configuration)
            .values(DbTrustQuorumConfiguration {
                rack_id: config.rack_id.into(),
                epoch,
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
                epoch,
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

    async fn update_tq_member_share_digest_and_state_prepared_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        member: DbTrustQuorumMember,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(member.rack_id))
            .filter(dsl::epoch.eq(member.epoch))
            .filter(dsl::hw_baseboard_id.eq(member.hw_baseboard_id))
            .filter(dsl::share_digest.is_null())
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Unacked))
            .set((
                dsl::share_digest.eq(member.share_digest),
                dsl::state.eq(DbTrustQuorumMemberState::Prepared),
            ))
            .execute_async(conn)
            .await?;

        Ok(())
    }

    async fn update_tq_members_state_commit_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        acked_commits: BTreeSet<BaseboardId>,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let hw_baseboard_ids: Vec<_> = Self::lookup_hw_baseboard_ids_conn(
            opctx,
            conn,
            acked_commits.into_iter(),
        )
        .await?
        .into_iter()
        .map(|hw| hw.id)
        .collect();

        diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::hw_baseboard_id.eq_any(hw_baseboard_ids))
            .filter(dsl::share_digest.is_not_null())
            .filter(dsl::state.eq_any(vec![
                DbTrustQuorumMemberState::Unacked,
                DbTrustQuorumMemberState::Prepared,
            ]))
            .set(dsl::state.eq(DbTrustQuorumMemberState::Committed))
            .execute_async(conn)
            .await?;
        Ok(())
    }

    async fn update_tq_member_share_digest_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        member: DbTrustQuorumMember,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(member.rack_id))
            .filter(dsl::epoch.eq(member.epoch))
            .filter(dsl::hw_baseboard_id.eq(member.hw_baseboard_id))
            .filter(dsl::share_digest.is_null())
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Unacked))
            .set(dsl::share_digest.eq(member.share_digest))
            .execute_async(conn)
            .await?;

        Ok(())
    }

    async fn update_tq_member_state_prepared_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        member: DbTrustQuorumMember,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(member.rack_id))
            .filter(dsl::epoch.eq(member.epoch))
            .filter(dsl::hw_baseboard_id.eq(member.hw_baseboard_id))
            .filter(dsl::share_digest.is_not_null())
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Unacked))
            .set(dsl::state.eq(DbTrustQuorumMemberState::Prepared))
            .execute_async(conn)
            .await?;

        Ok(())
    }

    async fn update_tq_encrypted_rack_secrets_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        encrypted_rack_secrets: EncryptedRackSecrets,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let salt = Some(hex::encode(encrypted_rack_secrets.salt.0));
        let secrets: Option<Vec<u8>> = Some(encrypted_rack_secrets.data.into());

        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::encrypted_rack_secrets_salt.is_null())
            .filter(dsl::encrypted_rack_secrets.is_null())
            .set((
                dsl::encrypted_rack_secrets_salt.eq(salt),
                dsl::encrypted_rack_secrets.eq(secrets),
            ))
            .execute_async(conn)
            .await?;

        Ok(())
    }

    async fn update_tq_encrypted_rack_secrets_and_commit_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        encrypted_rack_secrets: EncryptedRackSecrets,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let salt = Some(hex::encode(encrypted_rack_secrets.salt.0));
        let secrets: Option<Vec<u8>> = Some(encrypted_rack_secrets.data.into());

        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::encrypted_rack_secrets_salt.is_null())
            .filter(dsl::encrypted_rack_secrets.is_null())
            .filter(dsl::state.eq(DbTrustQuorumConfigurationState::Preparing))
            .set((
                dsl::encrypted_rack_secrets_salt.eq(salt),
                dsl::encrypted_rack_secrets.eq(secrets),
                dsl::state.eq(DbTrustQuorumConfigurationState::Committed),
            ))
            .execute_async(conn)
            .await?;

        Ok(())
    }

    /// Returns the number of rows update
    async fn update_tq_commit_state_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
    ) -> Result<usize, TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let num_rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq(DbTrustQuorumConfigurationState::Preparing))
            .set(dsl::state.eq(DbTrustQuorumConfigurationState::Committed))
            .execute_async(conn)
            .await?;

        Ok(num_rows_updated)
    }

    async fn update_tq_abort_state_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
    ) -> Result<(), TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq(DbTrustQuorumConfigurationState::Preparing))
            .set(dsl::state.eq(DbTrustQuorumConfigurationState::Aborted))
            .execute_async(conn)
            .await?;

        Ok(())
    }

    async fn lookup_hw_baseboard_ids_conn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        members: impl Iterator<Item = BaseboardId>,
    ) -> Result<Vec<HwBaseboardId>, TransactionError<Error>> {
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
            .load_async(conn)
            .await
            .map_err(TransactionError::Database)
    }

    async fn tq_get_latest_epoch_in_txn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<Option<Epoch>, TransactionError<Error>> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        let Some(latest_epoch) = dsl::trust_quorum_configuration
            .filter(dsl::rack_id.eq(rack_id.into_untyped_uuid()))
            .order_by(dsl::epoch.desc())
            .select(dsl::epoch)
            .first_async::<i64>(conn)
            .await
            .optional()?
        else {
            return Ok(None);
        };
        let latest_epoch = i64_to_epoch(latest_epoch)?;
        Ok(Some(latest_epoch))
    }

    async fn tq_get_latest_config_conn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<Option<DbTrustQuorumConfiguration>, TransactionError<Error>>
    {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let latest = dsl::trust_quorum_configuration
            .filter(dsl::rack_id.eq(rack_id.into_untyped_uuid()))
            .order_by(dsl::epoch.desc())
            .first_async::<DbTrustQuorumConfiguration>(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest)
    }

    async fn tq_get_members_conn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: i64,
    ) -> Result<
        Vec<(DbTrustQuorumMember, HwBaseboardId)>,
        TransactionError<Error>,
    > {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::hw_baseboard_id::dsl as hw_baseboard_id_dsl;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let members = dsl::trust_quorum_member
            .filter(dsl::rack_id.eq(rack_id.into_untyped_uuid()))
            .filter(dsl::epoch.eq(epoch))
            .inner_join(
                hw_baseboard_id_dsl::hw_baseboard_id
                    .on(hw_baseboard_id_dsl::id.eq(dsl::hw_baseboard_id)),
            )
            .select((
                DbTrustQuorumMember::as_select(),
                HwBaseboardId::as_select(),
            ))
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(members)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::{HwBaseboardId, LrtqMember};
    use nexus_types::trust_quorum::{
        TrustQuorumConfigState, TrustQuorumMemberState,
    };
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
        let mut config = TrustQuorumConfig {
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

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(config, read_config);

        // Inserting the same config again should fail
        datastore
            .tq_insert_latest_config(opctx, config.clone())
            .await
            .expect_err("duplicate insert should fail");

        // Bumping the epoch and inserting should succeed
        config.epoch = Epoch(2);
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(config, read_config);

        // We should get an error if we try to insert with a coordinator that is
        // not part of the membership.
        config.epoch = Epoch(3);
        let saved_serial = config.coordinator.serial_number.clone();
        config.coordinator.serial_number = "dummy".to_string();
        datastore
            .tq_insert_latest_config(opctx, config.clone())
            .await
            .expect_err("insert should fail with invalid coordinator");

        // Restoring the serial number should succeed
        config.coordinator.serial_number = saved_serial;
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(config, read_config);

        // Incrementing the epoch by more than one should fail
        config.epoch = Epoch(5);
        datastore
            .tq_insert_latest_config(opctx, config.clone())
            .await
            .expect_err(
                "insert should fail because previous epoch is incorrect",
            );
    }

    #[tokio::test]
    async fn test_tq_update_prepare_and_commit_normal_case() {
        let logctx =
            test_setup_log("test_tq_update_prepare_and_commit_normal_case");
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

        // A configuration returned from a coordinator is different
        let coordinator_config = trust_quorum_protocol::Configuration {
            rack_id: config.rack_id,
            epoch: config.epoch,
            coordinator: hw_ids.first().unwrap().clone().into(),
            members: config
                .members
                .keys()
                .cloned()
                .map(|id| (id, Sha3_256Digest([0u8; 32])))
                .collect(),
            threshold: config.threshold,
            encrypted_rack_secrets: None,
        };

        // Ack only the coordinator
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                [coordinator_config.coordinator.clone()].into_iter().collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // Ensure that Nexus has only seen the coordinator ack and that it has
        // not yet committed. There should also be no encrypted rack secrets,
        // and all members should now have share digests.
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Preparing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        for (id, info) in &read_config.members {
            assert!(info.digest.is_some());
            if *id == coordinator_config.coordinator {
                assert_eq!(info.state, TrustQuorumMemberState::Prepared);
            } else {
                assert_eq!(info.state, TrustQuorumMemberState::Unacked);
            }
        }

        // Ack a threshold of peers.
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(config.threshold.0 as usize)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // We've acked a threshold of nodes, but still should not have committed
        // because we haven't yet acked the `commit_crash_tolerance` number of
        // nodes in addition.
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Preparing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            config.threshold.0 as usize,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Prepared
                )
                .count()
        );

        // Ack an additional `commit_crash_tolerance` of nodes. This should
        // trigger a commit.
        let acked_prepares = config.threshold.0 as usize
            + config.commit_crash_tolerance as usize;

        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // We've acked enough nodes and should have committed
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Committed);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            acked_prepares,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Prepared
                )
                .count()
        );

        // Future prepare acks should fail because we have already committed.
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap_err();

        // Commit at all nodes
        datastore
            .tq_update_commit_status(
                opctx,
                rack_id,
                config.epoch,
                coordinator_config.members.keys().cloned().collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Committed);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(
            read_config.members.iter().all(
                |(_, info)| info.state == TrustQuorumMemberState::Committed
            )
        );

        // Repeating the same update and read succeeds
        datastore
            .tq_update_commit_status(
                opctx,
                rack_id,
                config.epoch,
                coordinator_config.members.keys().cloned().collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Committed);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(
            read_config.members.iter().all(
                |(_, info)| info.state == TrustQuorumMemberState::Committed
            )
        );
    }

    #[tokio::test]
    async fn test_tq_abort() {
        let logctx = test_setup_log("test_tq_abort");
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

        // Aborting should succeed, since we haven't committed
        datastore
            .tq_abort_config(opctx, config.rack_id, config.epoch)
            .await
            .unwrap();

        // Aborting is idempotent
        datastore
            .tq_abort_config(opctx, config.rack_id, config.epoch)
            .await
            .unwrap();

        // Committing will fail to update any rows
        // (This is not directly callable from a public API).
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            let num_rows_updated = DataStore::update_tq_commit_state_in_txn(
                opctx,
                &conn,
                config.rack_id.into(),
                config.epoch.0 as i64,
            )
            .await
            .unwrap();
            assert_eq!(num_rows_updated, 0);
        }

        // A configuration returned from a coordinator is different
        let coordinator_config = trust_quorum_protocol::Configuration {
            rack_id: config.rack_id,
            epoch: config.epoch,
            coordinator: hw_ids.first().unwrap().clone().into(),
            members: config
                .members
                .keys()
                .cloned()
                .map(|id| (id, Sha3_256Digest([0u8; 32])))
                .collect(),
            threshold: config.threshold,
            encrypted_rack_secrets: None,
        };

        // This is how we actually try to trigger commit operations. This should fail outright.
        let acked_prepares = config.threshold.0 as usize
            + config.commit_crash_tolerance as usize;
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap_err();

        // Retrieve the configuration and ensure it is actually aborted
        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");
        assert_eq!(read_config.state, TrustQuorumConfigState::Aborted);

        // Create a second config
        let config2 = TrustQuorumConfig { epoch: Epoch(2), ..config.clone() };
        datastore
            .tq_insert_latest_config(opctx, config2.clone())
            .await
            .unwrap();

        // Trying to abort the old config will fail because it's stale
        datastore
            .tq_abort_config(opctx, config.rack_id, config.epoch)
            .await
            .unwrap_err();

        // Commit it
        let coordinator_config2 = trust_quorum_protocol::Configuration {
            epoch: config2.epoch,
            ..coordinator_config
        };
        let acked_prepares = config2.threshold.0 as usize
            + config2.commit_crash_tolerance as usize;
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config2.clone(),
                coordinator_config2
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        // Abort of latest config should fail because it has already committed
        datastore
            .tq_abort_config(opctx, config2.rack_id, config2.epoch)
            .await
            .unwrap_err();
    }
}
