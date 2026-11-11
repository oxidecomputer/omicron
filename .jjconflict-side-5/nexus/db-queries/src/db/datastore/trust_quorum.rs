// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum related queries
//!
//! All methods that end with `_conn` in this module take a connection as a
//! parameter that is already authorized, so there is no need to reauthorize.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::to_db_sled_policy;
use crate::db::pagination::paginated;
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
use nexus_types::external_api::sled::SledPolicy;
use nexus_types::trust_quorum::IsLrtqUpgrade;
use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
use nexus_types::trust_quorum::{
    TrustQuorumConfig, TrustQuorumConfigState, TrustQuorumMemberData,
    TrustQuorumMemberState,
};
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::OptionalLookupResult;
use omicron_common::bail_unless;
use omicron_common::now_db_precision;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackKind;
use omicron_uuid_kinds::RackUuid;
use rand::rng;
use rand::seq::IteratorRandom;
use sled_hardware_types::BaseboardId;
use std::collections::{BTreeMap, BTreeSet};
use trust_quorum_types::membership::TrustQuorumMembers;
use trust_quorum_types::{
    crypto::{EncryptedRackSecrets, Salt, Sha3_256Digest},
    types::{Epoch, Threshold},
};

/// Return the error corresponding to no in-service sleds being available in this rack.
///
///
/// Conflict rather than ServiceUnavailable because dropshot replaces a 503's
/// message with "Service Unavailable", and the operator needs this one.
fn no_in_service_coordinator_error(
    proposed: &ProposedTrustQuorumConfig,
    candidates: &BTreeSet<BaseboardId>,
) -> Error {
    let candidate_rule = match proposed.is_lrtq_upgrade {
        IsLrtqUpgrade::No { last_committed_epoch } => format!(
            "a member of the proposed configuration that acknowledged the \
             commit of epoch {last_committed_epoch}"
        ),
        IsLrtqUpgrade::Yes => {
            String::from("a member of the proposed LRTQ upgrade configuration")
        }
    };
    let candidates = itertools::join(candidates, ", ");
    let ProposedTrustQuorumConfig { rack_id, epoch, .. } = proposed;

    // We return Conflict (409) rather than Service Unavailable (503) because we
    // hide 5xx errors and the operator would benefit from this message.
    Error::conflict(format!(
        "Cannot propose trust quorum configuration at epoch \
         {epoch} for rack {rack_id}, since no sled can coordinate it. A \
         coordinator must be {candidate_rule}, with an in-service sled in \
         this rack, and none of the candidates ({candidates}) has one. If \
         the rack was just initialized or these sleds were just added, wait \
         for them to register with the control plane and retry. Otherwise, \
         contact Oxide support."
    ))
}

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

/// An outcome of [`DataStore::tq_update_prepare_status`].
///
/// Other states (e.g., not latest, no longer preparing) are returned as errors.
#[derive(Debug, PartialEq, Eq)]
pub enum TqPrepareStatusOutcome {
    /// Trust quorum is still in the `Preparing` or `PreparingLrtqUpgrade`
    /// states.
    // (This doesn't carry the config for purely instrumental reasons -- no
    // caller needs the config here.)
    StillPreparing,
    /// Trust quorum has moved to the `Committing` state.
    Committing {
        /// The trust quorum config.
        ///
        /// The config is read at the end of the update's transaction, so it
        /// includes everything written by the `tq_update_prepare_status` call.
        config: Box<TrustQuorumConfig>,
    },
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

#[derive(Debug, PartialEq, Eq)]
enum TqAbortAction {
    AlreadyAborted,
    Abort,
}

/// Decide what [`DataStore::tq_abort_config`] does with the config at `epoch`,
/// given its state (None if there is no config at that epoch) and the latest
/// epoch for the rack.
fn classify_tq_abort(
    rack_id: RackUuid,
    epoch: i64,
    latest_epoch: Option<i64>,
    state_at_epoch: Option<DbTrustQuorumConfigurationState>,
) -> Result<TqAbortAction, Error> {
    let (Some(state), Some(latest_epoch)) = (state_at_epoch, latest_epoch)
    else {
        return Err(Error::non_resourcetype_not_found(format!(
            "No trust quorum configuration exists at epoch {epoch} for rack \
             {rack_id}."
        )));
    };
    let state = match state {
        DbTrustQuorumConfigurationState::Aborted => {
            return Ok(TqAbortAction::AlreadyAborted);
        }
        DbTrustQuorumConfigurationState::Preparing
        | DbTrustQuorumConfigurationState::PreparingLrtqUpgrade => {
            if latest_epoch == epoch {
                return Ok(TqAbortAction::Abort);
            }
            // Inserting a newer config is refused while the latest one is still
            // preparing, so this is an internal error.
            return Err(Error::internal_error(&format!(
                "Trust quorum configuration at epoch {epoch} for rack \
                 {rack_id} is still in state {state:?}, but the latest \
                 epoch is {latest_epoch}"
            )));
        }
        DbTrustQuorumConfigurationState::Committing => "committing",
        DbTrustQuorumConfigurationState::Committed => "committed",
        DbTrustQuorumConfigurationState::CommittedPartially => {
            "partially committed"
        }
    };
    Err(Error::conflict(format!(
        "Cannot abort the trust quorum configuration at epoch {epoch} for \
         rack {rack_id}: it is already {state} and can no longer be aborted."
    )))
}

// A configuration returned from `validate_new_config_conn` that is safe to
// be inserted into the database inside a transaction.
struct ValidatedConfig {
    config: TrustQuorumConfig,
    // The last committed configuration should have its state changed from
    // committing to committed-partially.
    commit_partially_epoch: Option<Epoch>,
}

impl DataStore {
    /// Return any active `RackUuid` for each trust quorum along with its latest
    /// `Epoch`.
    ///
    /// An active trust quorum configuration is one that has not `Committed`
    /// or `Aborted`, which means that nexus has more work to do on that
    /// configuration.
    ///
    /// For now, since we do not have multirack, and we aren't sure how big
    /// those clusters are going to be we return all values and don't bother
    /// paginating. The current `SQL_BATCH_SIZE` is also 1000, and it's unlikely
    /// that there will ever be more than 1000 racks in a single fleet, sharing
    /// a single CRDB cluster.
    pub async fn tq_get_all_active_rack_id_and_latest_epoch(
        &self,
        opctx: &OpContext,
    ) -> Result<BTreeMap<RackUuid, Epoch>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let values: Vec<(DbTypedUuid<RackKind>, i64)> =
            dsl::trust_quorum_configuration
                .filter(dsl::state.eq_any(vec![
                    DbTrustQuorumConfigurationState::Preparing,
                    DbTrustQuorumConfigurationState::PreparingLrtqUpgrade,
                    DbTrustQuorumConfigurationState::Committing,
                ]))
                .select((dsl::rack_id, dsl::epoch))
                .order_by((dsl::rack_id, dsl::epoch.desc()))
                .load_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

        let mut output = BTreeMap::new();
        for (rack_id, epoch) in values {
            output.insert(rack_id.into(), i64_to_epoch(epoch)?);
        }

        Ok(output)
    }

    /// This is a special method called during `rack_initialize` that inserts a
    /// configuration with all nodes acked. It specifically does no validation
    /// and is only expected to be called once during the lifetime of a rack.
    ///
    /// For reconfiguration and lrtq upgrade we always call
    /// `tq_insert_latest_config`.
    pub async fn tq_insert_rss_config_after_handoff(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_tq: authz::TrustQuorumConfig,
        initial_members: BTreeSet<BaseboardId>,
        coordinator: BaseboardId,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz_tq).await?;

        let rack_id = authz_tq.rack().id();
        let initial_members = TrustQuorumMembers::new(initial_members)
            .map_err(|error| Error::invalid_request(error.to_string()))?;
        let initial_config = TrustQuorumConfig::new_rss_committed_config(
            rack_id,
            initial_members,
            coordinator,
        );

        Self::insert_tq_config_conn(conn, initial_config)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    /// Get the latest trust quorum configuration from the database
    pub async fn tq_get_latest_config(
        &self,
        opctx: &OpContext,
        authz_tq: authz::TrustQuorumConfig,
    ) -> OptionalLookupResult<TrustQuorumConfig> {
        opctx.authorize(authz::Action::Read, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;
        let rack_id = authz_tq.rack().id();

        Self::tq_get_latest_config_with_members_conn(conn, rack_id)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    pub async fn tq_list_config(
        &self,
        opctx: &OpContext,
        authz_tq: authz::TrustQuorumConfig,
        pagparams: &DataPageParams<'_, i64>,
    ) -> ListResultVec<DbTrustQuorumConfiguration> {
        opctx.authorize(authz::Action::Read, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let rack_id = authz_tq.rack().id();
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        paginated(dsl::trust_quorum_configuration, dsl::epoch, pagparams)
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .select(DbTrustQuorumConfiguration::as_select())
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Get the trust quorum configuration from the database for the given Epoch
    pub async fn tq_get_config(
        &self,
        opctx: &OpContext,
        authz_tq: authz::TrustQuorumConfig,
        epoch: Epoch,
    ) -> OptionalLookupResult<TrustQuorumConfig> {
        opctx.authorize(authz::Action::Read, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;
        let rack_id = authz_tq.rack().id();

        Self::tq_get_config_with_members_from_epoch_conn(conn, rack_id, epoch)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    /// Load the latest committed trust quorum configuration for `rack_id` and
    /// return it along with the epoch for the latest configuration.
    ///
    /// If the configuration is aborted, then check to see what its latest
    /// `last_committed_epoch` is and load that configuration.
    ///
    /// Note that the configuration that comes back may not actually be
    /// committed yet if it's the latest configuration. That is ok because we
    /// will perform validation during insert of the any newly proposed config.
    pub async fn tq_load_latest_possible_committed_config(
        &self,
        opctx: &OpContext,
        authz_tq: authz::TrustQuorumConfig,
    ) -> Result<(TrustQuorumConfig, Epoch), Error> {
        opctx.authorize(authz::Action::Read, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;
        let rack_id = authz_tq.rack().id();

        Self::tq_load_latest_possible_committed_config_conn(conn, rack_id)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    pub(super) async fn tq_load_latest_possible_committed_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<(TrustQuorumConfig, Epoch), TransactionError<Error>> {
        // First get the latest configuration for this rack.
        let Some(latest_config) =
            Self::tq_get_latest_config_with_members_conn(conn, rack_id).await?
        else {
            return Err(TransactionError::CustomError(Error::invalid_request(
                format!(
                    "Missing trust quorum configurations for rack {rack_id}. \
                    Upgrade to trust quorum required."
                ),
            )));
        };

        let highest_epoch = latest_config.epoch;

        // We assume this config is committed as long as it is not aborted.
        //
        // This assumption will be validated by the datastore code before
        // insertion of the new configuration.
        let latest_committed_config = if latest_config.state.is_aborted() {
            let Some(epoch) = latest_config.last_committed_epoch else {
                return Err(TransactionError::CustomError(
                    Error::invalid_request(format!(
                        "No committed trust quorum configuration for \
                        rack {rack_id}."
                    )),
                ));
            };

            // Load the configuration for the last commmitted epoch
            let Some(latest_committed_config) =
                Self::tq_get_config_with_members_from_epoch_conn(
                    conn, rack_id, epoch,
                )
                .await?
            else {
                return Err(TransactionError::CustomError(
                    Error::invalid_request(format!(
                        "Missing expected last committed trust quorum \
                        configuration for rack {rack_id}, epoch {epoch}."
                    )),
                ));
            };
            latest_committed_config
        } else {
            latest_config
        };

        Ok((latest_committed_config, highest_epoch))
    }

    /// Return the `BaseboardId` of every member that is `Committed` in the
    /// latest configuration for this rack but has no corresponding sled on this
    /// rack that is both present and not expunged.
    ///
    /// Returns an empty set if the rack has no trust quorum configuration.
    pub async fn tq_get_committed_members_without_active_sled(
        &self,
        opctx: &OpContext,
        authz_tq: authz::TrustQuorumConfig,
    ) -> Result<BTreeSet<BaseboardId>, Error> {
        opctx.authorize(authz::Action::Read, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;
        let rack_id = authz_tq.rack().id();

        Self::tq_get_committed_members_without_active_sled_conn(conn, rack_id)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    async fn tq_get_latest_config_with_members_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<Option<TrustQuorumConfig>, TransactionError<Error>> {
        // First, retrieve our configuration if there is one.
        let Some(latest) =
            Self::tq_get_latest_config_conn(conn, rack_id).await?
        else {
            return Ok(None);
        };

        // Then fill in our members
        Self::tq_get_config_with_members_conn(conn, rack_id, latest).await
    }

    async fn tq_get_config_with_members_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        config: DbTrustQuorumConfiguration,
    ) -> Result<Option<TrustQuorumConfig>, TransactionError<Error>> {
        // Then get any members associated with the configuration
        let members =
            Self::tq_get_members_conn(conn, rack_id, config.epoch).await?;

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
            if config.coordinator == hw_baseboard_id.id {
                coordinator = Some(hw_baseboard_id.clone().into());
            }
            tq_members.insert(
                hw_baseboard_id.into(),
                TrustQuorumMemberData {
                    state: member.state.into(),
                    share_digest: digest,
                    time_prepared: member.time_prepared,
                    time_committed: member.time_committed,
                },
            );
        }

        let salt = if let Some(salt_str) = config.encrypted_rack_secrets_salt {
            let mut data = [0u8; 32];
            hex::decode_to_slice(&salt_str, &mut data).map_err(|e| {
                Error::internal_error(&format!(
                    "Failed to decode salt for trust quorum config: \
                    rack_id: {}, epoch: {}: {e}",
                    config.rack_id, config.epoch
                ))
            })?;
            Some(Salt(data))
        } else {
            None
        };

        let encrypted_rack_secrets = if let Some(salt) = salt {
            let Some(secrets) = config.encrypted_rack_secrets else {
                // This should never happend due to constraint checks
                return Err(Error::internal_error(&format!(
                    "Salt exists, but secrets do not for trust quorum config: \
                    rack_id: {}, epoch: {}",
                    config.rack_id, config.epoch
                ))
                .into());
            };
            Some(EncryptedRackSecrets::new(salt, secrets.into_boxed_slice()))
        } else {
            None
        };

        let Some(coordinator) = coordinator else {
            return Err(Error::internal_error(&format!(
                "Failed to find coordinator for hw_baseboard_id: {} \
                in trust quorum config.",
                config.coordinator
            ))
            .into());
        };

        let last_committed_epoch =
            if let Some(epoch) = config.last_committed_epoch {
                Some(i64_to_epoch(epoch)?)
            } else {
                None
            };

        Ok(Some(TrustQuorumConfig {
            rack_id: config.rack_id.into(),
            epoch: i64_to_epoch(config.epoch)?,
            last_committed_epoch,
            state: config.state.into(),
            threshold: Threshold(config.threshold.into()),
            commit_crash_tolerance: config.commit_crash_tolerance.into(),
            coordinator,
            encrypted_rack_secrets,
            members: tq_members,
            time_created: config.time_created,
            time_committing: config.time_committing,
            time_committed: config.time_committed,
            time_aborted: config.time_aborted,
            abort_reason: config.abort_reason,
        }))
    }

    async fn tq_get_config_with_members_from_epoch_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<Option<TrustQuorumConfig>, TransactionError<Error>> {
        // First, retrieve our configuration if there is one.
        let Some(latest) =
            Self::tq_get_config_conn(conn, rack_id, epoch).await?
        else {
            return Ok(None);
        };

        // Then fill in our members
        Self::tq_get_config_with_members_conn(conn, rack_id, latest).await
    }

    /// Insert a new trust quorum configuration, if the proposed configuration
    /// validates.
    pub async fn tq_insert_latest_config(
        &self,
        opctx: &OpContext,
        proposed: ProposedTrustQuorumConfig,
    ) -> Result<(), Error> {
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(proposed.rack_id);
        opctx.authorize(authz::Action::Modify, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_insert_latest_config")
            .transaction(&conn, |c| {
                let err = err.clone();
                let authz_tq = authz_tq.clone();
                let proposed = proposed.clone();

                async move {
                    Self::tq_insert_latest_config_conn(&c, &authz_tq, proposed)
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

    /// The core logic for inserting a trust quorum config.
    ///
    /// Meant to be called inside a retryable transaction.
    pub(super) async fn tq_insert_latest_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_tq: &authz::TrustQuorumConfig,
        proposed: ProposedTrustQuorumConfig,
    ) -> Result<TrustQuorumConfig, TransactionError<Error>> {
        let rack_id = authz_tq.rack().id();
        if proposed.rack_id != rack_id {
            return Err(TransactionError::CustomError(Error::internal_error(
                &format!(
                    "trust quorum proposal is for rack {}, but the trust \
                     quorum config authorized is for rack {rack_id}",
                    proposed.rack_id,
                ),
            )));
        }

        let validated = Self::validate_new_config_conn(conn, proposed).await?;

        // Mark the old configuration as committed-partially
        if let Some(epoch) = validated.commit_partially_epoch {
            Self::updated_tq_state_committed_partially_conn(
                conn,
                validated.config.rack_id,
                epoch,
            )
            .await?;
        }

        // Save the new config
        Self::insert_tq_config_conn(conn, validated.config.clone()).await?;

        Ok(validated.config)
    }

    async fn validate_new_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        proposed: ProposedTrustQuorumConfig,
    ) -> Result<ValidatedConfig, TransactionError<Error>> {
        //
        // Check validity of members in proposed config
        //

        // Ensure all baseboards actually exist
        let hw_baseboard_ids = Self::lookup_hw_baseboard_ids_conn(
            conn,
            proposed.members.iter().cloned(),
        )
        .await?;

        bail_unless!(
            hw_baseboard_ids.len()
                == usize::from(proposed.members.num_members()),
            "Trust Quorum proposed configuration missing hw_baseboard_ids \
            in database. Do all physical sleds exist in this rack?"
        );

        // Check that if a sled is commissioned with a matching baseboard that
        // it's for this `rack_id`.
        for member in &proposed.members {
            if let Some(db_rack_id) =
                Self::sled_get_rack_id_if_commissioned_conn(conn, member)
                    .await?
            {
                bail_unless!(
                    proposed.rack_id == db_rack_id,
                    "Sled {} already commissioned for another rack. \
                    Expected rack_id {}, Got {}",
                    member,
                    proposed.rack_id,
                    db_rack_id
                );
            }
        }

        //
        // Check that there is not an in progress reconfiguration:
        //
        let latest_config = Self::tq_get_latest_config_with_members_conn(
            conn,
            proposed.rack_id,
        )
        .await?;

        let latest_epoch = latest_config.as_ref().map(|c| c.epoch);
        // Check for an outdated epoch.
        //
        // The proposed epoch is computed to be `latest + 1`, so another
        // reconfiguration could race with this one. In this case, the caller
        // should retry.
        if let Some(latest_epoch) = latest_epoch
            && proposed.epoch <= latest_epoch
        {
            return Err(Error::conflict(format!(
                "Trust quorum reconfiguration in progress: a configuration \
                at epoch {latest_epoch} was proposed concurrently with this \
                one (proposed epoch {}). Retry the request.",
                proposed.epoch
            ))
            .into());
        }

        // Ensure that epochs are sequential or this is the inital attempt at an
        // LRTQ upgrade.
        //
        // In the latter case the proposed epoch will be 2, as LRTQ has an epoch
        // of 1 that is encoded as a ZFS dataset property.
        bail_unless!(
            latest_epoch == proposed.epoch.previous()
                || (latest_epoch.is_none()
                    && proposed.is_lrtq_upgrade == IsLrtqUpgrade::Yes
                    && proposed.epoch == Epoch(2)),
            "Epochs for trust quorum configurations must be sequential. \
            Current epoch = {:?}, Proposed Epoch = {:?}",
            latest_epoch,
            proposed.epoch
        );

        // Helper variable to operate on the last committed config, no matter
        // how we find it.
        let mut last_committed_config: Option<TrustQuorumConfig> = None;

        // Is there actually a latest configuration?
        if let Some(latest_config) = latest_config {
            // If the configuration is being prepared then it must be aborted
            // before a reconfiguration can occur.
            if latest_config.state.is_preparing() {
                return Err(Error::conflict(format!(
                    "Trust quorum reconfiguration in progress: still \
                    preparing in epoch {}. Wait for it to finish and retry, \
                    or abort it to reconfigure now.",
                    latest_config.epoch
                ))
                .into());
            }

            // If the latest configuration is at least committing, then the
            // proposed configuration must have the last committed configuration
            // set to match this.
            if latest_config.state.is_committed()
                || latest_config.state.is_committing()
            {
                bail_unless!(
                    Some(latest_config.epoch)
                        == proposed.last_committed_epoch(),
                    "Trust Quorum proposed configuration does not match \
                    the last committed epoch. Committed configurations must \
                    build upon each other sequentially. Expected epoch \
                    {}, Got {:?}",
                    latest_config.epoch,
                    proposed.last_committed_epoch()
                );

                last_committed_config = Some(latest_config);
            } else if latest_config.state.is_aborted() {
                // In this case, the caller should retry.
                if proposed.last_committed_epoch() == Some(latest_config.epoch)
                {
                    return Err(Error::conflict(format!(
                        "Trust quorum reconfiguration in progress: the \
                        configuration at epoch {} that this one builds on was \
                        aborted concurrently. Retry the request.",
                        latest_config.epoch
                    ))
                    .into());
                }

                // Inductively,the latest configuration should not have
                // been written to the database unless its last committed
                // configuration was valid.
                bail_unless!(
                    latest_config.last_committed_epoch
                        == proposed.last_committed_epoch(),
                    "Trust quorum proposed configuration has a last committed \
                    epoch that does not match the last committed epoch of the \
                    latest (aborted) configuration. \
                    Proposed = {:?}, Actual = {:?}",
                    proposed.last_committed_epoch(),
                    latest_config.last_committed_epoch
                );
            }
        }

        // The latest config was aborted or there wasn't one
        if last_committed_config.is_none() {
            // Do we need to load the last committed config?
            if let Some(epoch) = proposed.last_committed_epoch() {
                let config = Self::tq_get_config_with_members_from_epoch_conn(
                    conn,
                    proposed.rack_id,
                    epoch,
                )
                .await?;

                bail_unless!(
                    config.is_some(),
                    "Trust Quorum proposed configuration contains a last \
                    committed epoch ({}) for a configuration that does \
                    not exist.",
                    epoch
                );

                last_committed_config = config;
            }
        }

        // The last committed epoch may not have received acked commits from
        // all nodes, but it may have received enough nodes to allow a new
        // configuration to be installed. If the latter is true then we must
        // mark the old configuration `CommittedPartially` in the DB.
        let mut commit_partially_epoch = None;

        // At this point we have either loaded the proposed last committed
        // configuration or there is not one.
        let possible_coordinators: BTreeSet<BaseboardId> =
            if let Some(last_committed_config) = last_committed_config {
                bail_unless!(
                    last_committed_config.state.is_committed()
                        || last_committed_config.state.is_committing(),
                    "Trust Quorum proposed configuration contains a last \
                committed epoch ({}) for a configuration that is not \
                committed",
                    last_committed_config.epoch
                );

                if last_committed_config.state.is_committing() {
                    let acked = last_committed_config.acked_commits();
                    // K + Z
                    let min = last_committed_config.threshold.0 as usize
                        + last_committed_config.commit_crash_tolerance as usize;
                    if acked < min {
                        return Err(Error::conflict(format!(
                            "Trust quorum reconfiguration in progress: not \
                            enough nodes have acked commit in epoch {}. \
                            Expected at least {} acks, Got {} acks. If this \
                            persists please contact Oxide support.",
                            last_committed_config.epoch, min, acked
                        ))
                        .into());
                    }
                    commit_partially_epoch = Some(last_committed_config.epoch);
                }

                // A coordinator must have acked a commit in the prior configuration
                // if it's to be a candidate.
                let committed_members: BTreeSet<BaseboardId> =
                    last_committed_config
                        .members
                        .iter()
                        .filter_map(|(id, data)| {
                            if data.state == TrustQuorumMemberState::Committed {
                                Some(id.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                proposed
                    .members
                    .as_set()
                    .intersection(&committed_members)
                    .cloned()
                    .collect()
            } else {
                proposed.members.as_set().clone()
            };

        bail_unless!(
            !possible_coordinators.is_empty(),
            "Trust Quorum proposed configuration has no possible coordinators \
            for epoch {}",
            proposed.epoch
        );

        // Pick a random coordinator from our set of possibilities, narrowed
        // to members with an in-service sled in this rack.
        let in_service_baseboards =
            Self::sled_list_in_service_baseboards_in_rack_conn(
                conn,
                proposed.rack_id,
            )
            .await?;
        let Some(coordinator) = possible_coordinators
            .intersection(&in_service_baseboards)
            .cloned()
            .choose(&mut rng())
        else {
            return Err(TransactionError::CustomError(
                no_in_service_coordinator_error(
                    &proposed,
                    &possible_coordinators,
                ),
            ));
        };

        // Convert proposal to a new `TrustQuorumConfig`
        Ok(ValidatedConfig {
            config: TrustQuorumConfig::new(proposed, coordinator),
            commit_partially_epoch,
        })
    }

    /// If this configuration is the latest one and is in the `Preparing` or
    /// `PreparingLrtqUpgrade` state, update any members to acknowledge the
    /// prepare.
    ///
    /// Also, update any digests or encrypted rack secrets if necessary. Lastly,
    /// if enough members have acked prepares, move the configuration to the
    /// `Committing` state, and return the configuration as written by this
    /// transaction.
    ///
    /// Returns an error if this configuration is not the latest one, or if it
    /// is no longer preparing.
    pub async fn tq_update_prepare_status(
        &self,
        opctx: &OpContext,
        config: trust_quorum_types::configuration::Configuration,
        acked_prepares: BTreeSet<BaseboardId>,
    ) -> Result<TqPrepareStatusOutcome, Error> {
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(config.rack_id);
        opctx.authorize(authz::Action::Modify, &authz_tq).await?;
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
                    let latest = Self::tq_get_latest_config_conn(&c, rack_id)
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
                             Latest epoch does not match. Proposed {}, Got {}",
                            epoch,
                            actual
                        );
                    }

                    // If we aren't preparing, then ignore this call. Multiple
                    // Nexuses race to completion and we don't want to worry
                    // about overwriting commits with prepares in the `state`
                    // field of each member.
                    if db_config.state !=
                        DbTrustQuorumConfigurationState::Preparing
                      && db_config.state
                        != DbTrustQuorumConfigurationState::PreparingLrtqUpgrade
                  {
                        let state = db_config.state;
                        bail_txn!(
                            err,
                            "Ignoring stale update of trust quorum prepare \
                            status. Expected state = preparing || \
                            preparing-lrtq-upgrade, Got {:?}",
                            state
                        );
                    }

                    // Then get any members associated with the configuration
                    let db_members =
                        Self::tq_get_members_conn(&c, rack_id, db_config.epoch)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let mut total_acks = 0;
                    for (mut member, hw_id) in db_members {
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
                            Self::update_tq_member_share_digest_conn(
                                &c,
                                member.clone(),
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }

                        // Set the state of this member
                        if acked_prepares.contains(&baseboard_id)
                            && member.state == DbTrustQuorumMemberState::Unacked
                        {
                            total_acks += 1;
                            Self::update_tq_member_state_prepared_conn(
                                &c,
                                member.clone(),
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }

                        if member.state == DbTrustQuorumMemberState::Prepared {
                            total_acks += 1;
                        }
                    }

                    // We only update the configuration in the database if:
                    //  1. This is the first time we have seen encrypted rack secrets
                    //  2. We are transitioning from preparing to committed state.

                    // Should we write secrets?
                    if let (None, Some(encrypted_rack_secrets)) = (
                        &db_config.encrypted_rack_secrets_salt,
                        config.encrypted_rack_secrets,
                    ) {
                        Self::update_tq_encrypted_rack_secrets_conn(
                            &c,
                            db_config.rack_id,
                            db_config.epoch,
                            encrypted_rack_secrets,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    }

                    // Do we have enough acks to commit?
                    if total_acks
                        >= (db_config.threshold.0
                            + db_config.commit_crash_tolerance.0)
                            as usize
                    {
                        Self::update_tq_state_committing_conn(
                            &c,
                            db_config.rack_id,
                            db_config.epoch,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                        let committing_config =
                            Self::tq_get_config_with_members_from_epoch_conn(
                                &c,
                                rack_id,
                                config.epoch,
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        let Some(committing_config) = committing_config else {
                            bail_txn!(
                                err,
                                "Trust quorum config for rack_id {} at epoch \
                                 {} disappeared after moving it to committing",
                                rack_id,
                                epoch
                            );
                        };
                        return Ok(TqPrepareStatusOutcome::Committing {
                            config: Box::new(committing_config),
                        });
                    }

                    Ok(TqPrepareStatusOutcome::StillPreparing)
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// If this configuration is in the `Committing` state, then update any
    /// members to acknowledge their commit acknowledgements.
    pub async fn tq_update_commit_status(
        &self,
        opctx: &OpContext,
        authz_tq: authz::TrustQuorumConfig,
        epoch: Epoch,
        acked_commits: BTreeSet<BaseboardId>,
    ) -> Result<TrustQuorumConfigState, Error> {
        opctx.authorize(authz::Action::Modify, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let rack_id = authz_tq.rack().id();
        let epoch = epoch_to_i64(epoch)?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_update_commit_status")
            .transaction(&conn, |c| {
                let err = err.clone();
                let acked_commits = acked_commits.clone();
                async move {
                    // First, retrieve our configuration if there is one.
                    let latest = Self::tq_get_latest_config_conn(&c, rack_id)
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
                    // configuration is not `Committing`.
                    if db_config.state
                        != DbTrustQuorumConfigurationState::Committing
                    {
                        let state = db_config.state;
                        bail_txn!(
                            err,
                            "Invalid update of trust quorum commit status. \
                            Expected `Committing`, got {:?}",
                            state
                        );
                    }

                    Self::update_tq_members_state_commit_conn(
                        &c,
                        rack_id.into(),
                        epoch,
                        acked_commits,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    // Then get any members associated with the configuration
                    let db_members =
                        Self::tq_get_members_conn(&c, rack_id, db_config.epoch)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    // If all members have acked their commits then mark the
                    // configuration as committed.
                    if db_members.iter().all(|(m, _)| {
                        m.state == DbTrustQuorumMemberState::Committed
                    }) {
                        Self::update_tq_state_committed_conn(
                            &c,
                            db_config.rack_id,
                            db_config.epoch,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                        return Ok(TrustQuorumConfigState::Committed);
                    }

                    Ok(TrustQuorumConfigState::Committing)
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// Abort the configuration for a trust quorum if `epoch` is the latest per
    /// `rack_id` and the configuration has not started committing.
    ///
    /// Returns the configuration as read at the end of the transaction. If the
    /// configuration has already been aborted, it is returned unchanged,
    /// whether or not it is still the latest configuration.
    ///
    /// Returns a 409 if the configuration is committing or later, and a 404 if
    /// there is no configuration at `epoch`.
    pub async fn tq_abort_config(
        &self,
        opctx: &OpContext,
        authz_tq: authz::TrustQuorumConfig,
        epoch: Epoch,
        abort_reason: String,
    ) -> Result<TrustQuorumConfig, Error> {
        opctx.authorize(authz::Action::Modify, &authz_tq).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let rack_id = authz_tq.rack().id();
        let db_epoch = epoch_to_i64(epoch)?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_abort_config")
            .transaction(&conn, |c| {
                let err = err.clone();
                let abort_reason = abort_reason.clone();
                async move {
                    let latest = Self::tq_get_latest_config_conn(&c, rack_id)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    let at_epoch = Self::tq_get_config_conn(&c, rack_id, epoch)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let action = classify_tq_abort(
                        rack_id,
                        db_epoch,
                        latest.map(|config| config.epoch),
                        at_epoch.map(|config| config.state),
                    )
                    .map_err(|error| err.bail(error.into()))?;
                    match action {
                        TqAbortAction::AlreadyAborted => {}
                        TqAbortAction::Abort => {
                            Self::update_tq_abort_state_conn(
                                &c,
                                rack_id.into(),
                                db_epoch,
                                abort_reason,
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }
                    }

                    let aborted_config =
                        Self::tq_get_config_with_members_from_epoch_conn(
                            &c, rack_id, epoch,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    let Some(aborted_config) = aborted_config else {
                        bail_txn!(
                            err,
                            "Trust quorum config for rack_id {} at epoch {} \
                             disappeared after it was aborted",
                            rack_id,
                            epoch
                        );
                    };
                    Ok(aborted_config)
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    // Unconditional insert
    async fn insert_tq_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        config: TrustQuorumConfig,
    ) -> Result<(), TransactionError<Error>> {
        let hw_baseboard_ids = Self::lookup_hw_baseboard_ids_conn(
            conn,
            config.members.keys().cloned(),
        )
        .await?;

        bail_unless!(
            hw_baseboard_ids.len() == config.members.len(),
            "Failed to find all hw_baseboard_ids for trust quorum members \
            of rack_id {}, epoch = {}",
            config.rack_id,
            config.epoch
        );

        let (salt, secrets) =
            config.encrypted_rack_secrets.map_or((None, None), |s| {
                (Some(hex::encode(s.salt.0)), Some(s.data.into()))
            });

        // Max of 32 members to search. We could use binary search if we sorted
        // the output with an `order_by` in the DB query, or speed up search
        // if converted to a map. Neither seems necessary for such a rare
        // operation.
        let coordinator_id = hw_baseboard_ids.iter().find(|m| {
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

        let last_committed_epoch =
            if let Some(lc_epoch) = config.last_committed_epoch {
                Some(
                    epoch_to_i64(lc_epoch)
                        .map_err(|e| TransactionError::from(e))?,
                )
            } else {
                None
            };

        // Insert the configuration
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        let rows_updated = diesel::insert_into(dsl::trust_quorum_configuration)
            .values(DbTrustQuorumConfiguration {
                rack_id: config.rack_id.into(),
                epoch,
                last_committed_epoch,
                state: config.state.into(),
                threshold: config.threshold.0.into(),
                commit_crash_tolerance: config.commit_crash_tolerance.into(),
                coordinator: coordinator_id,
                encrypted_rack_secrets_salt: salt,
                encrypted_rack_secrets: secrets,
                time_created: config.time_created,
                time_committing: config.time_committing,
                time_committed: config.time_committed,
                time_aborted: config.time_aborted,
                abort_reason: config.abort_reason.clone(),
            })
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to insert trust quorum config for rack_id {}, epoch {}",
            config.rack_id,
            epoch
        );

        // Insert the members
        let members: Vec<_> = hw_baseboard_ids
            .iter()
            .zip(config.members.values())
            .map(|(hw_baseboard_id, m)| DbTrustQuorumMember {
                rack_id: config.rack_id.into(),
                epoch,
                hw_baseboard_id: hw_baseboard_id.id,
                state: m.state.into(),
                share_digest: m.share_digest.map(|d| hex::encode(d.0)),
                time_prepared: m.time_prepared,
                time_committed: m.time_committed,
            })
            .collect();

        let num_members = members.len();

        use nexus_db_schema::schema::trust_quorum_member::dsl as members_dsl;
        let rows_updated =
            diesel::insert_into(members_dsl::trust_quorum_member)
                .values(members)
                .execute_async(conn)
                .await?;

        bail_unless!(
            rows_updated == num_members,
            "Failed to insert all trust quorum members for config for \
            rack_id {}, epoch {}",
            config.rack_id,
            epoch
        );

        Ok(())
    }

    async fn update_tq_members_state_commit_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        acked_commits: BTreeSet<BaseboardId>,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let hw_baseboard_ids: Vec<_> =
            Self::lookup_hw_baseboard_ids_conn(conn, acked_commits.into_iter())
                .await?
                .into_iter()
                .map(|hw| hw.id)
                .collect();

        let num_members = hw_baseboard_ids.len();

        let rows_updated = diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::hw_baseboard_id.eq_any(hw_baseboard_ids))
            .filter(dsl::share_digest.is_not_null())
            .set((
                dsl::state.eq(DbTrustQuorumMemberState::Committed),
                dsl::time_committed.eq(Some(now_db_precision())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == num_members,
            "Failed to set commit state for all acked trust quorum members for \
            config for rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn update_tq_member_share_digest_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        member: DbTrustQuorumMember,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(member.rack_id))
            .filter(dsl::epoch.eq(member.epoch))
            .filter(dsl::hw_baseboard_id.eq(member.hw_baseboard_id))
            .filter(dsl::share_digest.is_null())
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Unacked))
            .set(dsl::share_digest.eq(member.share_digest))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to set share digest for trust quorum member {} for \
            config for rack_id {}, epoch {}",
            member.hw_baseboard_id,
            member.rack_id,
            member.epoch
        );

        Ok(())
    }

    async fn update_tq_member_state_prepared_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        member: DbTrustQuorumMember,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(member.rack_id))
            .filter(dsl::epoch.eq(member.epoch))
            .filter(dsl::hw_baseboard_id.eq(member.hw_baseboard_id))
            .filter(dsl::share_digest.is_not_null())
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Unacked))
            .set((
                dsl::state.eq(DbTrustQuorumMemberState::Prepared),
                dsl::time_prepared.eq(Some(now_db_precision())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum member state to prepared for \
            member {}, rack_id {}, epoch {}",
            member.hw_baseboard_id,
            member.rack_id,
            member.epoch
        );

        Ok(())
    }

    async fn update_tq_encrypted_rack_secrets_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        encrypted_rack_secrets: EncryptedRackSecrets,
    ) -> Result<(), TransactionError<Error>> {
        let salt = Some(hex::encode(encrypted_rack_secrets.salt.0));
        let secrets: Option<Vec<u8>> = Some(encrypted_rack_secrets.data.into());

        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
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

        bail_unless!(
            rows_updated == 1,
            "Failed to set encrypted rack secrets in trust quorum config for \
            rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    /// Returns the number of rows update
    async fn update_tq_state_committing_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq_any(vec![
                DbTrustQuorumConfigurationState::Preparing,
                DbTrustQuorumConfigurationState::PreparingLrtqUpgrade,
            ]))
            .set((
                dsl::state.eq(DbTrustQuorumConfigurationState::Committing),
                dsl::time_committing.eq(Some(now_db_precision())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to committing for \
            rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    /// Returns the number of rows update
    async fn update_tq_state_committed_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq(DbTrustQuorumConfigurationState::Committing))
            .set((
                dsl::state.eq(DbTrustQuorumConfigurationState::Committed),
                dsl::time_committed.eq(Some(now_db_precision())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to committed for \
            rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn updated_tq_state_committed_partially_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let epoch =
            epoch_to_i64(epoch).map_err(|e| TransactionError::from(e))?;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq(DbTrustQuorumConfigurationState::Committing))
            .set((
                dsl::state
                    .eq(DbTrustQuorumConfigurationState::CommittedPartially),
                dsl::time_committed.eq(Some(now_db_precision())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to committed-partially \
            for rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn update_tq_abort_state_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        abort_reason: String,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq_any([
                DbTrustQuorumConfigurationState::Preparing,
                DbTrustQuorumConfigurationState::PreparingLrtqUpgrade,
            ]))
            .set((
                dsl::state.eq(DbTrustQuorumConfigurationState::Aborted),
                dsl::time_aborted.eq(Some(now_db_precision())),
                dsl::abort_reason.eq(Some(abort_reason)),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to aborted \
            for rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn lookup_hw_baseboard_ids_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        members: impl Iterator<Item = BaseboardId>,
    ) -> Result<Vec<HwBaseboardId>, TransactionError<Error>> {
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

    async fn tq_get_latest_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<Option<DbTrustQuorumConfiguration>, TransactionError<Error>>
    {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let latest = dsl::trust_quorum_configuration
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .order_by(dsl::epoch.desc())
            .first_async::<DbTrustQuorumConfiguration>(conn)
            .await
            .optional()
            .map_err(TransactionError::Database)?;

        Ok(latest)
    }

    async fn tq_get_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<Option<DbTrustQuorumConfiguration>, TransactionError<Error>>
    {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let epoch =
            epoch_to_i64(epoch).map_err(|e| TransactionError::from(e))?;

        let latest = dsl::trust_quorum_configuration
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .filter(dsl::epoch.eq(epoch))
            .get_result_async::<DbTrustQuorumConfiguration>(conn)
            .await
            .optional()
            .map_err(TransactionError::Database)?;

        Ok(latest)
    }

    async fn tq_get_members_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: i64,
    ) -> Result<
        Vec<(DbTrustQuorumMember, HwBaseboardId)>,
        TransactionError<Error>,
    > {
        use nexus_db_schema::schema::hw_baseboard_id::dsl as hw_baseboard_id_dsl;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let members = dsl::trust_quorum_member
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
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
            .map_err(TransactionError::Database)?;

        Ok(members)
    }

    async fn tq_get_committed_members_without_active_sled_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<BTreeSet<BaseboardId>, TransactionError<Error>> {
        use nexus_db_schema::schema::hw_baseboard_id::dsl as hw_dsl;
        use nexus_db_schema::schema::sled::dsl as sled_dsl;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let Some(latest) =
            Self::tq_get_latest_config_conn(conn, rack_id).await?
        else {
            return Ok(BTreeSet::new());
        };

        let rows: Vec<(String, String)> = dsl::trust_quorum_member
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .filter(dsl::epoch.eq(latest.epoch))
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Committed))
            .inner_join(
                hw_dsl::hw_baseboard_id.on(hw_dsl::id.eq(dsl::hw_baseboard_id)),
            )
            .filter(diesel::dsl::not(diesel::dsl::exists(
                sled_dsl::sled
                    .filter(sled_dsl::part_number.eq(hw_dsl::part_number))
                    .filter(sled_dsl::serial_number.eq(hw_dsl::serial_number))
                    .filter(sled_dsl::rack_id.eq(rack_id.into_untyped_uuid()))
                    .filter(sled_dsl::time_deleted.is_null())
                    .filter(
                        sled_dsl::sled_policy
                            .ne(to_db_sled_policy(SledPolicy::Expunged)),
                    ),
            )))
            .select((hw_dsl::part_number, hw_dsl::serial_number))
            .load_async(conn)
            .await
            .map_err(TransactionError::Database)?;

        Ok(rows
            .into_iter()
            .map(|(part_number, serial_number)| BaseboardId {
                part_number,
                serial_number,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::sled::SledTransition;
    use crate::db::datastore::sled::TransitionError;
    use crate::db::model::to_db_typed_uuid;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::sled_upsert_for_baseboard;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::result::Error as DieselError;
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::HwBaseboardId;
    use nexus_db_model::PhysicalDisk;
    use nexus_db_model::PhysicalDiskKind;
    use nexus_db_model::PhysicalDiskPolicy;
    use nexus_db_model::SledState;
    use nexus_types::external_api::sled::SledProvisionPolicy;
    use nexus_types::identity::Asset;
    use nexus_types::trust_quorum::{
        IsLrtqUpgrade, TrustQuorumConfigState, TrustQuorumMemberState,
    };
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::RackUuid;
    use omicron_uuid_kinds::SledUuid;
    use uuid::Uuid;

    fn make_authz_tq(rack_id: RackUuid) -> authz::TrustQuorumConfig {
        authz::TrustQuorumConfig::for_rack_id(rack_id)
    }

    async fn insert_hw_baseboard_ids(db: &TestDatabase) -> Vec<HwBaseboardId> {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::hw_baseboard_id::dsl;
        let mut hw_baseboard_ids: Vec<_> = (0..10)
            .map(|_| HwBaseboardId {
                id: Uuid::new_v4(),
                part_number: "test-part".to_string(),
                serial_number: Uuid::new_v4().to_string(),
            })
            .collect();

        // Sort like a `BaseboardId` would be sorted in a `BTreeSet`. This
        // allows us to use the coordinator as first entry in the set for our
        // tests.
        hw_baseboard_ids.sort_by(|a, b| {
            (&a.part_number, &a.serial_number)
                .cmp(&(&b.part_number, &b.serial_number))
        });

        diesel::insert_into(dsl::hw_baseboard_id)
            .values(hw_baseboard_ids.clone())
            .execute_async(&*conn)
            .await
            .unwrap();

        hw_baseboard_ids
    }

    /// Insert a sled whose baseboard matches `hw`.
    async fn insert_sled_for_baseboard(
        datastore: &DataStore,
        rack_id: RackUuid,
        hw: &HwBaseboardId,
    ) -> SledUuid {
        sled_upsert_for_baseboard(
            datastore,
            rack_id,
            &BaseboardId::from(hw.clone()),
        )
        .await
        .id()
    }

    async fn insert_sleds_for_baseboards(
        datastore: &DataStore,
        rack_id: RackUuid,
        hw_ids: &[HwBaseboardId],
    ) {
        for hw in hw_ids {
            insert_sled_for_baseboard(datastore, rack_id, hw).await;
        }
    }

    #[tokio::test]
    async fn test_tq_insert_latest_errors() {
        let logctx = test_setup_log("test_tq_insert_latest_errors");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let coordinator = members.first().unwrap().clone();

        // Insert an initial config
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // Last committed epoch is incorrect (should be 1)
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(4),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        let e1 = datastore
            .tq_insert_latest_config(opctx, bad_config)
            .await
            .unwrap_err();
        println!("{e1}");

        // Missing baseboard
        let mut bad_members = members.clone();
        bad_members.insert(BaseboardId {
            part_number: "bad".into(),
            serial_number: "value".into(),
        });
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(bad_members)
                .expect("test members are within the size bounds"),
        };
        let e2 = datastore
            .tq_insert_latest_config(opctx, bad_config)
            .await
            .unwrap_err();
        println!("{e2}");

        // Non-sequential epoch (should be 2)
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(3),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        let e3 = datastore
            .tq_insert_latest_config(opctx, bad_config)
            .await
            .unwrap_err();
        println!("{e3}");

        assert_ne!(e1, e2);
        assert_ne!(e1, e3);
        assert_ne!(e2, e3);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[track_caller]
    fn assert_conflict(result: Result<(), Error>, expected: &str) {
        match result {
            Err(Error::Conflict { message }) => {
                assert_eq!(message.external_message(), expected);
            }
            other => panic!("expected Conflict {expected:?}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_tq_insert_latest_config_conn_rejects_other_rack() {
        let logctx = test_setup_log(
            "test_tq_insert_latest_config_conn_rejects_other_rack",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let proposed_rack_id = RackUuid::new_v4();
        let authorized_rack_id = RackUuid::new_v4();
        let proposed = ProposedTrustQuorumConfig {
            rack_id: proposed_rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::Yes,
            members: TrustQuorumMembers::new(
                hw_ids.iter().cloned().map(BaseboardId::from).collect(),
            )
            .expect("test members are within the size bounds"),
        };

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        match DataStore::tq_insert_latest_config_conn(
            &conn,
            &make_authz_tq(authorized_rack_id),
            proposed,
        )
        .await
        {
            Err(TransactionError::CustomError(error)) => assert_eq!(
                error,
                Error::internal_error(&format!(
                    "trust quorum proposal is for rack {proposed_rack_id}, \
                     but the trust quorum config authorized is for rack \
                     {authorized_rack_id}"
                )),
            ),
            result => panic!("expected a rack mismatch error, got {result:?}"),
        }
        assert_eq!(
            datastore
                .tq_get_latest_config(
                    db.opctx(),
                    make_authz_tq(proposed_rack_id)
                )
                .await
                .expect("read the latest config"),
            None,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_insert_latest_conflicts_while_reconfiguring() {
        let logctx = test_setup_log(
            "test_tq_insert_latest_conflicts_while_reconfiguring",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids).await;
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            members.clone(),
            members.first().unwrap().clone(),
        )
        .await
        .unwrap();

        let proposal =
            |epoch: u64, last_committed_epoch: u64| ProposedTrustQuorumConfig {
                rack_id,
                epoch: Epoch(epoch),
                is_lrtq_upgrade: IsLrtqUpgrade::No {
                    last_committed_epoch: Epoch(last_committed_epoch),
                },
                members: TrustQuorumMembers::new(members.clone())
                    .expect("test members are within the size bounds"),
            };

        datastore.tq_insert_latest_config(opctx, proposal(2, 1)).await.unwrap();

        assert_conflict(
            datastore.tq_insert_latest_config(opctx, proposal(3, 1)).await,
            "Trust quorum reconfiguration in progress: still preparing in \
             epoch 2. Wait for it to finish and retry, or abort it to \
             reconfigure now.",
        );
        assert_conflict(
            datastore.tq_insert_latest_config(opctx, proposal(2, 1)).await,
            "Trust quorum reconfiguration in progress: a configuration at \
             epoch 2 was proposed concurrently with this one (proposed epoch \
             2). Retry the request.",
        );
        match datastore.tq_insert_latest_config(opctx, proposal(4, 1)).await {
            Err(Error::InternalError { internal_message }) => assert!(
                internal_message.starts_with(
                    "Epochs for trust quorum configurations must be \
                     sequential."
                ),
                "unexpected message {internal_message:?}",
            ),
            other => panic!("expected InternalError, got {other:?}"),
        }

        // If epoch 2 is aborted before the insert, that's a lost race too.
        datastore
            .tq_abort_config(
                opctx,
                make_authz_tq(rack_id),
                Epoch(2),
                "test".into(),
            )
            .await
            .unwrap();
        assert_conflict(
            datastore.tq_insert_latest_config(opctx, proposal(3, 2)).await,
            "Trust quorum reconfiguration in progress: the configuration at \
             epoch 2 that this one builds on was aborted concurrently. Retry \
             the request.",
        );
        datastore.tq_insert_latest_config(opctx, proposal(3, 1)).await.unwrap();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_insert_initial_lrtq_upgrade() {
        let logctx = test_setup_log("test_tq_insert_initial_lrtq_upgrade");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids).await;
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        // Propse a an LRTQ upgrade and successfully insert it
        let config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::Yes,
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };

        // Insert should succeed
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Read the config back and check that it's preparing for LRTQ upgrade
        // with no acks.
        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        // The read config should be preparing
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(
            read_config.state,
            TrustQuorumConfigState::PreparingLrtqUpgrade
        );
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(read_config.members.iter().all(|(_, info)| {
            info.state == TrustQuorumMemberState::Unacked
        }));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_insert_requires_in_service_coordinator() {
        let logctx =
            test_setup_log("test_tq_insert_requires_in_service_coordinator");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let committed_members: BTreeSet<_> =
            hw_ids[..5].iter().cloned().map(BaseboardId::from).collect();
        let proposed_members: BTreeSet<_> =
            hw_ids[..7].iter().cloned().map(BaseboardId::from).collect();

        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            committed_members.clone(),
            committed_members.first().unwrap().clone(),
        )
        .await
        .unwrap();

        // | baseboard  | candidate?          | sled row                     | can coordinate?                |
        // |------------|---------------------|------------------------------|--------------------------------|
        // | 0, 2, 3, 4 | yes                 | none                         | no                             |
        // | 1          | yes                 | expunged, state still active | no (expunged isn't in service) |
        // | 5, 6       | no (not in epoch 1) | in service                   | not eligible                   |
        let expunged =
            insert_sled_for_baseboard(datastore, rack_id, &hw_ids[1]).await;
        let (authz_sled, _) = LookupPath::new(opctx, datastore)
            .sled_id(expunged)
            .fetch_for(authz::Action::Modify)
            .await
            .expect("looked up sled");
        datastore
            .sled_set_policy_to_expunged(opctx, &authz_sled)
            .await
            .expect("expunged sled");
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids[5..7]).await;

        let proposal = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(proposed_members)
                .expect("test members are within the size bounds"),
        };
        assert_conflict(
            datastore.tq_insert_latest_config(opctx, proposal.clone()).await,
            &format!(
                "Cannot propose trust quorum configuration at epoch 2 for \
                 rack {rack_id}, since no sled can coordinate it. A \
                 coordinator must be a member of the proposed configuration \
                 that acknowledged the commit of epoch 1, with an in-service \
                 sled in this rack, and none of the candidates ({}) has one. \
                 If the rack was just initialized or these sleds were just \
                 added, wait for them to register with the control plane and \
                 retry. Otherwise, contact Oxide support.",
                // insert_hw_baseboard_ids returns the baseboards in sorted
                // order, so we don't need to sort them.
                hw_ids[..5]
                    .iter()
                    .map(|hw| format!(
                        "{}:{}",
                        hw.part_number, hw.serial_number
                    ))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        );
        let latest = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("read the latest config")
            .expect("a latest config exists");
        assert_eq!(latest.epoch, Epoch(1));
        assert_eq!(latest.state, TrustQuorumConfigState::Committed);

        // Now make baseboard 3 in-service. Then this is the only one that can
        // be selected as the coordinator.
        insert_sled_for_baseboard(datastore, rack_id, &hw_ids[3]).await;
        datastore
            .tq_insert_latest_config(opctx, proposal)
            .await
            .expect("inserted the proposal once a candidate is in service");
        let inserted = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("read the latest config")
            .expect("a latest config exists");
        assert_eq!(inserted.epoch, Epoch(2));
        assert_eq!(inserted.coordinator, BaseboardId::from(hw_ids[3].clone()));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_update_prepare_and_commit() {
        let logctx = test_setup_log("test_tq_update_prepare_and_commit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids).await;
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let coordinator = members.first().unwrap().clone();

        // Insert an initial config
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // Propse a second configuration and successfully insert it
        let config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Read the config back and check that it's preparing with no acks
        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        // The read config should be preparing
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Preparing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(read_config.members.iter().all(|(_, info)| {
            info.state == TrustQuorumMemberState::Unacked
        }));

        // Trying to insert a new config should fail since we are currently
        // preparing
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(3),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        assert_conflict(
            datastore.tq_insert_latest_config(opctx, bad_config).await,
            "Trust quorum reconfiguration in progress: still preparing in \
             epoch 2. Wait for it to finish and retry, or abort it to \
             reconfigure now.",
        );

        // Trying to ack commits should fail since we are currently preparing
        let acked_commits = members.clone();
        let e = datastore
            .tq_update_commit_status(
                opctx,
                make_authz_tq(rack_id),
                config.epoch,
                acked_commits,
            )
            .await
            .unwrap_err();
        println!("{e}");

        // A configuration returned from a coordinator is part of the trust
        // quorum protocol
        let coordinator_config =
            trust_quorum_types::configuration::Configuration {
                rack_id: config.rack_id,
                epoch: config.epoch,
                coordinator: hw_ids.first().unwrap().clone().into(),
                members: members
                    .clone()
                    .into_iter()
                    .map(|id| (id, Sha3_256Digest([0u8; 32])))
                    .collect(),
                threshold: TrustQuorumConfig::threshold(members.len() as u8),
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
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
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
            assert!(info.share_digest.is_some());
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
                    .take(coordinator_config.threshold.0 as usize)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
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
            coordinator_config.threshold.0 as usize,
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
        let acked_prepares = coordinator_config.threshold.0 as usize
            + read_config.commit_crash_tolerance as usize;

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
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        // We've acked enough nodes and should have written our status to the DB
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Committing);
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

        // Future prepare acks should fail because we have already started
        // committing.
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
                make_authz_tq(rack_id),
                config.epoch,
                coordinator_config.members.keys().cloned().collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(read_config.epoch, config.epoch);
        // Now that all nodes have committed, we should see the config state
        // change from `Committing` to Committed.
        assert_eq!(read_config.state, TrustQuorumConfigState::Committed);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(
            read_config.members.iter().all(
                |(_, info)| info.state == TrustQuorumMemberState::Committed
            )
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_update_prepare_and_commit_partially() {
        let logctx =
            test_setup_log("test_tq_update_prepare_and_commit_partially");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids).await;
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let coordinator = members.first().unwrap().clone();

        // Insert an initial config
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // Propse a second configuration and successfully insert it
        let config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // A configuration returned from a coordinator is part of the trust
        // quorum protocol
        let coordinator_config =
            trust_quorum_types::configuration::Configuration {
                rack_id: config.rack_id,
                epoch: config.epoch,
                coordinator: hw_ids.first().unwrap().clone().into(),
                members: members
                    .clone()
                    .into_iter()
                    .map(|id| (id, Sha3_256Digest([0u8; 32])))
                    .collect(),
                threshold: TrustQuorumConfig::threshold(members.len() as u8),
                encrypted_rack_secrets: None,
            };

        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        // Ack enough nodes to go to the committing state
        let min_acks = coordinator_config.threshold.0 as usize
            + read_config.commit_crash_tolerance as usize;

        let outcome = datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(min_acks)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(
            outcome,
            TqPrepareStatusOutcome::Committing {
                config: Box::new(read_config.clone())
            }
        );

        // We've acked enough nodes and should have written our status to the DB
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Committing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            min_acks,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Prepared
                )
                .count()
        );

        // Try to start a new configuration with not enough nodes committed
        //
        // Trying to insert a new config should fail since we haven't committed
        // enough nodes.
        let next_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(3),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(2),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        assert_conflict(
            datastore.tq_insert_latest_config(opctx, next_config.clone()).await,
            &format!(
                "Trust quorum reconfiguration in progress: not enough nodes \
                 have acked commit in epoch 2. Expected at least {min_acks} \
                 acks, Got {} acks. If this persists please contact Oxide \
                 support.",
                0
            ),
        );

        // Commit at K + Z - 1 nodes
        datastore
            .tq_update_commit_status(
                opctx,
                make_authz_tq(rack_id),
                config.epoch,
                coordinator_config
                    .members
                    .keys()
                    .take(min_acks - 1)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Committing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            min_acks - 1,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Committed
                )
                .count()
        );

        // Insert should fail since again, there is not enough nodes:
        //
        // We have acked K+Z - 1 and not K+Z
        assert_conflict(
            datastore.tq_insert_latest_config(opctx, next_config.clone()).await,
            &format!(
                "Trust quorum reconfiguration in progress: not enough nodes \
                 have acked commit in epoch 2. Expected at least {min_acks} \
                 acks, Got {} acks. If this persists please contact Oxide \
                 support.",
                min_acks - 1
            ),
        );

        // Commit at K + Z
        datastore
            .tq_update_commit_status(
                opctx,
                make_authz_tq(rack_id),
                config.epoch,
                coordinator_config
                    .members
                    .keys()
                    .take(min_acks)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(read_config.epoch, config.epoch);
        // Even though K+Z nodes have committed, we should still see the state
        // at `Committing`, since a new config has not yet been proposed.
        assert_eq!(read_config.state, TrustQuorumConfigState::Committing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            min_acks,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Committed
                )
                .count()
        );

        // Now create a new config. This should succeed and we should
        // see the state of the old config move from `Committing` to
        // `CommittedPartially`.
        let err = OptionalError::new();
        let inserted_config = datastore
            .transaction_retry_wrapper(
                "test_tq_update_prepare_and_commit_partially",
            )
            .transaction(&conn, |c| {
                let err = err.clone();
                let next_config = next_config.clone();
                async move {
                    DataStore::tq_insert_latest_config_conn(
                        &c,
                        &make_authz_tq(rack_id),
                        next_config,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
            .expect("inserted the epoch 3 config");

        let stored_config = datastore
            .tq_get_config(opctx, make_authz_tq(rack_id), next_config.epoch)
            .await
            .expect("no error")
            .expect("returned config");
        assert_eq!(inserted_config, stored_config);

        let read_config = datastore
            .tq_get_config(opctx, make_authz_tq(rack_id), Epoch(2))
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(
            read_config.state,
            TrustQuorumConfigState::CommittedPartially
        );
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            min_acks,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Committed
                )
                .count()
        );
        assert_eq!(
            read_config
                .members
                .get(&inserted_config.coordinator)
                .expect("epoch 3 coordinator is an epoch 2 member")
                .state,
            TrustQuorumMemberState::Committed,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[track_caller]
    fn assert_unconverted<T: std::fmt::Debug>(
        helper: &str,
        result: Result<T, TransactionError<Error>>,
    ) {
        match result {
            Err(TransactionError::Database(DieselError::DatabaseError(
                _,
                info,
            ))) => assert!(
                info.message().starts_with("current transaction is aborted"),
                "{helper}: unexpected database error {:?}",
                info.message(),
            ),
            other => panic!(
                "{helper}: expected TransactionError::Database, got {other:?}"
            ),
        }
    }

    async fn assert_read_helpers_unconverted(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) {
        assert_unconverted(
            "tq_get_latest_config_conn",
            DataStore::tq_get_latest_config_conn(conn, rack_id).await,
        );
        assert_unconverted(
            "tq_get_config_conn",
            DataStore::tq_get_config_conn(conn, rack_id, Epoch(1)).await,
        );
        assert_unconverted(
            "tq_get_members_conn",
            DataStore::tq_get_members_conn(conn, rack_id, 1).await,
        );
        assert_unconverted(
            "tq_get_committed_members_without_active_sled_conn",
            DataStore::tq_get_committed_members_without_active_sled_conn(
                conn, rack_id,
            )
            .await,
        );
    }

    #[tokio::test]
    async fn test_tq_read_helpers_return_database_errors_unconverted() {
        let logctx = test_setup_log(
            "test_tq_read_helpers_return_database_errors_unconverted",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let datastore = db.datastore();
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let rack_id = RackUuid::new_v4();

        // Run a transaction starting with a failed statement. This leaves the
        // transaction in an aborted state, so that all the read methods in
        // assert_read_helpers_unconverted fail to run. We then get to check
        // that the return values are always `TransactionError::Database` (which
        // is required for the retry wrapper to recognize retryable errors.)
        //
        // It would be ideal to test that retries actually occur, though that
        // would require some kind of test-only fault injection that we don't
        // currently have the setup for. So we rely on this next-best thing.
        datastore
            .transaction_non_retry_wrapper(
                "test_tq_read_helpers_return_database_errors_unconverted",
            )
            .transaction(&conn, |conn| async move {
                conn.batch_execute_async("SELECT 1 / 0")
                    .await
                    .expect_err("division by zero aborted the transaction");
                assert_read_helpers_unconverted(&conn, rack_id).await;
                Err::<(), _>(DieselError::RollbackTransaction)
            })
            .await
            .expect_err("rolled back the aborted transaction");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_classify_tq_abort() {
        let rack_id = RackUuid::new_v4();
        let not_found = |epoch: i64| {
            Err(Error::non_resourcetype_not_found(format!(
                "No trust quorum configuration exists at epoch {epoch} for \
                 rack {rack_id}."
            )))
        };
        let already = |epoch: i64, state: &str| {
            Err(Error::conflict(format!(
                "Cannot abort the trust quorum configuration at epoch {epoch} \
                 for rack {rack_id}: it is already {state} and can no longer \
                 be aborted."
            )))
        };

        assert_eq!(classify_tq_abort(rack_id, 5, None, None), not_found(5));
        assert_eq!(classify_tq_abort(rack_id, 7, Some(6), None), not_found(7));

        for state in [
            DbTrustQuorumConfigurationState::Preparing,
            DbTrustQuorumConfigurationState::PreparingLrtqUpgrade,
            DbTrustQuorumConfigurationState::Committing,
            DbTrustQuorumConfigurationState::Committed,
            DbTrustQuorumConfigurationState::CommittedPartially,
            DbTrustQuorumConfigurationState::Aborted,
        ] {
            let (expected_latest, expected_older) = match state {
                DbTrustQuorumConfigurationState::Preparing
                | DbTrustQuorumConfigurationState::PreparingLrtqUpgrade => (
                    Ok(TqAbortAction::Abort),
                    Err(Error::internal_error(&format!(
                        "Trust quorum configuration at epoch 5 for rack \
                         {rack_id} is still in state {state:?}, but the \
                         latest epoch is 6"
                    ))),
                ),
                DbTrustQuorumConfigurationState::Aborted => (
                    Ok(TqAbortAction::AlreadyAborted),
                    Ok(TqAbortAction::AlreadyAborted),
                ),
                DbTrustQuorumConfigurationState::Committing => {
                    (already(6, "committing"), already(5, "committing"))
                }
                DbTrustQuorumConfigurationState::Committed => {
                    (already(6, "committed"), already(5, "committed"))
                }
                DbTrustQuorumConfigurationState::CommittedPartially => (
                    already(6, "partially committed"),
                    already(5, "partially committed"),
                ),
            };
            assert_eq!(
                classify_tq_abort(rack_id, 6, Some(6), Some(state)),
                expected_latest,
                "latest in state {state:?}",
            );
            assert_eq!(
                classify_tq_abort(rack_id, 5, Some(6), Some(state)),
                expected_older,
                "older in state {state:?}",
            );
        }
    }

    #[tokio::test]
    async fn test_tq_abort() {
        let logctx = test_setup_log("test_tq_abort");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids).await;
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let coordinator = members.first().unwrap().clone();
        let abort = |epoch: u64| {
            datastore.tq_abort_config(
                opctx,
                make_authz_tq(rack_id),
                Epoch(epoch),
                "test".into(),
            )
        };

        // Insert an initial config
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // Propse a second configuration and successfully insert it
        let config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Aborting should succeed, since we haven't committed
        let aborted = abort(2).await.expect("aborted the latest configuration");
        assert_eq!(aborted.state, TrustQuorumConfigState::Aborted);
        assert_eq!(aborted.abort_reason.as_deref(), Some("test"));

        // Aborting is idempotent
        assert_eq!(
            abort(2).await.expect("aborting again is idempotent"),
            aborted
        );

        // Committing will fail to update any rows
        // (This is not directly callable from a public API).
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            DataStore::update_tq_state_committing_conn(
                &conn,
                config.rack_id.into(),
                config.epoch.0 as i64,
            )
            .await
            .unwrap_err();
        }

        // Create a config to simulate retrieving one from a coordinator
        let coordinator_config =
            trust_quorum_types::configuration::Configuration {
                rack_id: config.rack_id,
                epoch: config.epoch,
                coordinator: hw_ids.first().unwrap().clone().into(),
                members: members
                    .clone()
                    .into_iter()
                    .map(|id| (id, Sha3_256Digest([0u8; 32])))
                    .collect(),
                threshold: TrustQuorumConfig::threshold(members.len() as u8),
                encrypted_rack_secrets: None,
            };

        // This is how we actually try to trigger commit operations. This should
        // fail outright.
        let acked_prepares = (coordinator_config.threshold.0
            + TrustQuorumConfig::commit_crash_tolerance(members.len() as u8))
            as usize;
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
            .tq_get_latest_config(opctx, make_authz_tq(rack_id))
            .await
            .expect("no error")
            .expect("returned config");
        assert_eq!(read_config, aborted);

        // Create and insert another config
        let config2 = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(3),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(members.clone())
                .expect("test members are within the size bounds"),
        };
        datastore
            .tq_insert_latest_config(opctx, config2.clone())
            .await
            .unwrap();

        // The old config is already aborted, so aborting it again succeeds
        // idempotently.
        assert_eq!(
            abort(2).await.expect(
                "aborting an older, already aborted epoch is idempotent"
            ),
            aborted
        );

        // Commit the new config
        let coordinator_config2 =
            trust_quorum_types::configuration::Configuration {
                epoch: config2.epoch,
                ..coordinator_config
            };
        let acked_prepares = (coordinator_config.threshold.0
            + TrustQuorumConfig::commit_crash_tolerance(members.len() as u8))
            as usize;
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
        assert_eq!(
            abort(3).await.unwrap_err(),
            Error::conflict(format!(
                "Cannot abort the trust quorum configuration at epoch 3 for \
                 rack {rack_id}: it is already committing and can no longer be \
                 aborted."
            )),
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_get_all_active_rack_id_and_latest_epoch() {
        let logctx =
            test_setup_log("test_get_all_active_rack_id_and_latest_epoch");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let rack_id1 = RackUuid::new_v4();
        let rack_id2 = RackUuid::new_v4();
        let rack_id3 = RackUuid::new_v4();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Save the membership for rack_id 2
        let mut rack2_members: BTreeSet<BaseboardId> = BTreeSet::new();

        // Create an initial config for 3 diff racks
        for rack_id in [rack_id1, rack_id2, rack_id3] {
            let hw_ids = insert_hw_baseboard_ids(&db).await;
            insert_sleds_for_baseboards(datastore, rack_id, &hw_ids).await;
            let members: BTreeSet<_> =
                hw_ids.iter().cloned().map(BaseboardId::from).collect();
            if rack_id == rack_id2 {
                rack2_members = members.clone();
            }
            let coordinator = members.first().unwrap().clone();
            DataStore::tq_insert_rss_config_after_handoff(
                opctx,
                &conn,
                make_authz_tq(rack_id),
                members.clone(),
                coordinator,
            )
            .await
            .unwrap();
        }

        // Create a second rack config for rack 2
        let config = ProposedTrustQuorumConfig {
            rack_id: rack_id2,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: TrustQuorumMembers::new(rack2_members)
                .expect("test members are within the size bounds"),
        };

        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Retreive the latest epochs per rack_id
        let values = datastore
            .tq_get_all_active_rack_id_and_latest_epoch(opctx)
            .await
            .unwrap();

        // We should only have retrieved the configuration for rack 2 that did
        // not commit or abort yet.
        assert_eq!(values.len(), 1);

        // The epoch should be the latest that exists
        assert_eq!(values[&rack_id2], Epoch(2));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_get_committed_members_without_active_sled() {
        let logctx =
            test_setup_log("test_tq_get_committed_members_without_active_sled");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        use nexus_db_schema::schema::sled::dsl as sled_dsl;

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();
        let coordinator = members.first().unwrap().clone();

        let missing_members = || async {
            datastore
                .tq_get_committed_members_without_active_sled(
                    opctx,
                    make_authz_tq(rack_id),
                )
                .await
                .unwrap()
        };

        // A rack with no configuration at all reports nothing.
        assert!(missing_members().await.is_empty());

        // This inserts a configuration in which every member is `Committed`.
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // No sleds exist yet, so every committed member is missing one.
        assert_eq!(missing_members().await, members);

        // Give the first 8 baseboards a sled.
        let mut sled_ids = Vec::new();
        for hw in &hw_ids[..8] {
            sled_ids
                .push(insert_sled_for_baseboard(datastore, rack_id, hw).await);
        }

        let mut expected: BTreeSet<_> =
            hw_ids[8..].iter().cloned().map(BaseboardId::from).collect();
        assert_eq!(missing_members().await, expected);

        // Expunging a sled makes its member missing again.
        diesel::update(sled_dsl::sled)
            .filter(sled_dsl::id.eq(sled_ids[0].into_untyped_uuid()))
            .set(
                sled_dsl::sled_policy
                    .eq(to_db_sled_policy(SledPolicy::Expunged)),
            )
            .execute_async(&*conn)
            .await
            .unwrap();
        expected.insert(hw_ids[0].clone().into());
        assert_eq!(missing_members().await, expected);

        // So does soft-deleting one.
        diesel::update(sled_dsl::sled)
            .filter(sled_dsl::id.eq(sled_ids[1].into_untyped_uuid()))
            .set(sled_dsl::time_deleted.eq(Some(now_db_precision())))
            .execute_async(&*conn)
            .await
            .unwrap();
        expected.insert(hw_ids[1].clone().into());
        assert_eq!(missing_members().await, expected);

        // A sled for the right baseboard but on a different rack does not
        // count as present.
        let sled9 = insert_sled_for_baseboard(
            datastore,
            RackUuid::new_v4(),
            &hw_ids[9],
        )
        .await;
        assert_eq!(missing_members().await, expected);

        // Prepare for next test: reset sleds 0 and 1 to their prior states
        diesel::update(sled_dsl::sled)
            .filter(sled_dsl::id.eq(sled_ids[0].into_untyped_uuid()))
            .set(sled_dsl::sled_policy.eq(to_db_sled_policy(
                SledPolicy::InService {
                    provision_policy: SledProvisionPolicy::Provisionable,
                },
            )))
            .execute_async(&*conn)
            .await
            .unwrap();
        diesel::update(sled_dsl::sled)
            .filter(sled_dsl::id.eq(sled_ids[1].into_untyped_uuid()))
            .set(
                sled_dsl::time_deleted
                    .eq(Option::<chrono::DateTime<chrono::Utc>>::None),
            )
            .execute_async(&*conn)
            .await
            .unwrap();

        // Prepare for next test: decommission sled 9
        diesel::update(sled_dsl::sled)
            .filter(sled_dsl::id.eq(sled9.into_untyped_uuid()))
            .set(sled_dsl::sled_state.eq(SledState::Decommissioned))
            .execute_async(&*conn)
            .await
            .unwrap();

        // All sleds having sled rows means missing_members() is empty
        for hw in &hw_ids[8..] {
            sled_ids
                .push(insert_sled_for_baseboard(datastore, rack_id, hw).await);
        }
        let expected = BTreeSet::new();
        assert_eq!(missing_members().await, expected);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_sled_set_policy_to_expunged_if_trust_quorum_allows() {
        let logctx = test_setup_log(
            "test_sled_set_policy_to_expunged_if_trust_quorum_allows",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let non_member_hw = &hw_ids[0];
        let member_hw = &hw_ids[1];
        let members: BTreeSet<_> =
            hw_ids[1..].iter().cloned().map(BaseboardId::from).collect();
        let coordinator = members.first().unwrap().clone();

        let non_member_sled =
            insert_sled_for_baseboard(datastore, rack_id, non_member_hw).await;
        let member_sled =
            insert_sled_for_baseboard(datastore, rack_id, member_hw).await;
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids[2..]).await;

        let insert_disk = |sled_id: SledUuid| async move {
            let disk = PhysicalDisk::from_parts(
                PhysicalDiskUuid::new_v4(),
                "vendor".to_string(),
                format!("serial-{sled_id}"),
                "model".to_string(),
                PhysicalDiskKind::U2,
                sled_id,
            );
            let disk_id = disk.id();
            datastore
                .physical_disk_insert(opctx, disk)
                .await
                .expect("inserted physical disk");
            disk_id
        };
        let non_member_disk = insert_disk(non_member_sled).await;
        let member_disk = insert_disk(member_sled).await;

        let expunge = |sled_id: SledUuid, hw: HwBaseboardId| async move {
            let (authz_sled, _) = LookupPath::new(opctx, datastore)
                .sled_id(sled_id)
                .fetch_for(authz::Action::Modify)
                .await
                .expect("looked up sled");
            datastore
                .sled_set_policy_to_expunged_if_trust_quorum_allows(
                    opctx,
                    &authz_sled,
                    &make_authz_tq(rack_id),
                    &BaseboardId::from(hw),
                )
                .await
        };
        let policy = |sled_id: SledUuid| async move {
            let (_, sled) = LookupPath::new(opctx, datastore)
                .sled_id(sled_id)
                .fetch()
                .await
                .expect("looked up sled");
            sled.policy()
        };
        let disk_policy = |disk_id: PhysicalDiskUuid| async move {
            use nexus_db_schema::schema::physical_disk::dsl;
            dsl::physical_disk
                .filter(dsl::id.eq(to_db_typed_uuid(disk_id)))
                .select(PhysicalDisk::as_select())
                .get_result_async(
                    &*datastore.pool_connection_for_tests().await.unwrap(),
                )
                .await
                .expect("looked up physical disk")
                .disk_policy
        };
        let in_service = SledPolicy::InService {
            provision_policy: SledProvisionPolicy::Provisionable,
        };

        match expunge(non_member_sled, non_member_hw.clone()).await {
            Err(Error::InvalidRequest { message }) => {
                assert_eq!(
                    message.external_message(),
                    format!(
                        "Missing trust quorum configurations for rack \
                         {rack_id}. Upgrade to trust quorum required."
                    ),
                );
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
        assert_eq!(policy(non_member_sled).await, in_service);
        assert_eq!(
            disk_policy(non_member_disk).await,
            PhysicalDiskPolicy::InService
        );

        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        let assert_member_conflict =
            |result: Result<SledPolicy, Error>, epoch: Epoch| {
                let member_baseboard = BaseboardId::from(member_hw.clone());
                match result {
                    Err(Error::Conflict { message }) => {
                        assert_eq!(
                            message.external_message(),
                            format!(
                                "Cannot expunge sled {member_sled}, as its \
                                 baseboard {member_baseboard} is still a \
                                 member of the latest trust quorum \
                                 configuration for rack {rack_id} (epoch \
                                 {epoch}, state Committed)"
                            ),
                        );
                    }
                    other => panic!("expected Conflict, got {other:?}"),
                }
            };

        assert_member_conflict(
            expunge(member_sled, member_hw.clone()).await,
            Epoch(1),
        );
        assert_eq!(policy(member_sled).await, in_service);
        assert_eq!(
            disk_policy(member_disk).await,
            PhysicalDiskPolicy::InService
        );

        assert_eq!(
            expunge(non_member_sled, non_member_hw.clone()).await,
            Ok(in_service),
        );
        assert_eq!(policy(non_member_sled).await, SledPolicy::Expunged);
        assert_eq!(
            disk_policy(non_member_disk).await,
            PhysicalDiskPolicy::Expunged
        );
        assert_eq!(
            expunge(non_member_sled, non_member_hw.clone()).await,
            Ok(SledPolicy::Expunged),
        );

        let (authz_non_member_sled, _) = LookupPath::new(opctx, datastore)
            .sled_id(non_member_sled)
            .fetch_for(authz::Action::Modify)
            .await
            .expect("looked up sled");
        datastore
            .sled_set_state_to_decommissioned(opctx, &authz_non_member_sled)
            .await
            .expect("decommissioned sled");
        let (_, decommissioned) = LookupPath::new(opctx, datastore)
            .sled_id(non_member_sled)
            .fetch()
            .await
            .expect("looked up sled");
        let expected_message = TransitionError::InvalidTransition {
            current: decommissioned,
            transition: SledTransition::Policy(SledPolicy::Expunged),
        }
        .to_string();
        match expunge(non_member_sled, non_member_hw.clone()).await {
            Err(Error::Conflict { message }) => {
                assert_eq!(message.external_message(), expected_message);
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        let without_member: BTreeSet<_> =
            hw_ids[2..].iter().cloned().map(BaseboardId::from).collect();
        datastore
            .tq_insert_latest_config(
                opctx,
                ProposedTrustQuorumConfig {
                    rack_id,
                    epoch: Epoch(2),
                    is_lrtq_upgrade: IsLrtqUpgrade::No {
                        last_committed_epoch: Epoch(1),
                    },
                    members: TrustQuorumMembers::new(without_member)
                        .expect("test members are within the size bounds"),
                },
            )
            .await
            .unwrap();
        datastore
            .tq_abort_config(
                opctx,
                make_authz_tq(rack_id),
                Epoch(2),
                "test".into(),
            )
            .await
            .unwrap();

        assert_member_conflict(
            expunge(member_sled, member_hw.clone()).await,
            Epoch(1),
        );
        assert_eq!(policy(member_sled).await, in_service);
        assert_eq!(
            disk_policy(member_disk).await,
            PhysicalDiskPolicy::InService
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_re_expunge_skips_trust_quorum_check() {
        let logctx = test_setup_log("test_re_expunge_skips_trust_quorum_check");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let sled_hw = &hw_ids[0];
        let others: BTreeSet<_> =
            hw_ids[1..].iter().cloned().map(BaseboardId::from).collect();
        let sled_id =
            insert_sled_for_baseboard(datastore, rack_id, sled_hw).await;
        insert_sleds_for_baseboards(datastore, rack_id, &hw_ids[1..]).await;

        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            make_authz_tq(rack_id),
            others.clone(),
            others.first().unwrap().clone(),
        )
        .await
        .unwrap();

        let (authz_sled, _) = LookupPath::new(opctx, datastore)
            .sled_id(sled_id)
            .fetch_for(authz::Action::Modify)
            .await
            .expect("looked up sled");
        let authz_tq = make_authz_tq(rack_id);
        let sled_baseboard = BaseboardId::from(sled_hw.clone());
        let expunge = || {
            datastore.sled_set_policy_to_expunged_if_trust_quorum_allows(
                opctx,
                &authz_sled,
                &authz_tq,
                &sled_baseboard,
            )
        };

        assert_eq!(
            expunge().await,
            Ok(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::Provisionable,
            }),
        );

        // Simulate re-adoption adding the expunged sled's baseboard back, so
        // the latest trust quorum config now contains it. Ensure that expunge()
        // returns Ok(SledPolicy::Expunged) (and not an error or something else
        // that's weird).
        let with_sled: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();
        datastore
            .tq_insert_latest_config(
                opctx,
                ProposedTrustQuorumConfig {
                    rack_id,
                    epoch: Epoch(2),
                    is_lrtq_upgrade: IsLrtqUpgrade::No {
                        last_committed_epoch: Epoch(1),
                    },
                    members: TrustQuorumMembers::new(with_sled)
                        .expect("test members are within the size bounds"),
                },
            )
            .await
            .unwrap();

        assert_eq!(expunge().await, Ok(SledPolicy::Expunged));

        // Simulate a decommissioned sled whose baseboard is back in the trust
        // quorum. We expect the return value to be the policy transition error,
        // not a trust quorum membership conflict, because the trust quorum
        // check is skipped.
        datastore
            .sled_set_state_to_decommissioned(opctx, &authz_sled)
            .await
            .expect("decommissioned sled");
        let (_, decommissioned) = LookupPath::new(opctx, datastore)
            .sled_id(sled_id)
            .fetch()
            .await
            .expect("looked up sled");
        let expected_message = TransitionError::InvalidTransition {
            current: decommissioned,
            transition: SledTransition::Policy(SledPolicy::Expunged),
        }
        .to_string();
        match expunge().await {
            Err(Error::Conflict { message }) => {
                assert_eq!(message.external_message(), expected_message);
            }
            other => panic!("expected Conflict, got {other:?}"),
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
