// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::authz;
use crate::context::OpContext;
use crate::db::DataStore;
use crate::db::datastore::TqPrepareStatusOutcome;
use nexus_types::trust_quorum::IsLrtqUpgrade;
use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
use nexus_types::trust_quorum::TrustQuorumConfig;
use nexus_types::trust_quorum::TrustQuorumConfigState;
use omicron_uuid_kinds::RackUuid;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;
use trust_quorum_types::configuration::Configuration;
use trust_quorum_types::crypto::Sha3_256Digest;
use trust_quorum_types::membership::TrustQuorumMembers;
use trust_quorum_types::types::Epoch;

/// A helper to drive a trust quorum reconfiguration state machine.
///
/// This simulates the trust quorum protocol from the perspective of:
///
/// * The Nexus trust quorum background task.
/// * The Sled Agent instances that the task talks to.
///
/// This panics on unexpected state transitions.
pub struct TrustQuorumConfigDriver<'a> {
    pub opctx: &'a OpContext,
    pub datastore: &'a DataStore,
    pub rack_id: RackUuid,
}

impl TrustQuorumConfigDriver<'_> {
    pub fn authz_tq(&self) -> authz::TrustQuorumConfig {
        authz::TrustQuorumConfig::for_rack_id(self.rack_id)
    }

    /// Retrieves the trust quorum config for the given epoch.
    pub async fn config(&self, epoch: u64) -> TrustQuorumConfig {
        self.datastore
            .tq_get_config(self.opctx, self.authz_tq(), Epoch(epoch))
            .await
            .expect("loaded trust quorum config")
            .unwrap_or_else(|| {
                panic!("trust quorum config at epoch {epoch} exists")
            })
    }

    /// Proposes a new trust quorum config with the given epoch and members.
    pub async fn propose(
        &self,
        epoch: u64,
        last_committed_epoch: u64,
        members: &BTreeSet<BaseboardId>,
    ) {
        self.datastore
            .tq_insert_latest_config(
                self.opctx,
                ProposedTrustQuorumConfig {
                    rack_id: self.rack_id,
                    epoch: Epoch(epoch),
                    is_lrtq_upgrade: IsLrtqUpgrade::No {
                        last_committed_epoch: Epoch(last_committed_epoch),
                    },
                    members: TrustQuorumMembers::new(members.clone())
                        .expect("test members are within the size bounds"),
                },
            )
            .await
            .unwrap_or_else(|error| {
                panic!("inserted trust quorum config at epoch {epoch}: {error}")
            });
        assert_eq!(
            self.config(epoch).await.state,
            TrustQuorumConfigState::Preparing
        );
    }

    /// Simulates Sled Agent instances acknowledging all prepares for the given
    /// epoch.
    ///
    /// As part of this simulation, this fills in all-zero share digests.
    pub async fn ack_all_prepares(&self, epoch: u64) {
        let config = self.config(epoch).await;
        let members: BTreeSet<_> = config.members.keys().cloned().collect();
        let coordinator_config = Configuration {
            rack_id: config.rack_id,
            epoch: config.epoch,
            coordinator: config.coordinator.clone(),
            members: members
                .iter()
                .cloned()
                .map(|id| (id, Sha3_256Digest([0u8; 32])))
                .collect(),
            threshold: config.threshold,
            encrypted_rack_secrets: None,
        };
        let outcome = self
            .datastore
            .tq_update_prepare_status(self.opctx, coordinator_config, members)
            .await
            .unwrap_or_else(|error| {
                panic!("acked prepares at epoch {epoch}: {error}")
            });
        let TqPrepareStatusOutcome::Committing { config: _ } = outcome else {
            panic!(
                "acked prepares at epoch {epoch} committing, got {outcome:?}"
            );
        };
    }

    /// Simulates Sled Agent instances acknowledging the specified commits for
    /// the given epoch.
    pub async fn ack_commits(
        &self,
        epoch: u64,
        acked: BTreeSet<BaseboardId>,
        expected_state: TrustQuorumConfigState,
    ) {
        let state = self
            .datastore
            .tq_update_commit_status(
                self.opctx,
                self.authz_tq(),
                Epoch(epoch),
                acked,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("acked commits at epoch {epoch}: {error}")
            });
        assert_eq!(state, expected_state);
    }

    /// Simulates Sled Agent instances acknowledging all commits for the given
    /// epoch.
    pub async fn ack_all_commits(&self, epoch: u64) {
        let members: BTreeSet<_> =
            self.config(epoch).await.members.into_keys().collect();
        self.ack_commits(epoch, members, TrustQuorumConfigState::Committed)
            .await;
    }

    /// Simulates a commit for the given epoch.
    pub async fn commit(&self, epoch: u64) {
        self.ack_all_prepares(epoch).await;
        self.ack_all_commits(epoch).await;
    }

    /// Simulates an abort for the given epoch.
    pub async fn abort(&self, epoch: u64) {
        let aborted = self
            .datastore
            .tq_abort_config(
                self.opctx,
                self.authz_tq(),
                Epoch(epoch),
                "aborted by test".to_string(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("aborted trust quorum config at epoch {epoch}: {error}")
            });
        assert_eq!(aborted.state, TrustQuorumConfigState::Aborted);
    }
}
