// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus APIs for trust quorum

use nexus_auth::context::OpContext;
use nexus_types::{
    external_api::params::UninitializedSledId,
    trust_quorum::{IsLrtqUpgrade, ProposedTrustQuorumConfig},
};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::RackUuid;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;

impl super::Nexus {
    /// Add a set of sleds to the trust quorum
    ///
    /// This will trigger a trust quorum reconfiguration. Background task(s)
    /// will then proceed to allocate the rack subnet and start sled agent once
    /// the new sled has acknowledged the trust quorum commit.
    pub(crate) async fn tq_add_sleds(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        new_sleds: BTreeSet<UninitializedSledId>,
    ) -> Result<(), Error> {
        // First get the latest configuration for this rack.
        let Some(latest_config) =
            self.db_datastore.tq_get_latest_config(opctx, rack_id).await?
        else {
            return Err(Error::invalid_request(format!(
                "Missing trust quorum configurations for rack {rack_id}. \
                Upgrade to trust quorum required."
            )));
        };

        let highest_epoch = latest_config.epoch;

        // We assume this config is committed whether it is or not.
        //
        // This assumption will be validated by the datastore code before
        // insertion of the new configuration.
        let latest_committed_config = if latest_config.state.is_aborted() {
            let Some(epoch) = latest_config.last_committed_epoch else {
                return Err(Error::invalid_request(format!(
                    "No committed trust quorum configuration for \
                    rack {rack_id}."
                )));
            };

            // Load the configuration for the last commmitted epoch
            let Some(latest_committed_config) =
                self.db_datastore.tq_get_config(opctx, rack_id, epoch).await?
            else {
                return Err(Error::invalid_request(format!(
                    "Missing expected last committed trust quorum \
                    configuration for rack {rack_id}, epoch {epoch}."
                )));
            };
            latest_committed_config
        } else {
            latest_config
        };

        let new_sleds: BTreeSet<BaseboardId> =
            new_sleds.into_iter().map(Into::into).collect();
        let existing: BTreeSet<_> =
            latest_committed_config.members.keys().cloned().collect();

        let intersection: BTreeSet<_> =
            existing.intersection(&new_sleds).collect();
        if !intersection.is_empty() {
            return Err(Error::invalid_request(format!(
                "The following sleds are already members of the trust quorum: \
                 {intersection:?}. Is there a problem with their sled agents?"
            )));
        }

        let proposed = ProposedTrustQuorumConfig {
            rack_id,
            epoch: highest_epoch.next(),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: latest_committed_config.epoch,
            },
            members: new_sleds.union(&existing).cloned().collect(),
        };

        self.db_datastore.tq_insert_latest_config(opctx, proposed).await
    }
}
