// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus APIs for trust quorum

use nexus_auth::context::OpContext;
use nexus_types::trust_quorum::{
    IsLrtqUpgrade, ProposedTrustQuorumConfig, TrustQuorumConfig,
};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, RackUuid, SledUuid};
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;
use std::time::Duration;
use trust_quorum_types::types::Epoch;

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
        new_sleds: BTreeSet<BaseboardId>,
    ) -> Result<TrustQuorumConfig, Error> {
        let (latest_committed_config, latest_epoch) = self
            .tq_load_latest_possible_committed_config(opctx, rack_id)
            .await?;
        let new_epoch = latest_epoch.next();
        let proposed = self
            .add_sleds_proposed_config(
                latest_committed_config,
                new_epoch,
                new_sleds,
            )
            .await?;
        self.db_datastore.tq_insert_latest_config(opctx, proposed).await?;

        // Read back the real configuration from the database. Importantly this
        // includes a chosen coordinator.
        let Some(new_config) =
            self.db_datastore.tq_get_config(opctx, rack_id, new_epoch).await?
        else {
            return Err(Error::internal_error(&format!(
                "Cannot retrieve newly inserted trust quorum \
                    configuration for rack {rack_id}, epoch {new_epoch}."
            )));
        };

        // Retrieve the sled for the coordinator
        let client = self
            .get_coordinator_client(
                opctx,
                rack_id,
                new_epoch,
                &new_config.coordinator,
            )
            .await?;

        // Now send the reconfiguration request to the coordinator. We do
        // this directly in the API handler because this is a non-idempotent
        // operation and we only want to issue it once.
        let req = trust_quorum_types::messages::ReconfigureMsg {
            rack_id: new_config.rack_id,
            epoch: new_config.epoch,
            last_committed_epoch: new_config.last_committed_epoch,
            members: new_config.members.keys().cloned().collect(),
            threshold: new_config.threshold,
        };
        client.trust_quorum_reconfigure(&req).await?;

        Ok(new_config)
    }

    /// Remove a sled from the trust quorum
    ///
    /// This will trigger a trust quorum reconfiguration. This is a required
    /// first step towards expunging a sled.
    ///
    /// Returns the epoch of the proposed configuration so it can be polled
    /// asynchronously.
    pub(crate) async fn tq_remove_sled(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<Epoch, Error> {
        // Look up the sled to get its rack_id and baseboard_id
        let (.., sled) = self.sled_lookup(opctx, &sled_id)?.fetch().await?;
        let rack_id = RackUuid::from_untyped_uuid(sled.rack_id);
        let sled_to_remove = BaseboardId {
            part_number: sled.part_number().to_string(),
            serial_number: sled.serial_number().to_string(),
        };

        let (latest_committed_config, latest_epoch) = self
            .tq_load_latest_possible_committed_config(opctx, rack_id)
            .await?;
        let new_epoch = latest_epoch.next();
        let proposed = self
            .remove_sled_proposed_config(
                latest_committed_config,
                new_epoch,
                sled_to_remove,
            )
            .await?;
        self.db_datastore.tq_insert_latest_config(opctx, proposed).await?;

        // Read back the real configuration from the database. Importantly this
        // includes a chosen coordinator.
        let Some(new_config) =
            self.db_datastore.tq_get_config(opctx, rack_id, new_epoch).await?
        else {
            return Err(Error::internal_error(&format!(
                "Cannot retrieve newly inserted trust quorum \
                 configuration for rack {rack_id}, epoch {new_epoch}."
            )));
        };

        // Retrieve the sled for the coordinator
        let client = self
            .get_coordinator_client(
                opctx,
                rack_id,
                new_epoch,
                &new_config.coordinator,
            )
            .await?;

        // Now send the reconfiguration request to the coordinator. We do
        // this directly in the API handler because this is a non-idempotent
        // operation and we only want to issue it once.
        let req = trust_quorum_types::messages::ReconfigureMsg {
            rack_id: new_config.rack_id,
            epoch: new_config.epoch,
            last_committed_epoch: new_config.last_committed_epoch,
            members: new_config.members.keys().cloned().collect(),
            threshold: new_config.threshold,
        };
        client.trust_quorum_reconfigure(&req).await?;

        Ok(new_epoch)
    }

    /// Abort the latest trust quorum configuration if it is still in the
    /// preparing state.
    pub(crate) async fn tq_abort_latest_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> Result<TrustQuorumConfig, Error> {
        let Some(latest_config) =
            self.db_datastore.tq_get_latest_config(opctx, rack_id).await?
        else {
            return Err(Error::non_resourcetype_not_found(
                "No trust quorum configuration exists for this rack",
            ));
        };

        self.db_datastore
            .tq_abort_config(
                opctx,
                rack_id,
                latest_config.epoch,
                "Aborted via API request".to_string(),
            )
            .await?;

        // Return the updated configuration
        self.db_datastore
            .tq_get_config(opctx, rack_id, latest_config.epoch)
            .await?
            .ok_or_else(|| {
                Error::internal_error(
                    "Configuration was just aborted but cannot be retrieved",
                )
            })
    }

    /// Read the set of all commissioned sleds from an existing deployment and
    /// issue an upgrade request to trust quorum.
    ///
    /// If there is a committed trust quorum configuration, or reconfiguration
    /// in progress, then an error will be returned.
    ///
    /// Returns the epoch of the configuration resulting from the upgrade
    /// request on success.
    pub(crate) async fn tq_upgrade_from_lrtq(
        &self,
        opctx: &OpContext,
    ) -> Result<Epoch, Error> {
        // We are only operating on a single rack here.
        let rack_id = RackUuid::from_untyped_uuid(self.rack_id());

        // Let's first see if a configuration exists.
        let new_epoch = if let Some(latest_config) =
            self.db_datastore.tq_get_latest_config(opctx, rack_id).await?
        {
            // Is there a committed configuration? `committing` is irreversable,
            // and also indicates trust quorum has taken over.
            if latest_config.last_committed_epoch.is_some()
                || latest_config.state.is_committing()
                || latest_config.state.is_committed()
            {
                return Err(Error::conflict(format!(
                    "Cannot upgrade from LRTQ. Rack {} is already upgraded.",
                    rack_id
                )));
            }

            // Is there an upgrade currently in progress?
            if latest_config.state.is_preparing() {
                return Err(Error::conflict(format!(
                    "Upgrade from LRTQ already in progress for Rack {rack_id}, \
                     epoch: {}.",
                    latest_config.epoch
                )));
            }

            // We can proceed with upgrade, but we need to bump the epoch from
            // the last attempt.
            latest_config.epoch.next()
        } else {
            Epoch(2)
        };

        // Now get the set of all commissioned sleds.
        //
        // It's fine to call the batched version, as we plan to remove all the
        // LRTQ code before we implement multirack.
        let sleds = self
            .db_datastore
            .sled_list_all_batched(
                opctx,
                nexus_types::deployment::SledFilter::Commissioned,
            )
            .await?;

        // Create the set of members from returned sleds
        let members: BTreeSet<_> = sleds
            .iter()
            .map(|sled| BaseboardId {
                part_number: sled.part_number().to_string(),
                serial_number: sled.serial_number().to_string(),
            })
            .collect();

        // Create a proposed configuration for the upgrade and save it to the DB
        let proposed = ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::Yes,
            members: members.clone(),
        };
        self.db_datastore.tq_insert_latest_config(opctx, proposed).await?;

        // Read back the real configuration from the database. Importantly this
        // includes a chosen coordinator.
        let Some(new_config) =
            self.db_datastore.tq_get_config(opctx, rack_id, new_epoch).await?
        else {
            return Err(Error::internal_error(&format!(
                "Cannot retrieve newly inserted trust quorum \
                configuration for LRTQ upgrade for rack {}, epoch {}.",
                rack_id, new_epoch
            )));
        };

        // Retrieve the sled for the coordinator
        let client = self
            .get_coordinator_client(
                opctx,
                rack_id,
                new_epoch,
                &new_config.coordinator,
            )
            .await?;

        // Finally, initiate the upgrade at the coordinator
        let req = trust_quorum_types::messages::LrtqUpgradeMsg {
            rack_id,
            epoch: new_epoch,
            members,
            threshold: new_config.threshold,
        };
        client.trust_quorum_upgrade_from_lrtq(&req).await?;

        Ok(new_epoch)
    }

    /// Retrieve a `Sled` from the database for a specific coordinator ID and
    // create a client to talk to the coordinator sled agent.
    async fn get_coordinator_client(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        epoch: Epoch,
        coordinator: &BaseboardId,
    ) -> Result<sled_agent_client::Client, Error> {
        // Retrieve the sled for the coordinator
        let Some(sled) = self
            .db_datastore
            .sled_get_commissioned_by_baseboard_and_rack_id(
                opctx,
                rack_id,
                coordinator.clone(),
            )
            .await?
        else {
            let msg = format!(
                "Coordinator sled for trust quorum reconfiguration is no \
                longer commissioned or is present in another rack. \
                Configuration stored in database for this configuration will \
                be aborted. Expected rack_id: {rack_id}, baseboard_id: {}.",
                coordinator
            );

            self.db_datastore
                .tq_abort_config(opctx, rack_id, epoch, msg.clone())
                .await
                .map_err(|e| {
                    Error::conflict(format!(
                        "{}. Unfortunately, writing \
                        the abort to the database also failed with error: {e}. \
                        Abort will have to be performed explicitly by the \
                        operator.",
                        msg
                    ))
                })?;

            return Err(Error::conflict(msg));
        };

        // Construct a sled agent client to talk to the coordinator
        let timeout = Duration::from_secs(60);
        let url = format!("http://{}", sled.address());
        let log = self.log.new(o!("SledAgent" => url.clone()));
        let reqwest_client = reqwest::ClientBuilder::new()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .unwrap();

        Ok(sled_agent_client::Client::new_with_client(
            &url,
            reqwest_client,
            log,
        ))
    }

    // Create a new `ProposedTrustQuorumConfig` with `sled_to_remove` removed
    // from the membership. Return an error if the sled is not a member of the
    // `latest_committed_config`.
    async fn remove_sled_proposed_config(
        &self,
        latest_committed_config: TrustQuorumConfig,
        new_epoch: Epoch,
        sled_to_remove: BaseboardId,
    ) -> Result<ProposedTrustQuorumConfig, Error> {
        let rack_id = latest_committed_config.rack_id;
        let mut members: BTreeSet<_> =
            latest_committed_config.members.keys().cloned().collect();

        if !members.remove(&sled_to_remove) {
            return Err(Error::invalid_request(format!(
                "Sled {} is not a member of the trust quorum.",
                sled_to_remove
            )));
        }

        Ok(ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: latest_committed_config.epoch,
            },
            members,
        })
    }

    // Create a new `ProposedTrustQuorumConfig` including `new_sleds` in
    // the membership. Return an error if any of the new sleds exist in the
    // `latest_committed_config` membership already.
    async fn add_sleds_proposed_config(
        &self,
        latest_committed_config: TrustQuorumConfig,
        new_epoch: Epoch,
        new_sleds: BTreeSet<BaseboardId>,
    ) -> Result<ProposedTrustQuorumConfig, Error> {
        let rack_id = latest_committed_config.rack_id;
        let existing: BTreeSet<_> =
            latest_committed_config.members.keys().cloned().collect();

        let intersection: BTreeSet<_> =
            existing.intersection(&new_sleds).collect();
        if !intersection.is_empty() {
            return Err(Error::invalid_request(format!(
                "The following sleds are already members of the trust quorum: \
                 {intersection:?}."
            )));
        }

        Ok(ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: latest_committed_config.epoch,
            },
            members: new_sleds.union(&existing).cloned().collect(),
        })
    }

    // Load the latest committed trust quorum configuration for `rack_id` and
    // return it along with the epoch for the latest configuration.
    //
    // If the configuration is aborted, then check to see what its latest
    // `last_committed_epoch` is and load that configuration.
    //
    // Note that the configuration that comes back may not actually be committed
    // yet if it's the latest configuration. That is ok because we will perform
    // validation during insert of the any newly proposed config.
    pub async fn tq_load_latest_possible_committed_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> Result<(TrustQuorumConfig, Epoch), Error> {
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

        // We assume this config is committed as long as it is not aborted.
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

        Ok((latest_committed_config, highest_epoch))
    }
}
