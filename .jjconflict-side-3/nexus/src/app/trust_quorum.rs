// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus APIs for trust quorum

use nexus_auth::authz;
use nexus_auth::context::OpContext;
use nexus_networking::InServiceSledClientError;
use nexus_networking::sled_client_by_baseboard_id_and_rack_id_if_in_service_ext;
use nexus_types::external_api::rack::SledAddPrecheckOverrides;
use nexus_types::trust_quorum::{
    IsLrtqUpgrade, ProposedTrustQuorumConfig, TrustQuorumConfig,
    TrustQuorumMembers,
};
use omicron_common::api::external::Error;
use omicron_common::http_client::internal_http_client_builder;
use omicron_uuid_kinds::RackUuid;
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
        authz_tq: authz::TrustQuorumConfig,
        new_sleds: BTreeSet<BaseboardId>,
        override_prechecks: &SledAddPrecheckOverrides,
    ) -> Result<TrustQuorumConfig, Error> {
        let rack_id = authz_tq.rack().id();
        let (latest_committed_config, latest_epoch) = self
            .db_datastore
            .tq_load_latest_possible_committed_config(opctx, authz_tq.clone())
            .await?;

        // Check the new sleds' software before allowing them to be added to
        // the system.
        self.sled_check_versions(opctx, &new_sleds, override_prechecks).await?;

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
            self.db_datastore.tq_get_config(opctx, authz_tq, new_epoch).await?
        else {
            return Err(Error::internal_error(&format!(
                "Cannot retrieve newly inserted trust quorum \
                    configuration for rack {rack_id}, epoch {new_epoch}."
            )));
        };

        // Now send the reconfiguration request to the coordinator. We do
        // this directly in the API handler because this is a non-idempotent
        // operation and we only want to issue it once.
        self.tq_send_reconfigure(opctx, &new_config).await?;

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
        rack_id: RackUuid,
        sled_to_remove: BaseboardId,
    ) -> Result<Epoch, Error> {
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(rack_id);

        let (latest_committed_config, latest_epoch) = self
            .db_datastore
            .tq_load_latest_possible_committed_config(opctx, authz_tq.clone())
            .await?;
        let new_epoch = latest_epoch.next();
        let proposed = ProposedTrustQuorumConfig::remove_sled(
            &latest_committed_config,
            new_epoch,
            &sled_to_remove,
        )?;
        self.db_datastore.tq_insert_latest_config(opctx, proposed).await?;

        // Read back the real configuration from the database. Importantly this
        // includes a chosen coordinator.
        let Some(new_config) =
            self.db_datastore.tq_get_config(opctx, authz_tq, new_epoch).await?
        else {
            return Err(Error::internal_error(&format!(
                "Cannot retrieve newly inserted trust quorum \
                 configuration for rack {rack_id}, epoch {new_epoch}."
            )));
        };

        // Now send the reconfiguration request to the coordinator. We do
        // this directly in the API handler because this is a non-idempotent
        // operation and we only want to issue it once.
        self.tq_send_reconfigure(opctx, &new_config).await?;

        Ok(new_epoch)
    }

    /// Abort the latest trust quorum configuration if it is still in the
    /// preparing state.
    pub(crate) async fn tq_abort_latest_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> Result<TrustQuorumConfig, Error> {
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(rack_id);
        let Some(latest_config) = self
            .db_datastore
            .tq_get_latest_config(opctx, authz_tq.clone())
            .await?
        else {
            return Err(Error::non_resourcetype_not_found(
                "No trust quorum configuration exists for this rack",
            ));
        };

        self.db_datastore
            .tq_abort_config(
                opctx,
                authz_tq,
                latest_config.epoch,
                "Aborted via API request".to_string(),
            )
            .await
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
        let rack_id = self.rack_id();
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(rack_id);

        // Let's first see if a configuration exists.
        let new_epoch = if let Some(latest_config) = self
            .db_datastore
            .tq_get_latest_config(opctx, authz_tq.clone())
            .await?
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
        let members = TrustQuorumMembers::new(members)
            .map_err(|error| Error::internal_error(&error.to_string()))?;
        let proposed = ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::Yes,
            members,
        };
        self.db_datastore.tq_insert_latest_config(opctx, proposed).await?;

        // Read back the real configuration from the database. Importantly this
        // includes a chosen coordinator.
        let Some(new_config) =
            self.db_datastore.tq_get_config(opctx, authz_tq, new_epoch).await?
        else {
            return Err(Error::internal_error(&format!(
                "Cannot retrieve newly inserted trust quorum \
                configuration for LRTQ upgrade for rack {}, epoch {}.",
                rack_id, new_epoch
            )));
        };

        // Finally, initiate the upgrade at the coordinator
        self.tq_send_lrtq_upgrade(opctx, &new_config).await?;

        Ok(new_epoch)
    }

    /// Send a `ReconfigureMsg` corresponding to a config to its coordinator.
    ///
    /// The Nexus that successfully inserted `config` into the database should
    /// call this, once, right after that insert. A re-send of the same config
    /// is a no-op while the coordinator is still coordinating that epoch, but
    /// may be rejected with an error once it has lost its in-memory state
    /// (e.g., after a restart). Resending is harmless but can surface a
    /// spurious error.
    ///
    /// XXX: The caller obligation is a bit strange. If the Nexus that inserted
    /// the config crashes before this is called, the coordinator never receives
    /// the config, and the reconfiguration gets stuck in the preparing phase.
    /// We should fix this for real.
    async fn tq_send_reconfigure(
        &self,
        opctx: &OpContext,
        config: &TrustQuorumConfig,
    ) -> Result<(), Error> {
        let client = match self.get_coordinator_client(opctx, config).await {
            Ok(client) => client,
            Err(InServiceSledClientError::NotInService { .. }) => {
                self.background_tasks.task_trust_quorum_manager.activate();
                return Err(coordinator_not_in_service_error(config));
            }
            Err(InServiceSledClientError::LookupFailed { error, .. }) => {
                return Err(error);
            }
        };
        let req = trust_quorum_types::messages::ReconfigureMsg {
            rack_id: config.rack_id,
            epoch: config.epoch,
            last_committed_epoch: config.last_committed_epoch,
            members: config.members.keys().cloned().collect(),
            threshold: config.threshold,
        };
        client.trust_quorum_reconfigure(&req).await?;
        Ok(())
    }

    /// Send an `LrtqUpgradeMsg` corresponding to a config to its coordinator.
    ///
    /// A re-sent LRTQ upgrade always produces an error.
    ///
    /// TODO: All customer racks have been upgraded to real trust quorum, so we
    /// should clean this up.
    async fn tq_send_lrtq_upgrade(
        &self,
        opctx: &OpContext,
        config: &TrustQuorumConfig,
    ) -> Result<(), Error> {
        let client = match self.get_coordinator_client(opctx, config).await {
            Ok(client) => client,
            Err(InServiceSledClientError::NotInService { .. }) => {
                self.background_tasks.task_trust_quorum_manager.activate();
                return Err(coordinator_not_in_service_error(config));
            }
            Err(InServiceSledClientError::LookupFailed { error, .. }) => {
                return Err(error);
            }
        };
        let req = trust_quorum_types::messages::LrtqUpgradeMsg {
            rack_id: config.rack_id,
            epoch: config.epoch,
            members: config.members.keys().cloned().collect(),
            threshold: config.threshold,
        };
        client.trust_quorum_upgrade_from_lrtq(&req).await?;
        Ok(())
    }

    /// Retrieve a `Sled` from the database for a specific coordinator ID and
    /// create a client to talk to the coordinator sled agent.
    ///
    /// Return an error if the sled is no longer in service, or if the lookup
    /// fails.
    async fn get_coordinator_client(
        &self,
        opctx: &OpContext,
        config: &TrustQuorumConfig,
    ) -> Result<sled_agent_client::Client, InServiceSledClientError> {
        let timeout = Duration::from_secs(60);
        let reqwest_client = internal_http_client_builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .unwrap();
        sled_client_by_baseboard_id_and_rack_id_if_in_service_ext(
            &self.db_datastore,
            opctx,
            &config.coordinator,
            config.rack_id,
            &self.log,
            reqwest_client,
        )
        .await
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

        let members = TrustQuorumMembers::new(
            new_sleds.union(&existing).cloned().collect(),
        )
        .map_err(|error| Error::internal_error(&error.to_string()))?;

        Ok(ProposedTrustQuorumConfig {
            rack_id,
            epoch: new_epoch,
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: latest_committed_config.epoch,
            },
            members,
        })
    }
}

fn coordinator_not_in_service_error(config: &TrustQuorumConfig) -> Error {
    Error::conflict(format!(
        "Coordinator {} for the trust quorum configuration at epoch {} is no \
         longer an in-service sled in rack {}, so the request was not sent to \
         it. The trust quorum background task will abort this \
         configuration; retry once it has been aborted.",
        config.coordinator, config.epoch, config.rack_id
    ))
}
