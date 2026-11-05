// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sleds, and the hardware and services within them.

use crate::internal_api::params::{
    PhysicalDiskPutRequest, SledAgentInfo, ZpoolPutRequest,
};
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::sled::SledReservationReason;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api;
use nexus_types::external_api::path_params;
use nexus_types::external_api::physical_disk::PhysicalDiskPolicy;
use nexus_types::external_api::sled::{SledPolicy, SledProvisionPolicy};
use omicron_common::api::external::ByteCount;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PhysicalDiskAdoptionRequestUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use sled_agent_client::Client as SledAgentClient;
use sled_agent_types::inventory::SledRegistrationInfo;
use sled_agent_types::inventory::SledRole;
use sled_hardware_types::BaseboardId;
use slog_error_chain::InlineErrorChain;
use std::net::SocketAddrV6;
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    // Sleds
    pub fn sled_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        sled_id: &SledUuid,
    ) -> LookupResult<lookup::Sled<'a>> {
        nexus_networking::sled_lookup(&self.db_datastore, opctx, *sled_id)
    }

    // TODO-robustness we should have a limit on how many sled agents there can
    // be (for graceful degradation at large scale).
    pub(crate) async fn upsert_sled(
        &self,
        opctx: &OpContext,
        id: SledUuid,
        info: SledAgentInfo,
    ) -> Result<(), Error> {
        let sa_address = info.sa_address;
        let log = opctx.log.new(o!(
            "sled_uuid" => id.to_string(),
            "sa_address" => sa_address.to_string(),
        ));

        // Fetch additional information via a `sled_registration_info` callback.
        //
        // This is a synchronous callback, which puts some fairly tight
        // requirements on sled_registration_info.
        let client = nexus_networking::sled_client_from_address_ext(
            id,
            sa_address,
            &log,
            self.reqwest_client.clone(),
        );
        let registration_info: SledRegistrationInfo = client
            .sled_registration_info()
            .await
            .map_err(|error| {
                match classify_sled_registration_info_fetch_error(
                    id, sa_address, &error,
                ) {
                    SledRegistrationInfoFetchError::Transient(public_error) => {
                        warn!(
                            log,
                            "failed to fetch sled registration info (transient)";
                            InlineErrorChain::new(&error),
                        );
                        public_error
                    }
                    SledRegistrationInfoFetchError::Permanent(public_error) => {
                        error!(
                            log,
                            "failed to fetch sled registration info";
                            InlineErrorChain::new(&error),
                        );
                        public_error
                    }
                }
            })?
            .into_inner();

        if registration_info.sled_id != id {
            error!(
                log,
                "sled agent reported a different sled ID than the request path";
                "reported_sled_uuid" => %registration_info.sled_id,
            );
            return Err(Error::invalid_request(format!(
                "sled agent at {sa_address} reports sled ID {}, which does \
                 not match sled ID {id} in the request path; refusing to \
                 register sled {id} with another sled's information",
                registration_info.sled_id,
            )));
        }

        let rack_id = registration_info.rack_id;
        match self.rack_lookup(opctx, &rack_id).await {
            Ok(_) => {}
            Err(Error::ObjectNotFound {
                type_name: ResourceType::Rack,
                lookup_type: _,
            }) => {
                // Warn rather than error and 503 rather than 400, since this
                // state is expected while a rack is being added to the fleet.
                // Sled Agent is expected to keep retrying until this succeeds.
                warn!(
                    log,
                    "sled agent reported a rack that is not part of the fleet";
                    "reported_rack_id" => %rack_id,
                );
                return Err(Error::unavail(&format!(
                    "sled agent at {sa_address} reports rack ID {rack_id} for \
                     sled {id}, but that rack is not part of the fleet (it \
                     may not have been adopted yet); not registering the sled \
                     until it is",
                )));
            }
            Err(error) => {
                return Err(error.internal_context(format!(
                    "checking that rack {rack_id} reported for sled {id} by \
                     the sled agent at {sa_address} is part of the fleet",
                )));
            }
        }

        let is_scrimlet = match info.role {
            SledRole::Gimlet => false,
            SledRole::Scrimlet => true,
        };

        let sled = db::model::SledUpdate::new(
            id,
            info.sa_address,
            info.repo_depot_port,
            db::model::SledBaseboard {
                serial_number: info.baseboard.serial,
                part_number: info.baseboard.part,
                revision: info.baseboard.revision,
            },
            db::model::SledSystemHardware {
                is_scrimlet,
                usable_hardware_threads: info.usable_hardware_threads,
                usable_physical_ram: info.usable_physical_ram.into(),
                reservoir_size: info.reservoir_size.into(),
                cpu_family: info.cpu_family.into(),
            },
            rack_id,
            info.generation,
        );
        self.db_datastore.sled_upsert(sled).await?;
        info!(log, "registered sled agent"; "rack_id" => %rack_id);

        // If a new sled-agent just came online we want to trigger inventory
        // collection.
        //
        // This will allow us to learn about disks so that they can be added to
        // the control plane.
        //
        // Note that sled_upsert returns an Err when nothing was written, so if
        // we get to this point, it means that an upsert actually happened.
        self.activate_inventory_collection();

        Ok(())
    }

    /// Mark a sled as expunged
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    ///
    /// This is idempotent, and it returns the old policy of the sled.
    pub(crate) async fn sled_expunge(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<SledPolicy, Error> {
        let sled_lookup = self.sled_lookup(opctx, &sled_id)?;
        let (authz_sled, sled) =
            sled_lookup.fetch_for(authz::Action::Modify).await?;

        let rack_id = sled.rack_id();
        let authz_tq = authz::TrustQuorumConfig::for_rack_id(rack_id);

        // If the sled still exists in the latest committed trust quorum
        // configuration, it cannot be expunged.
        //
        // This is just a sanity check. There is an inherent TOCTUO here, since
        // we aren't combining the check with with the policy change inside
        // a transaction. However, it is unlikely that an operator would be
        // creating a different trust quorum configuration while trying to
        // expunge this sled, since both have to be done from omdb by an oxide
        // employee right now.
        //
        // When we add an external API, we'll probably want to come back and add
        // some extra safeguards including checking that there are no ongoing
        // configurations currently, as the last committed configuration can be
        // different from the current configuration. However, if we checked that
        // here we'd have to do yet more DB lookups to check that the state of
        // the last configuration wasn't aborted. This is probably good enough
        // for now given that the user already has to confirm a bunch of prompts
        // before kicking off the operation.
        let (tq_latest_committed_config, _) = self
            .tq_load_latest_possible_committed_config(opctx, authz_tq)
            .await?;
        let baseboard_id = BaseboardId {
            part_number: sled.part_number().to_string(),
            serial_number: sled.serial_number().to_string(),
        };
        if tq_latest_committed_config.members.contains_key(&baseboard_id) {
            return Err(Error::conflict(format!(
                "Cannot expunge sled {sled_id}, as its baseboard \
                {baseboard_id} is still a member of the latest committed \
                trust quorum configuration at epoch {} for rack {rack_id}",
                tq_latest_committed_config.epoch
            )));
        }

        let prev_policy = self
            .db_datastore
            .sled_set_policy_to_expunged(opctx, &authz_sled)
            .await?;

        // The instance-watcher background task is responsible for marking any
        // VMMs running on `Expunged` sleds as `Failed`, so that their instances
        // can transition to `Failed` and be deleted or restarted. Let's go
        // ahead and activate it now so that those instances don't need to wait
        // for the next periodic activation before they can be cleaned up.
        self.background_tasks.task_instance_watcher.activate();

        // The blueprint planner is going to perform actions based on the
        // expungement, so kick it off now.
        self.background_tasks.task_blueprint_planner.activate();

        Ok(prev_policy)
    }

    pub(crate) async fn sled_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<external_api::sled::Sled> {
        let inv = self.inventory_load_rx().borrow().clone();
        let sleds = self
            .db_datastore
            .sled_list(&opctx, &pagparams, SledFilter::InService)
            .await?
            .into_iter()
            .map(|sled| sled.to_external_api(&inv))
            .collect::<Vec<_>>();
        Ok(sleds)
    }

    pub async fn sled_client(
        &self,
        id: &SledUuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let client =
            nexus_networking::default_reqwest_client_builder().build().unwrap();
        self.sled_client_ext(id, client).await
    }

    pub async fn sled_client_ext(
        &self,
        id: &SledUuid,
        client: reqwest::Client,
    ) -> Result<Arc<SledAgentClient>, Error> {
        // TODO: We should consider injecting connection pooling here,
        // but for now, connections to sled agents are constructed
        // on an "as requested" basis.
        //
        // Frankly, returning an "Arc" here without a connection pool is a
        // little silly; it's not actually used if each client connection exists
        // as a one-shot.
        let client = nexus_networking::sled_client_ext(
            &self.db_datastore,
            &self.opctx_alloc,
            *id,
            &self.log,
            client,
        )
        .await?;
        Ok(Arc::new(client))
    }

    pub(crate) async fn reserve_on_random_sled(
        &self,
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        resources: db::model::Resources,
        constraints: db::model::SledReservationConstraints,
        reservation_reason: SledReservationReason,
    ) -> Result<db::model::SledResourceVmm, Error> {
        self.db_datastore
            .sled_reservation_create(
                &self.opctx_alloc,
                instance_id,
                propolis_id,
                resources,
                constraints,
                reservation_reason,
            )
            .await
    }

    pub(crate) async fn delete_sled_reservation(
        &self,
        vmm_id: PropolisUuid,
    ) -> Result<(), Error> {
        self.db_datastore
            .sled_reservation_delete(&self.opctx_alloc, vmm_id)
            .await
    }

    /// Returns the old provision policy.
    pub(crate) async fn sled_set_provision_policy(
        &self,
        opctx: &OpContext,
        sled_lookup: &lookup::Sled<'_>,
        new_policy: SledProvisionPolicy,
    ) -> Result<SledProvisionPolicy, Error> {
        let (authz_sled,) =
            sled_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .sled_set_provision_policy(opctx, &authz_sled, new_policy)
            .await
    }

    // Physical disks

    pub fn physical_disk_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        disk_selector: &path_params::PhysicalDiskPath,
    ) -> Result<lookup::PhysicalDisk<'a>, Error> {
        // XXX how to do typed UUID as part of dropshot path?
        Ok(LookupPath::new(&opctx, &self.db_datastore).physical_disk(
            PhysicalDiskUuid::from_untyped_uuid(disk_selector.disk_id),
        ))
    }

    /// Return a page of physical disks for a given sled id
    pub(crate) async fn sled_list_physical_disks(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::PhysicalDisk> {
        self.db_datastore
            .sled_list_physical_disks(&opctx, sled_id, pagparams)
            .await
    }

    pub(crate) async fn physical_disk_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::PhysicalDisk> {
        self.db_datastore
            .physical_disk_list(&opctx, pagparams, DiskFilter::InService)
            .await
    }

    pub(crate) async fn physical_disk_list_unadopted(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<db::model::InvPhysicalDisk> {
        let collection_id =
            self.db_datastore.inventory_get_latest_collection_id(opctx).await?;
        let Some(collection_id) = collection_id else {
            return Ok(vec![]);
        };
        self.db_datastore
            .physical_disk_unadopted_list(opctx, collection_id)
            .await
    }

    pub(crate) async fn physical_disk_adoption_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<db::model::PhysicalDiskAdoptionRequest> {
        self.db_datastore
            .physical_disk_adoption_request_list(opctx, pagparams)
            .await
    }

    pub(crate) async fn physical_disk_enable_adoption(
        &self,
        opctx: &OpContext,
        disk_id: nexus_types::external_api::physical_disk::PhysicalDiskManufacturerIdentity,
    ) -> Result<
        nexus_types::external_api::physical_disk::PhysicalDiskAdoptionRequest,
        Error,
    > {
        let request = self
            .db_datastore
            .physical_disk_enable_adoption(opctx, disk_id)
            .await?;
        Ok(request.into())
    }

    pub(crate) async fn physical_disk_disable_adoption(
        &self,
        opctx: &OpContext,
        req_uuid: PhysicalDiskAdoptionRequestUuid,
    ) -> Result<(), Error> {
        self.db_datastore
            .physical_disk_adoption_request_delete(opctx, req_uuid)
            .await
    }

    /// Inserts a physical disk into the database unless it already exists.
    ///
    /// NOTE: I'd like to re-work this to avoid the upsert-like behavior - can
    /// we restructure our tests to ensure they ask for this physical disk
    /// exactly once?
    pub(crate) async fn insert_test_physical_disk_if_not_exists(
        &self,
        opctx: &OpContext,
        request: PhysicalDiskPutRequest,
    ) -> Result<(), Error> {
        info!(
            self.log, "inserting test physical disk";
            "physical_disk_id" => %request.id,
            "sled_id" => %request.sled_id,
            "vendor" => %request.vendor,
            "serial" => %request.serial,
            "model" => %request.model,
        );

        match LookupPath::new(&opctx, &self.db_datastore)
            .physical_disk(request.id)
            .fetch()
            .await
        {
            Ok((_authz_disk, existing_disk)) => {
                if existing_disk.vendor != request.vendor
                    || existing_disk.serial != request.serial
                    || existing_disk.model != request.model
                {
                    return Err(Error::internal_error(&format!(
                        "Invalid Physical Disk update (was: {existing_disk:?}, asking for {request:?})"
                    )));
                }
                return Ok(());
            }
            Err(Error::ObjectNotFound { .. }) => {}
            Err(err) => return Err(err),
        }

        let disk = db::model::PhysicalDisk::from_parts(
            request.id,
            request.vendor,
            request.serial,
            request.model,
            request.variant.into(),
            request.sled_id,
        );
        self.db_datastore.physical_disk_insert(&opctx, disk).await?;
        Ok(())
    }

    /// Mark a physical disk as expunged
    ///
    /// This is an irreversible process! It should only be called after
    /// sufficient warning to the operator.
    pub(crate) async fn physical_disk_expunge(
        &self,
        opctx: &OpContext,
        disk: path_params::PhysicalDiskPath,
    ) -> Result<(), Error> {
        let physical_disk_lookup = self.physical_disk_lookup(opctx, &disk)?;
        let (authz_disk,) =
            physical_disk_lookup.lookup_for(authz::Action::Modify).await?;
        self.db_datastore
            .physical_disk_update_policy(
                opctx,
                authz_disk.id(),
                PhysicalDiskPolicy::Expunged.into(),
            )
            .await
    }

    // Zpools (contained within sleds)

    /// Upserts a Zpool into the database, updating it if it already exists.
    pub(crate) async fn upsert_zpool(
        &self,
        opctx: &OpContext,
        request: ZpoolPutRequest,
    ) -> Result<(), Error> {
        info!(
            self.log, "upserting zpool";
            "sled_id" => %request.sled_id,
            "zpool_id" => %request.id,
            "physical_disk_id" => %request.physical_disk_id,
        );

        let zpool = db::model::Zpool::new(
            request.id,
            request.sled_id,
            request.physical_disk_id,
            // This function is only called from tests, so it does not need a
            // real value here.
            ByteCount::from_gibibytes_u32(0).into(),
        );
        self.db_datastore.zpool_insert(&opctx, zpool).await?;
        Ok(())
    }

    // Datasets (contained within zpools)

    /// Upserts a Crucible dataset into the database, updating it if it already
    /// exists.
    pub(crate) async fn upsert_crucible_dataset(
        &self,
        id: DatasetUuid,
        zpool_id: ZpoolUuid,
        address: SocketAddrV6,
    ) -> Result<(), Error> {
        info!(
            self.log,
            "upserting Crucible dataset";
            "zpool_id" => zpool_id.to_string(),
            "dataset_id" => id.to_string(),
        );
        let dataset = db::model::CrucibleDataset::new(id, zpool_id, address);
        self.db_datastore.crucible_dataset_upsert(dataset).await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum SledRegistrationInfoFetchError {
    Transient(Error),
    Permanent(Error),
}

fn classify_sled_registration_info_fetch_error(
    sled_id: SledUuid,
    sa_address: SocketAddrV6,
    error: &sled_agent_client::Error<sled_agent_client::types::Error>,
) -> SledRegistrationInfoFetchError {
    let message = format!(
        "failed to fetch registration info for sled {sled_id} from its sled \
         agent at {sa_address}: {}",
        InlineErrorChain::new(error),
    );
    let is_transient = match error {
        sled_agent_client::Error::CommunicationError(_)
        | sled_agent_client::Error::ResponseBodyError(_) => true,
        sled_agent_client::Error::ErrorResponse(_)
        | sled_agent_client::Error::UnexpectedResponse(_)
        | sled_agent_client::Error::InvalidUpgrade(_) => error.is_retryable(),
        sled_agent_client::Error::InvalidRequest(_)
        | sled_agent_client::Error::InvalidResponsePayload(..)
        | sled_agent_client::Error::Custom(_) => false,
    };
    if is_transient {
        SledRegistrationInfoFetchError::Transient(Error::unavail(&message))
    } else {
        SledRegistrationInfoFetchError::Permanent(Error::internal_error(
            &message,
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn error_response(
        status: reqwest::StatusCode,
    ) -> sled_agent_client::Error<sled_agent_client::types::Error> {
        sled_agent_client::Error::ErrorResponse(
            progenitor_client::ResponseValue::new(
                sled_agent_client::types::Error {
                    error_code: None,
                    message: "stub sled agent failure".to_string(),
                    request_id: "stub-request".to_string(),
                },
                status,
                reqwest::header::HeaderMap::new(),
            ),
        )
    }

    #[test]
    fn sled_registration_info_fetch_error_response_status() {
        let sled_id = SledUuid::new_v4();
        let sa_address: SocketAddrV6 =
            "[::1]:12345".parse().expect("parsed a socket address");
        let message = |error: &sled_agent_client::Error<
            sled_agent_client::types::Error,
        >| {
            format!(
                "failed to fetch registration info for sled {sled_id} from \
                 its sled agent at {sa_address}: {error}",
            )
        };

        let unavailable =
            error_response(reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            classify_sled_registration_info_fetch_error(
                sled_id,
                sa_address,
                &unavailable,
            ),
            SledRegistrationInfoFetchError::Transient(
                Error::ServiceUnavailable {
                    internal_message: message(&unavailable),
                }
            ),
        );

        let internal =
            error_response(reqwest::StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            classify_sled_registration_info_fetch_error(
                sled_id, sa_address, &internal,
            ),
            SledRegistrationInfoFetchError::Permanent(Error::InternalError {
                internal_message: message(&internal),
            }),
        );
    }
}
