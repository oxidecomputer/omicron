// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests for APIs against sled-based endpoints.

use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use nexus_db_lookup::LookupPath;
use nexus_db_model::PhysicalDisk as DbPhysicalDisk;
use nexus_db_model::PhysicalDiskKind as DbPhysicalDiskKind;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_lockstep_client::types::SledPolicy;
use nexus_lockstep_client::types::SledProvisionPolicy;
use nexus_lockstep_client::types::SledSelector;
use nexus_test_utils::SLED_AGENT_UUID;
use nexus_test_utils::background::run_inventory_collection;
use nexus_test_utils::resource_helpers::create_default_ip_pools;
use nexus_test_utils::resource_helpers::create_instance;
use nexus_test_utils::resource_helpers::create_project;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::physical_disk::PhysicalDisk;
use nexus_types::external_api::sled::Sled;
use nexus_types::external_api::sled::SledInstance;
use nexus_types::trust_quorum::IsLrtqUpgrade;
use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
use nexus_types::trust_quorum::TrustQuorumConfig;
use nexus_types::trust_quorum::TrustQuorumConfigState;
use omicron_sled_agent::sim;
use omicron_sled_agent::sim::SledCpuFamily;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use sled_hardware_types::BaseboardId;
use std::collections::BTreeSet;
use std::str::FromStr;
use trust_quorum_types::configuration::Configuration;
use trust_quorum_types::crypto::Sha3_256Digest;
use trust_quorum_types::types::Epoch;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

pub(crate) async fn sleds_list(
    client: &ClientTestContext,
    sleds_url: &str,
) -> Vec<Sled> {
    objects_list_page_authz::<Sled>(client, sleds_url).await.items
}

async fn physical_disks_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<PhysicalDisk> {
    objects_list_page_authz::<PhysicalDisk>(client, url).await.items
}

async fn sled_instance_list(
    client: &ClientTestContext,
    url: &str,
) -> Vec<SledInstance> {
    objects_list_page_authz::<SledInstance>(client, url).await.items
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_sleds_list(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&client, &sleds_url).await.len(), 2);

    // Now start a few more sled agents.
    let nsleds = 3;
    let mut sas = Vec::with_capacity(nsleds);
    for _ in 0..nsleds {
        sas.push(
            cptestctx
                .add_sled(
                    SledUuid::new_v4(),
                    sim::SimMode::Explicit,
                    SledCpuFamily::AmdMilan,
                )
                .await,
        );
    }

    // List sleds again.
    let sleds_found = sleds_list(&client, &sleds_url).await;
    assert_eq!(sleds_found.len(), nsleds + 2);

    let sledids_found =
        sleds_found.iter().map(|sv| sv.identity.id).collect::<Vec<Uuid>>();
    let mut sledids_found_sorted = sledids_found.clone();
    sledids_found_sorted.sort();
    assert_eq!(sledids_found, sledids_found_sorted);

    // Tear down the agents.
    for sa in sas {
        sa.http_server.close().await.unwrap();
    }
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_physical_disk_create_list_delete(
    cptestctx: &ControlPlaneTestContext,
) {
    let external_client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    assert_eq!(sleds_list(&external_client, &sleds_url).await.len(), 2);

    // The test framework may set up some disks initially.
    let disks_url =
        format!("/v1/system/hardware/sleds/{SLED_AGENT_UUID}/disks");
    let disks_initial = physical_disks_list(&external_client, &disks_url).await;

    // Inject a disk into the database, observe it in the external API
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let sled_id = SledUuid::from_str(&SLED_AGENT_UUID).unwrap();
    let physical_disk = DbPhysicalDisk::from_parts(
        PhysicalDiskUuid::new_v4(),
        "v".into(),
        "s".into(),
        "m".into(),
        DbPhysicalDiskKind::U2,
        sled_id,
    );

    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let _disk_id = datastore
        .physical_disk_insert(&opctx, physical_disk.clone())
        .await
        .expect("Failed to upsert physical disk");

    let disks = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(disks.len(), disks_initial.len() + 1);
    let new_disk = disks
        .iter()
        .find(|found_disk| {
            found_disk.vendor == "v"
                && found_disk.serial == "s"
                && found_disk.model == "m"
        })
        .expect("did not find the new disk");

    // Delete that disk using the internal API, observe it in the external API
    datastore
        .physical_disk_delete(
            &opctx,
            PhysicalDiskUuid::from_untyped_uuid(new_disk.identity.id),
        )
        .await
        .expect("Failed to upsert physical disk");

    let list = physical_disks_list(&external_client, &disks_url).await;
    assert_eq!(list, disks_initial, "{:#?}", list,);
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_sled_instance_list(cptestctx: &ControlPlaneTestContext) {
    let external_client = &cptestctx.external_client;

    // Verify that there are two sleds to begin with.
    let sleds_url = "/v1/system/hardware/sleds";
    let sleds = sleds_list(&external_client, &sleds_url).await;
    assert_eq!(sleds.len(), 2);

    // Verify that there are no instances on the sleds.
    for sled in &sleds {
        let sled_id = sled.identity.id;
        let instances_url =
            format!("/v1/system/hardware/sleds/{sled_id}/instances");
        assert!(
            sled_instance_list(&external_client, &instances_url)
                .await
                .is_empty()
        );
    }

    // Create an IP pool and project that we'll use for testing.
    create_default_ip_pools(&external_client).await;
    let project = create_project(&external_client, "test-project").await;
    let instance =
        create_instance(&external_client, "test-project", "test-instance")
            .await;

    // Ensure 1 instance was created on a sled
    let sled_instances = wait_for_condition(
        || {
            let sleds = sleds.clone();

            async move {
                let mut total_instances = vec![];

                for sled in &sleds {
                    let sled_id = sled.identity.id;

                    let instances_url = format!(
                        "/v1/system/hardware/sleds/{sled_id}/instances"
                    );

                    let mut sled_instances =
                        sled_instance_list(&external_client, &instances_url)
                            .await;

                    total_instances.append(&mut sled_instances);
                }

                if total_instances.len() == 1 {
                    Ok(total_instances)
                } else {
                    Err(CondCheckError::<()>::NotYet { status: None })
                }
            }
        },
        &std::time::Duration::from_millis(500),
        &std::time::Duration::from_secs(60),
    )
    .await
    .expect("one sled instance");

    assert_eq!(project.identity.name, sled_instances[0].project_name);
    assert_eq!(instance.identity.name, sled_instances[0].name);
}

struct TrustQuorumConfigDriver<'a> {
    opctx: &'a OpContext,
    datastore: &'a DataStore,
    rack_id: RackUuid,
}

impl TrustQuorumConfigDriver<'_> {
    fn authz_tq(&self) -> authz::TrustQuorumConfig {
        authz::TrustQuorumConfig::for_rack_id(self.rack_id)
    }

    async fn config(&self, epoch: u64) -> TrustQuorumConfig {
        self.datastore
            .tq_get_config(self.opctx, self.authz_tq(), Epoch(epoch))
            .await
            .expect("loaded trust quorum config")
            .unwrap_or_else(|| {
                panic!("trust quorum config at epoch {epoch} exists")
            })
    }

    async fn propose(
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
                    members: members.clone(),
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

    async fn ack_all_prepares(&self, epoch: u64) {
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
        let state = self
            .datastore
            .tq_update_prepare_status(self.opctx, coordinator_config, members)
            .await
            .unwrap_or_else(|error| {
                panic!("acked prepares at epoch {epoch}: {error}")
            });
        assert_eq!(state, TrustQuorumConfigState::Committing);
    }

    async fn ack_commits(
        &self,
        epoch: u64,
        acked: &BTreeSet<BaseboardId>,
        expected_state: TrustQuorumConfigState,
    ) {
        let state = self
            .datastore
            .tq_update_commit_status(
                self.opctx,
                self.authz_tq(),
                Epoch(epoch),
                acked.clone(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("acked commits at epoch {epoch}: {error}")
            });
        assert_eq!(state, expected_state);
    }

    async fn abort(&self, epoch: u64) {
        self.datastore
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
        assert_eq!(
            self.config(epoch).await.state,
            TrustQuorumConfigState::Aborted
        );
    }
}

async fn expunge_sled(
    client: &nexus_lockstep_client::Client,
    sled_id: SledUuid,
) -> Result<SledPolicy, (StatusCode, String)> {
    match client
        .sled_expunge(&SledSelector { sled: sled_id.into_untyped_uuid() })
        .await
    {
        Ok(response) => Ok(response.into_inner()),
        Err(nexus_lockstep_client::Error::ErrorResponse(response)) => {
            let status = response.status();
            Err((status, response.into_inner().message))
        }
        Err(error) => panic!("unexpected error expunging {sled_id}: {error}"),
    }
}

#[track_caller]
fn assert_rejected_as_member(
    result: Result<SledPolicy, (StatusCode, String)>,
    member_epoch: u64,
    member_state: TrustQuorumConfigState,
) {
    let (status, message) = result.expect_err("expunge was rejected");
    assert_eq!(status, StatusCode::CONFLICT, "message: {message}");
    for expected in [
        "is still a member of the latest trust quorum configuration for rack "
            .to_string(),
        format!("(epoch {member_epoch}, state {member_state:?})"),
    ] {
        assert!(
            message.contains(&expected),
            "message {message:?} contains {expected:?}"
        );
    }
}

#[track_caller]
fn assert_rejected_as_preparing(
    result: Result<SledPolicy, (StatusCode, String)>,
    preparing_epoch: u64,
) {
    let (status, message) = result.expect_err("expunge was rejected");
    assert_eq!(status, StatusCode::CONFLICT, "message: {message}");
    for expected in [
        format!("(epoch {preparing_epoch}, state Preparing) does not include"),
        "has not started committing and can still be aborted".to_string(),
        format!(
            "Wait for the configuration at epoch {preparing_epoch} to start \
             committing, then retry."
        ),
    ] {
        assert!(
            message.contains(&expected),
            "message {message:?} contains {expected:?}"
        );
    }
}

#[nexus_test(extra_sled_agents = 1)]
async fn test_sled_expunge_trust_quorum_gate(
    cptestctx: &ControlPlaneTestContext,
) {
    let lockstep_client = cptestctx.lockstep_client();
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let sled_id = cptestctx.second_sled_id();

    // Run an inventory collection, which will cause the sled's hw_baseboard_id
    // row to be inserted. Then it can be added to the trust quorum.
    run_inventory_collection(&cptestctx.lockstep_client).await;

    let (.., sled) = LookupPath::new(&opctx, datastore)
        .sled_id(sled_id)
        .fetch()
        .await
        .expect("looked up sled");
    let sled_baseboard = BaseboardId {
        part_number: sled.part_number().to_string(),
        serial_number: sled.serial_number().to_string(),
    };
    let tq = TrustQuorumConfigDriver {
        opctx: &opctx,
        datastore,
        rack_id: sled.rack_id(),
    };

    let initial_config = tq.config(1).await;
    assert_eq!(initial_config.state, TrustQuorumConfigState::Committed);
    let others: BTreeSet<_> = initial_config.members.keys().cloned().collect();
    assert!(!others.contains(&sled_baseboard));
    let with_sled: BTreeSet<_> =
        others.iter().cloned().chain([sled_baseboard.clone()]).collect();

    tq.propose(2, 1, &with_sled).await;
    tq.ack_all_prepares(2).await;
    tq.ack_commits(2, &with_sled, TrustQuorumConfigState::Committed).await;

    tq.propose(3, 2, &others).await;
    assert_rejected_as_preparing(
        expunge_sled(&lockstep_client, sled_id).await,
        3,
    );

    tq.abort(3).await;
    assert_rejected_as_member(
        expunge_sled(&lockstep_client, sled_id).await,
        2,
        TrustQuorumConfigState::Committed,
    );

    tq.propose(4, 2, &others).await;
    assert_rejected_as_preparing(
        expunge_sled(&lockstep_client, sled_id).await,
        4,
    );

    tq.ack_all_prepares(4).await;
    assert_eq!(
        expunge_sled(&lockstep_client, sled_id).await,
        Ok(SledPolicy::InService(SledProvisionPolicy::Provisionable)),
    );
}
