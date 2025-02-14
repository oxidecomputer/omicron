// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database test helpers
//!
//! These are largely ripped out of "nexus/db-queries/src/db/datastore".
//!
//! Benchmarks are compiled as external binaries from library crates, so we
//! can only access `pub` code.
//!
//! It may be worth refactoring some of these functions to a test utility
//! crate to avoid the de-duplication.

use anyhow::Context;
use anyhow::Result;
use chrono::Utc;
use nexus_db_model::ByteCount;
use nexus_db_model::Generation;
use nexus_db_model::Instance;
use nexus_db_model::InstanceRuntimeState;
use nexus_db_model::InstanceState;
use nexus_db_model::Project;
use nexus_db_model::Resources;
use nexus_db_model::Sled;
use nexus_db_model::SledBaseboard;
use nexus_db_model::SledReservationConstraintBuilder;
use nexus_db_model::SledSystemHardware;
use nexus_db_model::SledUpdate;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::DataStore;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
use std::str::FromStr;
use uuid::Uuid;

pub async fn create_project(
    opctx: &OpContext,
    datastore: &DataStore,
) -> (authz::Project, Project) {
    let authz_silo = opctx.authn.silo_required().unwrap();

    // Create a project
    let project = Project::new(
        authz_silo.id(),
        params::ProjectCreate {
            identity: external::IdentityMetadataCreateParams {
                name: "project".parse().unwrap(),
                description: "desc".to_string(),
            },
        },
    );
    datastore.project_create(&opctx, project).await.unwrap()
}

pub fn rack_id() -> Uuid {
    Uuid::parse_str(nexus_test_utils::RACK_UUID).unwrap()
}

// Creates a "fake" Sled Baseboard.
pub fn sled_baseboard_for_test() -> SledBaseboard {
    SledBaseboard {
        serial_number: Uuid::new_v4().to_string(),
        part_number: String::from("test-part"),
        revision: 1,
    }
}

// Creates "fake" sled hardware accounting
pub fn sled_system_hardware_for_test() -> SledSystemHardware {
    SledSystemHardware {
        is_scrimlet: false,
        usable_hardware_threads: 32,
        usable_physical_ram: ByteCount::try_from(1 << 40).unwrap(),
        reservoir_size: ByteCount::try_from(1 << 39).unwrap(),
    }
}

pub fn test_new_sled_update() -> SledUpdate {
    let sled_id = Uuid::new_v4();
    let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
    let repo_depot_port = 0;
    SledUpdate::new(
        sled_id,
        addr,
        repo_depot_port,
        sled_baseboard_for_test(),
        sled_system_hardware_for_test(),
        rack_id(),
        Generation::new(),
    )
}

pub async fn create_sleds(datastore: &DataStore, count: usize) -> Vec<Sled> {
    let mut sleds = vec![];
    for _ in 0..count {
        let (sled, _) =
            datastore.sled_upsert(test_new_sled_update()).await.unwrap();
        sleds.push(sled);
    }
    sleds
}

fn small_resource_request() -> Resources {
    Resources::new(
        1,
        // Just require the bare non-zero amount of RAM.
        ByteCount::try_from(1024).unwrap(),
        ByteCount::try_from(1024).unwrap(),
    )
}

/// Given a `sled_count`, returns the number of times a call to
/// `create_reservation` should succeed.
///
/// This can be used to validate parameters before running benchmarks.
pub fn max_resource_request_count(sled_count: usize) -> usize {
    let threads_per_request: usize =
        small_resource_request().hardware_threads.0.try_into().unwrap();
    let threads_per_sled: usize = sled_system_hardware_for_test()
        .usable_hardware_threads
        .try_into()
        .unwrap();

    threads_per_sled * sled_count / threads_per_request
}

// Helper function for creating an instance without a VMM.
pub async fn create_instance_record(
    opctx: &OpContext,
    datastore: &DataStore,
    authz_project: &authz::Project,
    name: &str,
) -> InstanceUuid {
    let instance = Instance::new(
        InstanceUuid::new_v4(),
        authz_project.id(),
        &params::InstanceCreate {
            identity: external::IdentityMetadataCreateParams {
                name: name.parse().unwrap(),
                description: "".to_string(),
            },
            ncpus: 2i64.try_into().unwrap(),
            memory: external::ByteCount::from_gibibytes_u32(16).into(),
            hostname: "myhostname".try_into().unwrap(),
            user_data: Vec::new(),
            network_interfaces:
                params::InstanceNetworkInterfaceAttachment::None,
            external_ips: Vec::new(),
            disks: Vec::new(),
            boot_disk: None,
            ssh_public_keys: None,
            start: false,
            auto_restart_policy: Default::default(),
        },
    );

    let instance = datastore
        .project_create_instance(&opctx, &authz_project, instance)
        .await
        .unwrap();

    let id = InstanceUuid::from_untyped_uuid(instance.id());
    datastore
        .instance_update_runtime(
            &id,
            &InstanceRuntimeState {
                nexus_state: InstanceState::NoVmm,
                time_updated: Utc::now(),
                propolis_id: None,
                migration_id: None,
                dst_propolis_id: None,
                gen: Generation::from(Generation::new().0.next()),
                time_last_auto_restarted: None,
            },
        )
        .await
        .expect("Failed to update runtime state");

    id
}
pub async fn delete_instance_record(
    opctx: &OpContext,
    datastore: &DataStore,
    instance_id: InstanceUuid,
) {
    let (.., authz_instance) = LookupPath::new(opctx, datastore)
        .instance_id(instance_id.into_untyped_uuid())
        .lookup_for(authz::Action::Delete)
        .await
        .unwrap();
    datastore.project_delete_instance(&opctx, &authz_instance).await.unwrap();
}

pub async fn create_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    authz_project: &authz::Project,
    group_name: &'static str,
    policy: external::AffinityPolicy,
) {
    db.affinity_group_create(
        &opctx,
        &authz_project,
        nexus_db_model::AffinityGroup::new(
            authz_project.id(),
            params::AffinityGroupCreate {
                identity: external::IdentityMetadataCreateParams {
                    name: group_name.parse().unwrap(),
                    description: "desc".to_string(),
                },
                policy,
                failure_domain: external::FailureDomain::Sled,
            },
        ),
    )
    .await
    .unwrap();
}

pub async fn delete_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    group_name: &'static str,
) {
    let project = external::Name::from_str("project").unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Delete)
        .await
        .unwrap();

    db.affinity_group_delete(&opctx, &authz_group).await.unwrap();
}

pub async fn create_anti_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    authz_project: &authz::Project,
    group_name: &'static str,
    policy: external::AffinityPolicy,
) {
    db.anti_affinity_group_create(
        &opctx,
        &authz_project,
        nexus_db_model::AntiAffinityGroup::new(
            authz_project.id(),
            params::AntiAffinityGroupCreate {
                identity: external::IdentityMetadataCreateParams {
                    name: group_name.parse().unwrap(),
                    description: "desc".to_string(),
                },
                policy,
                failure_domain: external::FailureDomain::Sled,
            },
        ),
    )
    .await
    .unwrap();
}

pub async fn delete_anti_affinity_group(
    opctx: &OpContext,
    db: &DataStore,
    group_name: &'static str,
) {
    let project = external::Name::from_str("project").unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .anti_affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Delete)
        .await
        .unwrap();

    db.anti_affinity_group_delete(&opctx, &authz_group).await.unwrap();
}

pub async fn create_affinity_group_member(
    opctx: &OpContext,
    db: &DataStore,
    group_name: &'static str,
    instance_id: InstanceUuid,
) -> Result<()> {
    let project = external::Name::from_str("project").unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Modify)
        .await?;

    db.affinity_group_member_add(
        opctx,
        &authz_group,
        external::AffinityGroupMember::Instance(instance_id),
    )
    .await?;
    Ok(())
}

pub async fn create_anti_affinity_group_member(
    opctx: &OpContext,
    db: &DataStore,
    group_name: &'static str,
    instance_id: InstanceUuid,
) -> Result<()> {
    let project = external::Name::from_str("project").unwrap();
    let group = external::Name::from_str(group_name).unwrap();
    let (.., authz_group) = LookupPath::new(opctx, db)
        .project_name_owned(project.into())
        .anti_affinity_group_name_owned(group.into())
        .lookup_for(authz::Action::Modify)
        .await?;

    db.anti_affinity_group_member_add(
        opctx,
        &authz_group,
        external::AntiAffinityGroupMember::Instance(instance_id),
    )
    .await?;
    Ok(())
}

pub async fn create_reservation(
    opctx: &OpContext,
    db: &DataStore,
    instance_id: InstanceUuid,
) -> Result<PropolisUuid> {
    let vmm_id = PropolisUuid::new_v4();

    loop {
        match db
            .sled_reservation_create(
                &opctx,
                instance_id,
                vmm_id,
                small_resource_request(),
                SledReservationConstraintBuilder::new().build(),
            )
            .await
        {
            Ok(_) => break,
            Err(err) => {
                // This condition is bad - it would result in a user-visible
                // error, in most cases - but it's also an indication of failure
                // due to contention. We normally bubble this out to users,
                // rather than stalling the request, but in this particular
                // case, we choose to retry immediately.
                if err.to_string().contains("restart transaction") {
                    continue;
                }
                return Err(err).context("Failed to create reservation");
            }
        }
    }
    Ok(vmm_id)
}

pub async fn delete_reservation(
    opctx: &OpContext,
    db: &DataStore,
    vmm_id: PropolisUuid,
) -> Result<()> {
    db.sled_reservation_delete(&opctx, vmm_id)
        .await
        .context("Failed to delete reservation")?;
    Ok(())
}
