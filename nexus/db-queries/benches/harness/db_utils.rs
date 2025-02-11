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
use nexus_db_model::ByteCount;
use nexus_db_model::Generation;
use nexus_db_model::Project;
use nexus_db_model::Resources;
use nexus_db_model::Sled;
use nexus_db_model::SledBaseboard;
use nexus_db_model::SledReservationConstraintBuilder;
use nexus_db_model::SledSystemHardware;
use nexus_db_model::SledUpdate;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::external_api::params;
use omicron_common::api::external;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use std::net::Ipv6Addr;
use std::net::SocketAddrV6;
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

pub async fn create_reservation(
    opctx: &OpContext,
    db: &DataStore,
) -> Result<PropolisUuid> {
    let instance_id = InstanceUuid::new_v4();
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
                    eprintln!("Warning: Transaction aborted due to contention");
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
