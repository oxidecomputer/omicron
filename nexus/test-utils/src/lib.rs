// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration testing facilities for Nexus

use nexus_test_interface::NexusServer;
use omicron_common::api::external::IdentityMetadata;
use omicron_sled_agent::sim;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use omicron_uuid_kinds::GenericUuid;
use std::fmt::Debug;
use std::net::Ipv6Addr;
use std::time::Duration;
use uuid::Uuid;

pub use sim::TEST_HARDWARE_THREADS;
pub use sim::TEST_RESERVOIR_RAM;

pub mod background;
pub mod db;
pub mod http_testing;
mod nexus_test;
pub mod resource_helpers;
pub mod sql;
mod starter;

pub use nexus_test::ControlPlaneTestContext;
pub use nexus_test::load_test_config;
#[cfg(feature = "omicron-dev")]
pub use nexus_test::omicron_dev_setup_with_config;
pub use nexus_test::test_setup;
pub use nexus_test::test_setup_with_config;
pub use starter::ControlPlaneStarter;
pub use starter::ControlPlaneTestContextSledAgent;
pub use starter::register_test_producer;
pub use starter::start_oximeter;
pub use starter::start_producer_server;
pub use starter::start_sled_agent;
pub use starter::start_sled_agent_with_config;

pub const SLED_AGENT_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
pub const SLED_AGENT2_UUID: &str = "039be560-54cc-49e3-88df-1a29dadbf913";
pub const RACK_UUID: &str = nexus_db_queries::db::pub_test_utils::RACK_UUID;
pub const SWITCH_UUID: &str = "dae4e1f1-410e-4314-bff1-fec0504be07e";
pub const PHYSICAL_DISK_UUID: &str = "fbf4e1f1-410e-4314-bff1-fec0504be07e";
pub const OXIMETER_UUID: &str = "39e6175b-4df2-4730-b11d-cbc1e60a2e78";
pub const PRODUCER_UUID: &str = "a6458b7d-87c3-4483-be96-854d814c20de";
pub const RACK_SUBNET: &str = "fd00:1122:3344:0100::/56";

/// Password for the user created by the test suite
///
/// This is only used by the test suite and `omicron-dev run-all` (the latter of
/// which uses the test suite setup code for most of its operation).   These are
/// both transient deployments with no sensitive data.
pub const TEST_SUITE_PASSWORD: &str = "oxide";

/// Returns whether the two identity metadata objects are identical.
pub fn identity_eq(ident1: &IdentityMetadata, ident2: &IdentityMetadata) {
    assert_eq!(ident1.id, ident2.id);
    assert_eq!(ident1.name, ident2.name);
    assert_eq!(ident1.description, ident2.description);
    assert_eq!(ident1.time_created, ident2.time_created);
    assert_eq!(ident1.time_modified, ident2.time_modified);
}

/// Order-agnostic vec equality
pub fn assert_same_items<T: PartialEq + Debug>(v1: Vec<T>, v2: Vec<T>) {
    assert_eq!(v1.len(), v2.len(), "{:?} and {:?} don't match", v1, v2);
    for item in v1.iter() {
        assert!(v2.contains(item), "{:?} and {:?} don't match", v1, v2);
    }
}

/// Wait until a producer is registered with Oximeter.
///
/// This blocks until the producer is registered, for up to 60s. It panics if
/// the retry loop hits a permanent error.
pub async fn wait_for_producer<G: GenericUuid>(
    oximeter: &oximeter_collector::Oximeter,
    producer_id: G,
) {
    wait_for_producer_impl(oximeter, producer_id.into_untyped_uuid()).await;
}

// This function is outlined from wait_for_producer to avoid unnecessary
// monomorphization.
async fn wait_for_producer_impl(
    oximeter: &oximeter_collector::Oximeter,
    producer_id: Uuid,
) {
    wait_for_condition(
        || async {
            if oximeter
                .list_producers(None, usize::MAX)
                .iter()
                .any(|p| p.id == producer_id)
            {
                Ok(())
            } else {
                Err(CondCheckError::<()>::NotYet)
            }
        },
        &Duration::from_secs(1),
        &Duration::from_secs(60),
    )
    .await
    .expect("Failed to find producer within time limit");
}

/// Build a DPD client for test validation using the first running dendrite instance
pub fn dpd_client<N: NexusServer>(
    cptestctx: &ControlPlaneTestContext<N>,
) -> dpd_client::Client {
    // Get the first available dendrite instance and extract the values we need
    let dendrite_guard = cptestctx.dendrite.read().unwrap();
    let (switch_location, dendrite_instance) = dendrite_guard
        .iter()
        .next()
        .expect("No dendrite instances running for test");

    // Copy the values we need while the guard is still alive
    let switch_location = *switch_location;
    let port = dendrite_instance.port;
    drop(dendrite_guard);

    let client_state = dpd_client::ClientState {
        tag: String::from("nexus-test"),
        log: cptestctx.logctx.log.new(slog::o!(
            "component" => "DpdClient",
            "switch" => switch_location.to_string()
        )),
    };

    let addr = Ipv6Addr::LOCALHOST;
    dpd_client::Client::new(&format!("http://[{addr}]:{port}"), client_state)
}
