// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces for Nexus under test.
//!
//! By splitting these interfaces into a new crate, we can avoid a circular
//! dependency on Nexus during testing.
//!
//! Both Nexus unit tests and Integration tests want to able to share
//! utilities for launching Nexus, a multi-service setup process that exists in
//! `nexus-test-utils`.
//!
//! Without a separate test interface crate, this dependency looks like the
//! following (note: "->" means "depends on")
//!
//! - nexus -> nexus-test-utils
//! - nexus-test-utils -> nexus
//! - integration tests -> nexus
//! - integration tests -> nexus-test-utils
//!
//! As we can see, this introduces a circular dependency between
//! `nexus-test-utils` and `nexus`.
//!
//! However, by separating out the portion of `nexus` used by `nexus-test-utils`
//! into a separate trait, we can break the circular dependency:
//!
//! - nexus -> nexus-test-interface
//! - nexus -> nexus-test-utils
//! - nexus-test-utils -> nexus-test-interface
//! - integration tests -> nexus
//! - integration tests -> nexus-test-utils

use async_trait::async_trait;
use omicron_common::nexus_config::Config;
use slog::Logger;
use std::net::{SocketAddr, SocketAddrV6};
use uuid::Uuid;

#[async_trait]
pub trait NexusServer {
    type InternalServer;

    async fn start_internal(
        config: &Config,
        log: &Logger,
    ) -> (Self::InternalServer, SocketAddr);

    async fn start(
        internal_server: Self::InternalServer,
        config: &Config,
        services: Vec<nexus_types::internal_api::params::ServicePutRequest>,
        external_dns_zone_name: &str,
        recovery_silo: nexus_types::internal_api::params::RecoverySiloConfig,
    ) -> Self;

    async fn get_http_server_external_address(&self) -> Option<SocketAddr>;
    async fn get_https_server_external_address(&self) -> Option<SocketAddr>;
    async fn get_http_server_internal_address(&self) -> SocketAddr;

    async fn set_resolver(&self, resolver: internal_dns::resolver::Resolver);

    // Previously, as a dataset was created (within the sled agent),
    // we'd use an internal API from Nexus to record that the dataset
    // now exists. In other words, Sled Agent was in control, by telling
    // Nexus when it should record persistent information about datasets.
    //
    // However, as of https://github.com/oxidecomputer/omicron/pull/1954,
    // control over dataset provisioning is shifting to Nexus. There is
    // a short window where RSS controls dataset provisioning, but afterwards,
    // Nexus should be calling the shots on "when to provision datasets".
    //
    // For test purposes, we have many situations where we want to carve up
    // zpools and datasets precisely for disk-based tests. As a result, we
    // *want* tests (namely, an entity outside of Nexus) to have this control.
    //
    // This test-based API provides one such mechanism of control.
    //
    // TODO: In the future, we *could* re-structure our tests to more rigorously
    // use the "RackInitializationRequest" handoff, but this would require
    // creating all our Zpools and Datasets before performing handoff to Nexus.
    // However, doing so would let us remove this test-only API.
    async fn upsert_crucible_dataset(
        &self,
        id: Uuid,
        zpool_id: Uuid,
        address: SocketAddrV6,
    );

    async fn close(self);
}
