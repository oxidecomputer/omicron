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
use nexus_config::NexusConfig;
use nexus_types::deployment::Blueprint;
use nexus_types::internal_api::params::{
    PhysicalDiskPutRequest, ZpoolPutRequest,
};
use nexus_types::inventory::Collection;
use omicron_common::api::external::Error;
use slog::Logger;
use std::net::{SocketAddr, SocketAddrV6};
use uuid::Uuid;

#[async_trait]
pub trait NexusServer: Send + Sync + 'static {
    type InternalServer: Send + Sync + 'static;

    async fn start_internal(
        config: &NexusConfig,
        log: &Logger,
    ) -> (Self::InternalServer, SocketAddr);

    #[allow(clippy::too_many_arguments)]
    async fn start(
        internal_server: Self::InternalServer,
        config: &NexusConfig,
        blueprint: Blueprint,
        physical_disks: Vec<PhysicalDiskPutRequest>,
        zpools: Vec<nexus_types::internal_api::params::ZpoolPutRequest>,
        datasets: Vec<nexus_types::internal_api::params::DatasetCreateRequest>,
        internal_dns_config: nexus_types::internal_api::params::DnsConfigParams,
        external_dns_zone_name: &str,
        recovery_silo: nexus_types::internal_api::params::RecoverySiloConfig,
        tls_certificates: Vec<nexus_types::internal_api::params::Certificate>,
        disable_sled_id: Uuid,
    ) -> Self;

    async fn get_http_server_external_address(&self) -> SocketAddr;
    async fn get_http_server_techport_address(&self) -> SocketAddr;
    async fn get_http_server_internal_address(&self) -> SocketAddr;

    // Previously, as a dataset was created (within the sled agent),
    // we'd use an internal API from Nexus to record that the dataset
    // now exists. In other words, Sled Agent was in control, by telling
    // Nexus when it should record persistent information about datasets.
    //
    // However, as of https://github.com/oxidecomputer/omicron/pull/1954,
    // control over dataset provisioning is shifting to Nexus. There is
    // a short window where RSS controls dataset provisioning, but afterwards,
    // Nexus should be calling the shots on "when to provision datasets".
    // Furthermore, with https://github.com/oxidecomputer/omicron/pull/5172,
    // physical disk and zpool provisioning has already moved into Nexus. This
    // provides a "back-door" for tests to control the set of control plane
    // disks that are considered active.
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
        physical_disk: PhysicalDiskPutRequest,
        zpool: ZpoolPutRequest,
        dataset_id: Uuid,
        address: SocketAddrV6,
    );

    async fn inventory_collect_and_get_latest_collection(
        &self,
    ) -> Result<Option<Collection>, Error>;

    async fn close(self);
}
