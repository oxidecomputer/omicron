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
use std::net::SocketAddr;

#[async_trait]
pub trait NexusServer {
    async fn start_and_populate(config: &Config, log: &Logger) -> Self;

    fn get_http_servers_external(&self) -> Vec<SocketAddr>;
    fn get_http_server_internal(&self) -> SocketAddr;

    async fn set_resolver(
        &self,
        resolver: internal_dns_client::multiclient::Resolver,
    );
    async fn close(self);
}
