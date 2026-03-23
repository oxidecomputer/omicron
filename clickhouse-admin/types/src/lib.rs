// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for the ClickHouse Admin APIs.
//!
//! This crate re-exports types from `clickhouse-admin-types-versions` for use
//! in business logic. For versioned type definitions and API version
//! conversions, see the versions crate directly.

pub mod config;
pub mod keeper;
pub mod server;

// Constants for file paths - not API-published
pub const CLICKHOUSE_SERVER_CONFIG_DIR: &str =
    "/opt/oxide/clickhouse_server/config.d";
pub const CLICKHOUSE_SERVER_CONFIG_FILE: &str = "replica-server-config.xml";
pub const CLICKHOUSE_KEEPER_CONFIG_DIR: &str = "/opt/oxide/clickhouse_keeper";
pub const CLICKHOUSE_KEEPER_CONFIG_FILE: &str = "keeper_config.xml";
pub const OXIMETER_CLUSTER: &str = "oximeter_cluster";
