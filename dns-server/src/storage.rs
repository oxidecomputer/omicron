// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages DNS data (configured zone(s), records, etc.)
// XXX-dap design doc -- why no channel / separate task?  why we might consider
// it?

use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{error, info, o, trace};
use std::net::Ipv6Addr;
use std::sync::Arc;
// XXX-dap
use crate::dns_types::*;

/// Configuration related to persistent DNS data storage
#[derive(Deserialize, Debug)]
pub struct Config {
    /// The path for the embedded kv store
    pub storage_path: String,
}

// XXX-dap document
/// Client for persistent DNS data storage
pub struct Client {
    log: slog::Logger,
    db: Arc<sled::Db>,
}

impl Client {
    pub fn new(
        log: slog::Logger,
        db: Arc<sled::Db>,
    ) -> Client {
        Client { log, db }
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub async fn records_for_key(
        &self,
        key: DnsRecordKey,
    ) -> Result<Vec<DnsKV>, anyhow::Error> {
        // XXX-dap log, atomically replace everything
        todo!();
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub async fn dns_config(
        &self,
    ) -> Result<DnsConfig, anyhow::Error> {
        // XXX-dap log, fetch everything, assemble result
        todo!();
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub async fn dns_config_update(
        &self,
        config: &DnsConfig,
    ) -> Result<(), anyhow::Error> {
        // XXX-dap log, atomically replace everything
        todo!();
    }
}
