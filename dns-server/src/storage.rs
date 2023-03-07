// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages DNS data (configured zone(s), records, etc.)
// XXX-dap design doc -- why no channel / separate task?  why we might consider
// it?

use anyhow::Context;
use camino::Utf8PathBuf;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{error, info, o, trace};
use std::net::Ipv6Addr;
use std::sync::Arc;
// XXX-dap
use crate::dns_types::*;

/// Configuration for persistent storage of DNS data
#[derive(Deserialize, Debug)]
pub struct Config {
    /// The path for the embedded "sled" kv store
    pub storage_path: Utf8PathBuf,
}

/// Encapsulates persistent storage of DNS data
#[derive(Clone)]
pub struct Store {
    log: slog::Logger,
    db: Arc<sled::Db>,
}

impl Store {
    pub fn new(
        log: slog::Logger,
        config: &Config,
    ) -> Result<Self, anyhow::Error> {
        let db = sled::open(&config.storage_path).with_context(|| {
            format!("open DNS database {:?}", &config.storage_path)
        })?;

        Self::new_with_db(log, Arc::new(db));
    }

    pub fn new_with_db(log: slog::Logger, db: Arc<sled::Db>) -> Store {
        Store { log, db }
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub(crate) async fn dns_config(&self) -> Result<DnsConfig, anyhow::Error> {
        // XXX-dap log, fetch everything, assemble result
        todo!();
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub(crate) async fn dns_config_update(
        &self,
        config: &DnsConfig,
    ) -> Result<(), anyhow::Error> {
        // XXX-dap log, atomically replace everything
        todo!();
    }

    pub(crate) async fn query(
        &self,
        query: trust_dns_server::authority::MessageRequest,
    ) -> Result<Vec<DnsRecord>, QueryError> {
        // XXX-dap
        todo!();

        //    // Ensure the query is for one of the zones that we're operating.
        //    // Otherwise, bail with servfail. This will cause resolvers to look to other
        //    // DNS servers for this query.
        //    let name = mr.query().name();
        //    if !zone.zone_of(name) {
        //        nack(&log, &mr, &socket, &header, &src).await;
        //        return;
        //    }
        //
        //    let name = mr.query().original().name().clone();
        //    let key = name.to_string();
        //    let key = key.trim_end_matches('.');
        //
        //    let bits = match db.get(key.as_bytes()) {
        //        Ok(Some(bits)) => bits,
        //
        //        // If no record is found bail with NXDOMAIN.
        //        Ok(None) => {
        //            respond_nxdomain(&log, socket, src, rb, header, &mr).await;
        //            return;
        //        }
        //
        //        // If we encountered an error bail with SERVFAIL.
        //        Err(e) => {
        //            error!(log, "db get: {}", e);
        //            nack(&log, &mr, &socket, &header, &src).await;
        //            return;
        //        }
        //    };
        //
        //    let records: Vec<crate::dns_types::DnsRecord> =
        //        match serde_json::from_slice(bits.as_ref()) {
        //            Ok(r) => r,
        //            Err(e) => {
        //                error!(log, "deserialize record: {}", e);
        //                nack(&log, &mr, &socket, &header, &src).await;
        //                return;
        //            }
        //        };
        //
        //    if records.is_empty() {
        //        error!(log, "No records found for {}", key);
        //        respond_nxdomain(&log, socket, src, rb, header, &mr).await;
        //        return;
        //    }
    }
}

#[derive(Debug, Error)]
pub(crate) enum QueryError {
    #[error("server is not authoritative for zone: {0:?}")]
    NoZone(String),

    #[error("no records found for name: {0:?}")]
    NoName(String),

    #[error("failed to query database")]
    QueryFail(#[source] anyhow::Error),

    #[error("failed to parse database result")]
    ParseFail(#[source] anyhow::Error),
}
