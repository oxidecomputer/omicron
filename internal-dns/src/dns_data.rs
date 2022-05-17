// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manages DNS data (configured zone(s), records, etc.)

use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{error, info, o, trace};
use std::net::Ipv6Addr;
use std::sync::Arc;

/// Configuration related to data model
#[derive(Deserialize, Debug)]
pub struct Config {
    /// maximum number of channel messages to buffer
    pub nmax_messages: usize,

    /// The path for the embedded kv store
    pub storage_path: String,
}

/// default maximum number of messages to buffer
const NMAX_MESSAGES_DEFAULT: usize = 16;

impl Default for Config {
    fn default() -> Self {
        Config {
            nmax_messages: NMAX_MESSAGES_DEFAULT,
            storage_path: ".".into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename = "Srv")]
pub struct SRV {
    pub prio: u16,
    pub weight: u16,
    pub port: u16,
    pub target: String,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "data")]
pub enum DnsRecord {
    AAAA(Ipv6Addr),
    SRV(SRV),
}
#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct DnsRecordKey {
    name: String,
}
#[derive(Debug)]
pub struct DnsResponse<T> {
    tx: tokio::sync::oneshot::Sender<T>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename = "DnsKv")]
pub struct DnsKV {
    key: DnsRecordKey,
    records: Vec<DnsRecord>,
}

// XXX some refactors to help
// - each variant should have its own struct containing the data.  This way we
//   can pass it to functions as a bundle without them having to consume the
//   whole enum (which might in principle be a different variant)
// - each variant's data should include some generic responder<T> so that we can
//   have common functions for logging and sending the T
#[derive(Debug)]
pub enum DnsCmd {
    // XXX
    // MakeExist(DnsRecord, DnsResponse<()>),
    // MakeGone(DnsRecordKey, DnsResponse<()>),
    Get(Option<DnsRecordKey>, DnsResponse<Vec<DnsKV>>),
    Set(Vec<DnsKV>, DnsResponse<()>),
    Delete(Vec<DnsRecordKey>, DnsResponse<()>),
}

/// Data model client
///
/// The Dropshot server has one of these to send commands to modify and update
/// the data model.
pub struct Client {
    log: slog::Logger,
    sender: tokio::sync::mpsc::Sender<DnsCmd>,
}

impl Client {
    pub fn new(
        log: slog::Logger,
        config: &Config,
        db: Arc<sled::Db>,
    ) -> Client {
        let (sender, receiver) =
            tokio::sync::mpsc::channel(config.nmax_messages);
        let server = Server {
            log: log.new(o!("component" => "DataServer")),
            receiver,
            db,
        };
        tokio::spawn(async move { data_server(server).await });
        Client { log, sender }
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub async fn get_records(
        &self,
        key: Option<DnsRecordKey>,
    ) -> Result<Vec<DnsKV>, anyhow::Error> {
        slog::trace!(&self.log, "get_records"; "key" => ?key);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(DnsCmd::Get(key, DnsResponse { tx }))
            .context("send message")?;
        rx.await.context("recv response")
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub async fn set_records(
        &self,
        records: Vec<DnsKV>,
    ) -> Result<(), anyhow::Error> {
        slog::trace!(&self.log, "set_records"; "records" => ?records);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(DnsCmd::Set(records, DnsResponse { tx }))
            .context("send message")?;
        rx.await.context("recv response")
    }

    // XXX error type needs to be rich enough for appropriate HTTP response
    pub async fn delete_records(
        &self,
        records: Vec<DnsRecordKey>,
    ) -> Result<(), anyhow::Error> {
        slog::trace!(&self.log, "delete_records"; "records" => ?records);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(DnsCmd::Delete(records, DnsResponse { tx }))
            .context("send message")?;
        rx.await.context("recv response")
    }
}

/// Runs the body of the data model server event loop
async fn data_server(mut server: Server) {
    let log = &server.log;
    loop {
        trace!(log, "waiting for message");
        let msg = match server.receiver.recv().await {
            None => {
                info!(log, "exiting due to channel close");
                break;
            }
            Some(m) => m,
        };

        trace!(log, "rx message"; "message" => ?msg);
        match msg {
            DnsCmd::Get(key, response) => {
                server.cmd_get_records(key, response).await;
            }
            DnsCmd::Set(records, response) => {
                server.cmd_set_records(records, response).await;
            }
            DnsCmd::Delete(records, response) => {
                server.cmd_delete_records(records, response).await;
            }
        }
    }
}

/// Data model server
pub struct Server {
    log: slog::Logger,
    receiver: tokio::sync::mpsc::Receiver<DnsCmd>,
    db: Arc<sled::Db>,
}

impl Server {
    async fn cmd_get_records(
        &self,
        key: Option<DnsRecordKey>,
        response: DnsResponse<Vec<DnsKV>>,
    ) {
        // If a key is provided search just for that key. Otherwise return all
        // the db entries.
        if let Some(key) = key {
            let bits = match self.db.get(key.name.as_bytes()) {
                Ok(Some(bits)) => bits,
                _ => {
                    match response.tx.send(Vec::new()) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(self.log, "response tx: {:?}", e);
                        }
                    }
                    return;
                }
            };
            let records: Vec<DnsRecord> =
                match serde_json::from_slice(bits.as_ref()) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(self.log, "deserialize record: {}", e);
                        match response.tx.send(Vec::new()) {
                            Ok(_) => {}
                            Err(e) => {
                                error!(self.log, "response tx: {:?}", e);
                            }
                        }
                        return;
                    }
                };
            match response.tx.send(vec![DnsKV { key, records }]) {
                Ok(_) => {}
                Err(e) => {
                    error!(self.log, "response tx: {:?}", e);
                }
            }
        } else {
            let mut result = Vec::new();
            let mut iter = self.db.iter();
            loop {
                match iter.next() {
                    Some(Ok((k, v))) => {
                        let records: Vec<DnsRecord> =
                            match serde_json::from_slice(v.as_ref()) {
                                Ok(r) => r,
                                Err(e) => {
                                    error!(
                                        self.log,
                                        "deserialize record: {}", e
                                    );
                                    match response.tx.send(Vec::new()) {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!(
                                                self.log,
                                                "response tx: {:?}", e
                                            );
                                        }
                                    }
                                    return;
                                }
                            };
                        let key = match std::str::from_utf8(k.as_ref()) {
                            Ok(s) => s.to_string(),
                            Err(e) => {
                                error!(self.log, "key encoding: {}", e);
                                match response.tx.send(Vec::new()) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!(
                                            self.log,
                                            "response tx: {:?}", e
                                        );
                                    }
                                }
                                return;
                            }
                        };
                        result.push(DnsKV {
                            key: DnsRecordKey { name: key },
                            records,
                        });
                    }
                    Some(Err(e)) => {
                        error!(self.log, "db iteration error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            match response.tx.send(result) {
                Ok(_) => {}
                Err(e) => {
                    error!(self.log, "response tx: {:?}", e);
                }
            }
        }
    }

    async fn cmd_set_records(
        &self,
        records: Vec<DnsKV>,
        response: DnsResponse<()>,
    ) {
        for kv in records {
            let bits = match serde_json::to_string(&kv.records) {
                Ok(bits) => bits,
                Err(e) => {
                    error!(self.log, "serialize record: {}", e);
                    match response.tx.send(()) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(self.log, "response tx: {:?}", e);
                        }
                    }
                    return;
                }
            };
            match self.db.insert(kv.key.name.as_bytes(), bits.as_bytes()) {
                Ok(_) => {}
                Err(e) => {
                    error!(self.log, "db insert: {}", e);
                    match response.tx.send(()) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(self.log, "response tx: {:?}", e);
                        }
                    }
                    return;
                }
            }
        }
        match response.tx.send(()) {
            Ok(_) => {}
            Err(e) => {
                error!(self.log, "response tx: {:?}", e);
            }
        }
    }

    async fn cmd_delete_records(
        &self,
        records: Vec<DnsRecordKey>,
        response: DnsResponse<()>,
    ) {
        for k in records {
            match self.db.remove(k.name.as_bytes()) {
                Ok(_) => {}
                Err(e) => {
                    error!(self.log, "db delete: {}", e);
                    match response.tx.send(()) {
                        Ok(_) => {}
                        Err(e) => {
                            error!(self.log, "response tx: {:?}", e);
                        }
                    }
                    return;
                }
            }
        }
        match response.tx.send(()) {
            Ok(_) => {}
            Err(e) => {
                error!(self.log, "response tx: {:?}", e);
            }
        }
    }
}
