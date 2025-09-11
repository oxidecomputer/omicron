// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Packets sent from client to server.

use super::server::ProfileInfo;
use super::server::Progress;
use crate::QuerySummary;
use crate::native::block::Block;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::LazyLock;
use uuid::Uuid;

/// A packet sent from client to server in the native protocol.
#[derive(Clone, Debug, serde::Serialize)]
#[allow(dead_code)]
pub enum Packet {
    /// The initial packet to the server, to say hello.
    Hello(Box<Hello>),
    /// Send a query to the server.
    Query(Box<Query>),
    /// Send a block of data to the server.
    Data(Block),
    /// Cancel the current query.
    Cancel,
    /// Ping the server.
    Ping,
}

impl Packet {
    pub const HELLO: u8 = 0;
    pub const QUERY: u8 = 1;
    pub const DATA: u8 = 2;
    pub const CANCEL: u8 = 3;
    pub const PING: u8 = 4;

    /// Return the kind of this packet, as a string.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Packet::Hello(_) => "Hello",
            Packet::Query(_) => "Query",
            Packet::Data(_) => "Data",
            Packet::Cancel => "Cancel",
            Packet::Ping => "Ping",
        }
    }
}

/// The initial packet sent from client to server after connecting.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Hello {
    /// The name of the client.
    pub client_name: Cow<'static, str>,
    /// Major version of the client
    pub version_major: u64,
    /// Minor version of the client
    pub version_minor: u64,
    /// Native protocol version of the client
    pub protocol_version: u64,
    /// Name of the default database.
    pub database: Cow<'static, str>,
    /// Client username
    pub username: Cow<'static, str>,
    /// Client password
    pub password: Cow<'static, str>,
}

pub const CLIENT_NAME: Cow<'static, str> = Cow::Borrowed("oximeter");
pub const VERSION_MAJOR: u64 = 23;
pub const VERSION_MINOR: u64 = 8;
pub const VERSION_PATCH: u64 = 7;
pub const PROTOCOL_VERSION: u64 = super::server::REVISION;
// NOTE: We would like to use the default database as "oximeter" to make things
// easier. However, ClickHouse rejects connection requests if the default
// database doesn't exist, so we just use the real "default" and always
// fully-qualify table names with the database in queries.
const DATABASE: Cow<'static, str> = Cow::Borrowed("default");
const USERNAME: Cow<'static, str> = Cow::Borrowed("default");
const PASSWORD: Cow<'static, str> = Cow::Borrowed("");

/// Static hello packet sent from the oximeter client.
pub static OXIMETER_HELLO: Hello = Hello {
    client_name: CLIENT_NAME,
    version_major: VERSION_MAJOR,
    version_minor: VERSION_MINOR,
    protocol_version: PROTOCOL_VERSION,
    database: DATABASE,
    username: USERNAME,
    password: PASSWORD,
};

/// A query sent from the client.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Query {
    /// An ID for the query. In our case, these are always UUIDs.
    pub id: Cow<'static, str>,
    /// Information about the client.
    pub client_info: ClientInfo,
    /// Per-query settings.
    pub settings: Settings,
    /// A client-server secret.
    pub secret: Cow<'static, str>,
    /// The stage through which the query should be run.
    pub stage: Stage,
    /// Compress...what? This data, the result data?
    pub compression: u64,
    /// The raw query string itself.
    pub body: Cow<'static, str>,
}

impl Query {
    pub fn new(id: Uuid, address: SocketAddr, query: &str) -> Self {
        Self {
            id: id.to_string().into(),
            client_info: ClientInfo::new(id.to_string(), address),
            settings: Settings::new(),
            secret: "".into(),
            stage: Stage::Complete,
            compression: 0,
            body: query.to_string().into(),
        }
    }
}

/// The result of a SQL query.
#[derive(Clone, Debug)]
pub struct QueryResult {
    /// The ID of the query.
    pub id: Uuid,
    /// The raw SQL query.
    pub query: String,
    /// The accumulated query progress for the whole query.
    pub progress: Progress,
    /// Any data returned by the query.
    ///
    /// For insert queries or DDL, this may be None. For any select query, all
    /// blocks are concatenated into one.
    pub data: Option<Block>,
    /// Profiling information from the server.
    pub profile_info: Option<ProfileInfo>,
    /// Additional data describing resource usage during the query.
    pub profile_events: Option<Block>,
}

impl QueryResult {
    /// Return a query summary from the full query result.
    pub fn query_summary(&self) -> QuerySummary {
        QuerySummary {
            id: self.id,
            query: self.query.clone(),
            elapsed: self.progress.query_time,
            io_summary: self.progress.into(),
        }
    }
}

/// The stage through which we run a query.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[allow(dead_code)]
pub enum Stage {
    /// Fetch the column names and types resulting from the query.
    FetchColumns,
    /// Run the query to completion on each server, but do not combine them.
    WithMergeableState,
    /// Run the query to completion.
    ///
    /// This is almost always what we use.
    Complete,
}

/// The details about the client, sent in each query.
// Also mostly static data, no otel.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ClientInfo {
    /// Who initiated the query?
    pub query_kind: QueryKind,
    /// The user name, if the query came from an external client.
    pub initial_user: Cow<'static, str>,
    /// The query ID, if the query came from an external client.
    pub initial_query_id: Cow<'static, str>,
    /// The client socket address, if the query came from an external client.
    pub initial_address: Cow<'static, str>,
    /// The query start time, in microseconds, if the query came from an
    /// external client.
    pub initial_time: i64,
    /// Which interface the query came over. This is always 1, for TCP.
    pub interface: u8,
    /// Name of the user on the OS.
    pub os_user: Cow<'static, str>,
    /// Hostname of the client machine.
    pub client_hostname: Cow<'static, str>,
    /// The name of the client.
    pub client_name: Cow<'static, str>,
    /// Major version of the client.
    pub version_major: u64,
    /// Minor version of the client.
    pub version_minor: u64,
    /// TCP protocol version of the client.
    pub protocol_version: u64,
    /// A key used to apply quotas.
    pub quota_key: Cow<'static, str>,
    /// Limit to recursive queries on Distributed tables.
    pub distributed_depth: u64,
    /// Patch version of the client.
    pub version_patch: u64,
    /// Optional OpenTelemetry tracing state.
    ///
    /// We don't use this now.
    pub otel_state: Option<OtelState>,
}

static OS_USER: LazyLock<String> = LazyLock::new(|| {
    let maybe_name = unsafe { libc::getlogin() };
    if maybe_name.is_null() {
        return String::from("unknown");
    }
    unsafe { std::ffi::CStr::from_ptr(maybe_name.cast()) }
        .to_str()
        .map(|s| String::from(s))
        .unwrap_or_else(|_| String::from("unknown"))
});

static CLIENT_HOSTNAME: LazyLock<String> = LazyLock::new(|| {
    gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| String::from("unknown"))
});

impl ClientInfo {
    fn new(id: String, address: SocketAddr) -> Self {
        Self {
            query_kind: QueryKind::Initial,
            initial_user: USERNAME,
            initial_query_id: id.into(),
            initial_address: address.to_string().into(),
            initial_time: 0,
            interface: TCP_INTERFACE_KIND,
            os_user: Cow::from(OS_USER.clone()),
            client_hostname: Cow::from(CLIENT_HOSTNAME.clone()),
            client_name: CLIENT_NAME,
            version_major: VERSION_MAJOR,
            version_minor: VERSION_MINOR,
            protocol_version: PROTOCOL_VERSION,
            quota_key: Cow::from(""),
            distributed_depth: 0,
            version_patch: VERSION_PATCH,
            otel_state: None,
        }
    }
}

/// We always use TCP to talk to the server.
pub const TCP_INTERFACE_KIND: u8 = 1;

#[derive(Debug, Clone, serde::Serialize)]
pub struct OtelState {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 16],
    pub trace_state: Cow<'static, str>,
    pub trace_flags: u8,
}

/// Indicates who initiated the query.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[allow(dead_code)]
pub enum QueryKind {
    /// Default, unused value.
    None,
    /// This query was initiated by a client itself.
    Initial,
    /// This query was initiated by another server, possibly on behalf of an
    /// external client.
    Secondary,
}

/// Per-query settings.
///
/// This is just a map of strings-to-strings, and is almost always empty.
pub type Settings = BTreeMap<Cow<'static, str>, Setting>;

/// A single setting in the settings map.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Setting {
    /// The value for the setting.
    pub value: Cow<'static, str>,
    /// If false, the server may choose to ignore the setting in some
    /// situations.
    pub important: bool,
}
