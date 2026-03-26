// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Low-level native client to the ClickHouse database.
//!
//! The ClickHouse server offers a number of client interfaces. We continue to
//! use JSON-over-HTTP today, because it's so easy. The server communicates with
//! the official client using what they call the "Native" protocol over TCP
//! sockets. This module implements a prototype client able to speak this
//! protocol directly.
//!
//! # Rationale
//!
//! For a long time, we've made do by talking to ClickHouse over HTTP, and
//! transferring data as JSON rows. This works, but has a number of drawbacks.
//! It incurs a lot of serialization overhead, both for the client and the
//! server. JSON is pretty verbose, and the server also needs to do a lot of
//! work to convert the native columnar formats into row-based outputs. All of
//! that goes away with the native protocol, where both peers can do little more
//! than memcpy the columns over the network.
//!
//! The HTTP interface also incurs a ot of additional _latency_. We ask the
//! server to send us metadata about the resource usage of each SQL query, such
//! as the number of rows read or written. This comes in an HTTP header, which
//! must be at the beginning of the HTTP response. However, ClickHouse normally
//! accrues these progress measures throughout a query, not just at the end. By
//! default, ClickHouse will send the first of these only! Since that's not very
//! useful for us, we ask the server to buffer the entire response, _then_ send
//! the progress metadata, and then the response itself. That means we don't get
//! any bytes from the server until it's completely done.
//!
//! Lastly, the JSON-based protocol works pretty well for inserting and
//! selecting data straight from the tables themselves. This uses the existing
//! model types in `oximeter_db::model`, which we annotate with
//! `#[serde::Serialize]` so that we can call `serde_json::to_string()` on those
//! structs. However, that only works when the data we get from the server has a
//! format or schema known at compile-time. That's not the case for OxQL, where
//! we'd like to continue to push as much of the query as possible into the
//! database. That means we need to have a dynamically-typed response. We
//! _could_ use JSON values themselves for that, but it's a good bit of work to
//! handle and we still haven't addressed the resource consumption issues.
//!
//! # Protocol
//!
//! The ClickHouse native protocol has a very bare-bones "spec", described
//! starting [here](https://clickhouse.com/docs/en/native-protocol/basics). The
//! client and server exchange "packets" with each other: for example, the
//! exchange starts with both sides sending a "Hello" packet, which includes the
//! peer's name and basic metadata.
//!
//! The protocol is described in detail
//! [here](https://github.com/ClickHouse/ClickHouse/blob/98a2c1c638c2ff9cea36e68c9ac16b3cf142387b/src/Core/Protocol.h#L11)
//! as well, though be aware that certain pieces of it are missing details or
//! incorrect. The source is is always the ultimate arbiter, and packet captures
//! can aid better understanding the format.
//!
//! The client may also initiate queries or send data in special packets. The
//! server responds to queries with "Data" packets, but has a number of other
//! [kinds](https://clickhouse.com/docs/en/native-protocol/server) as well. We
//! don't implement all these here, just those that we need as we go along.
//!
//! Packets are identified by a single byte at the start. (Technically, it's a
//! variable-length u64 encoding, but all the extant packets fit in the first
//! byte.) There is _no length_ included in the packets, or any other
//! fixed-length header. This presents a number of frustrating challenges for
//! serialization, but they're mostly invisible outside this module.
//!
//! ## Packet requests and responses
//!
//! ## Hello
//!
//! Exchanges begin with a Hello packet from the client. The server responds
//! with its own Hello packet.
//!
//! ## Ping / pong
//!
//! The client may ping the server by sending a Ping packet. The server responds
//! with Pong. Neither has any data, so consists of just a single byte.
//!
//! ## Queries
//!
//! A client may sent a Query packet to run a query. Note that the data for
//! INSERT queries is _not_ expected to be included in the query packet itself,
//! but in a following Data packet.
//!
//! The server responds with several kinds of packets.
//!
//! - First, the server sends an _empty_ Data packet. This describes the format
//! of the data to come, such as column names and types.
//!
//! - The server then sends zero or more non-empty Data packets, which contains
//! the actual results of the query. This structure matches the first, empty
//! packet.
//!
//! - During processing, the server may send Progress packets. These include the
//! amount of data read / written so far, and are _deltas_. They are accumulated
//! during a query, and their final sum is returned.
//!
//! - The server may send an Exception packet if something fails. This may
//! include one or more actual exceptions, each of which has code, message, and
//! stack trace. The query ends if one is received.
//!
//! - The server also sends two kinds of profiling packets. A ProfileInfo packet
//! contains basic information about the amount of data returned in the results.
//! For example, this contains the total number of rows the query processed,
//! which may be more than were actually sent. At the end, a ProfileEvents
//! packet is also sent. This is actually a Data block, which contains lots of
//! details about the resource consumption during the query, such as maximum
//! memory consumption, or number of readers-writer locks acquired.
//!
//! - The query ends when an EndOfStream packet has been received from the
//! server (or an exception).
//!
//! ## Canceling
//!
//! A query can be canceled by sending a Cancel client packet. This is only
//! actually sent if we believe we have an outstanding query.

pub use connection::Connection;
pub use connection::Pool;
pub use io::packet::client::Encoder;
pub use io::packet::server::Decoder;
use packets::client::Packet as ClientPacket;
pub use packets::client::QueryResult;
pub use packets::server::Exception;
use packets::server::Packet as ServerPacket;

pub mod block;
pub mod connection;
mod io;
mod packets;

#[usdt::provider(provider = "clickhouse_io")]
mod probes {

    /// Fires just before we send a packet.
    fn packet__send__start(
        addr: &str,
        kind: &str,
        packet: &crate::native::ClientPacket,
    ) {
    }

    /// Fires just after we finish sending a packet.
    fn packet__send__done(addr: &str) {}

    /// Fires when we receive a packet from the server.
    fn packet__received(
        addr: &str,
        kind: &str,
        packet: &crate::native::ServerPacket,
    ) {
    }

    /// Emitted when we learn we've been disconnected from the server.
    fn disconnected(addr: &str) {}

    /// Emitted when we receive an unrecognized packet, with the kind and the
    /// length of the discarded buffer.
    fn unrecognized__server__packet(addr: &str, kind: u64, len: usize) {}

    /// Emitted when we receive an unexpected packet, based on the messages we've
    /// sent, with the received packet type.
    fn unexpected__server__packet(addr: &str, kind: &str) {}

    /// Emitted when we receive an invalid packet, with the kind we think it is
    /// supposed to be and the length of the discarded buffer.
    fn invalid__packet(addr: &str, kind: &str, len: usize) {}
}

/// An error interacting ClickHouse over the native protocol.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    #[error("Unrecognized server packet, kind = {0}")]
    UnrecognizedServerPacket(u8),

    #[error("Invalid data packet kind = '{kind}', msg = {msg}")]
    InvalidPacket { kind: &'static str, msg: String },

    #[error("Encountered non-UTF8 string")]
    NonUtf8String,

    #[error("Unsupported data type in block: '{0}'")]
    UnsupportedDataType(String),

    #[error("TCP connection to server disconnected")]
    Disconnected,

    #[error("Unexpected server packet, expected {0}")]
    UnexpectedPacket(&'static str),

    #[error("Unrecognized ClickHouse server")]
    UnrecognizedClickHouseServer {
        name: String,
        major_version: u64,
        minor_version: u64,
        patch_version: u64,
        revision: u64,
    },

    #[error("Unsupported protocol feature: {0}")]
    UnsupportedProtocolFeature(&'static str),

    #[error(
        "Server exception: \n{}",
        .exceptions
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    )]
    Exception { exceptions: Vec<Exception> },

    #[error(
        "Mismatched data block structure when concatenating blocks or \
        inserting data blocks into the database"
    )]
    MismatchedBlockStructure,

    #[error("Value out of range for corresponding ClickHouse type")]
    OutOfRange { type_name: String, min: String, max: String, value: String },

    #[error("Failed to serialize / deserialize value from the database")]
    Serde(String),

    #[error("No column with name '{0}'")]
    NoSuchColumn(String),

    #[error("Too many rows to create block")]
    TooManyRows,

    #[error(
        "Column '{name}' was expected to have type '{expected}', \
        but it actually has type '{actual}'"
    )]
    UnexpectedColumnType { name: String, expected: String, actual: String },

    #[error("Data block is too large")]
    BlockTooLarge,

    #[error("Expected an empty data block")]
    ExpectedEmptyDataBlock,

    #[error(
        "A query unexpectedly resulted in an empty data block; query: {query}"
    )]
    UnexpectedEmptyBlock { query: String },
}

impl Error {
    pub(crate) fn unexpected_column_type(
        block: &block::Block,
        name: &str,
        expected: impl std::fmt::Display,
    ) -> Self {
        Error::UnexpectedColumnType {
            name: name.to_string(),
            expected: expected.to_string(),
            actual: block
                .columns
                .get(name)
                .map(|col| col.data_type.to_string())
                .unwrap_or_else(|| String::from("unknown")),
        }
    }
}

/// Error codes and related constants.
///
/// See `ClickHouse/src/Common/ErrorCodes.cpp` for reference.
pub mod errors {
    pub const UNKNOWN_TABLE: i32 = 60;
    pub const UNKNOWN_DATABASE: i32 = 81;
}
