// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Packets sent from the server.

use std::fmt;
use std::time::Duration;

use crate::native::block::Block;

/// A packet sent from the ClickHouse server to the client.
#[derive(Clone, Debug, PartialEq)]
pub enum Packet {
    /// The initial handshake packet, announcing the server.
    ///
    /// The response to a client Hello packet.
    Hello(Hello),
    /// The packet contains a block of data.
    Data(Block),
    /// The packet contains one or more exceptions.
    Exception(Vec<Exception>),
    /// The packet describes the server's progress in running a query.
    Progress(Progress),
    /// The response to a client Ping packet.
    Pong,
    /// The query has completed, and all data blocks have been sent.
    EndOfStream,
    /// Profiling data for a query.
    ProfileInfo(ProfileInfo),
    /// A data block containing profiling events during a query.
    ProfileEvents(Block),
}

impl Packet {
    pub const HELLO: u8 = 0;
    pub const DATA: u8 = 1;
    pub const EXCEPTION: u8 = 2;
    pub const PROGRESS: u8 = 3;
    pub const PONG: u8 = 4;
    pub const END_OF_STREAM: u8 = 5;
    pub const PROFILE_INFO: u8 = 6;
    pub const PROFILE_EVENTS: u8 = 14;

    /// Return the kind of the packet as a string.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Packet::Hello(_) => "Hello",
            Packet::Data(_) => "Data",
            Packet::Exception(_) => "Exception",
            Packet::Progress(_) => "Progress",
            Packet::Pong => "Pong",
            Packet::EndOfStream => "EndOfStream",
            Packet::ProfileInfo(_) => "ProfileInfo",
            Packet::ProfileEvents(_) => "ProfileEvents",
        }
    }
}

/// The initial packet from the server to the client, announcing itself.
#[derive(Clone, Debug, PartialEq)]
pub struct Hello {
    /// The name of the server.
    pub name: String,
    /// The major version of the server.
    pub version_major: u64,
    /// The minor version of the server.
    pub version_minor: u64,
    /// The TCP protocol revision of the server.
    pub revision: u64,
    /// The timezone of the server.
    pub tz: String,
    /// The display name (hostname) of the server.
    pub display_name: String,
    /// The patch version of the server.
    pub version_patch: u64,
    /// Rules about password complexity.
    ///
    /// We do not use these, but they may exist and are parsed in any case.
    pub password_complexity_rules: Vec<PasswordComplexityRule>,
    /// The interserver secret the server uses when communicating with other
    /// servers. This is a nonce based on the time this packet is sent.
    pub interserver_secret: u64,
}

/// Rules about password complexity.
///
/// We don't use this, but it exists in server Hello packets.
#[derive(Clone, Debug, PartialEq)]
pub struct PasswordComplexityRule {
    pub pattern: String,
    pub exception: String,
}

/// Expected revision of the server
pub const REVISION: u64 = 54465;

/// Describes an exception the server caught during query processing.
#[derive(Clone, Debug, PartialEq)]
pub struct Exception {
    /// The integer code of the exception.
    pub code: i32,
    /// The name of the exception.
    pub name: String,
    /// A human-friendly error message attached to the exception.
    pub message: String,
    /// The stack trace of the caught exception.
    pub stack_trace: String,
    /// If true, the server exception packet contains additional exceptions.
    pub nested: bool,
}

impl fmt::Display for Exception {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, " code:     {}", self.code)?;
        writeln!(f, " name:     '{}'", self.name)?;
        writeln!(f, " message:  '{}'", self.message)?;
        writeln!(f, " stack:")?;
        if self.stack_trace.is_empty() {
            writeln!(f, "   <no stack>")?;
        } else {
            for line in self.stack_trace.lines() {
                writeln!(f, "   {line}")?;
            }
        }
        Ok(())
    }
}

impl Exception {
    /// Return a brief summary of the error as a string.
    pub fn summary(&self) -> String {
        format!("{} ({}: {})", self.message, self.code, self.name)
    }
}

/// Describes the server's progress during a query.
///
/// As the server runs large queries, it may send these periodically. They are
/// always deltas, and need to be summed on the client to understand the current
/// or total progress of the query.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Progress {
    pub rows_read: u64,
    pub bytes_read: u64,
    pub total_rows_to_read: u64,
    pub total_bytes_to_read: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
    pub query_time: Duration,
}

impl core::ops::Add for Progress {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            rows_read: self.rows_read.saturating_add(rhs.rows_read),
            bytes_read: self.bytes_read.saturating_add(rhs.bytes_read),
            total_rows_to_read: self
                .total_rows_to_read
                .saturating_add(rhs.total_rows_to_read),
            total_bytes_to_read: self
                .total_bytes_to_read
                .saturating_add(rhs.total_rows_to_read),
            rows_written: self.rows_written.saturating_add(rhs.rows_written),
            bytes_written: self.bytes_written.saturating_add(rhs.bytes_written),
            query_time: self.query_time.saturating_add(rhs.query_time),
        }
    }
}

impl core::ops::AddAssign for Progress {
    fn add_assign(&mut self, rhs: Self) {
        *self = Self {
            rows_read: self.rows_read.saturating_add(rhs.rows_read),
            bytes_read: self.bytes_read.saturating_add(rhs.bytes_read),
            total_rows_to_read: self
                .total_rows_to_read
                .saturating_add(rhs.total_rows_to_read),
            total_bytes_to_read: self
                .total_bytes_to_read
                .saturating_add(rhs.total_rows_to_read),
            rows_written: self.rows_written.saturating_add(rhs.rows_written),
            bytes_written: self.bytes_written.saturating_add(rhs.bytes_written),
            query_time: self.query_time.saturating_add(rhs.query_time),
        };
    }
}

/// Profiling information sent at the end of a query.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProfileInfo {
    /// Total number of rows accessed.
    pub n_rows: u64,
    /// Total number of blocks accessed.
    pub n_blocks: u64,
    /// Total number of blocks processed.
    pub n_bytes: u64,
    /// True if a limit was applied to the query result.
    pub applied_limit: bool,
    /// Total number of rows applied prior to the limit.
    pub rows_before_limit: u64,
    /// Whether the number of rows before limit was actually computed, e.g., to
    /// distinguish it from 0.
    pub calculated_rows_before_limit: bool,
}
