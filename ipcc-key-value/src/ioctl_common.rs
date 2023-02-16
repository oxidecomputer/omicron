// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! This module is logically part of the [`crate::ioctl`] module, but is split
//! out to be shared with the non-illumos stub module.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IpccKeyLookupError {
    #[error("IPCC key lookup ioctl failed: errno {errno}")]
    IoctlFailed { errno: i32 },
    #[error("IPCC key lookup failed: unknown key {key}")]
    UnknownKey { key: String },
    #[error("IPCC key lookup failed: no value for key")]
    NoValueForKey,
    #[error("IPCC key lookup failed: buffer too small for value")]
    BufferTooSmallForValue,
    #[error("IPCC key lookup failed: unknown result value {0}")]
    UnknownResultValue(u8),
}

#[derive(Debug, Error)]
pub enum InstallinatorImageIdError {
    #[error(transparent)]
    IpccKeyLookupError(#[from] IpccKeyLookupError),
    #[error("deserializing installinator image ID failed: {0}")]
    DeserializationFailed(String),
}

#[derive(Debug, Error)]
pub enum PingError {
    #[error(transparent)]
    IpccKeyLookupError(#[from] IpccKeyLookupError),
    #[error("unexpected reply from SP (expected `pong`: {0:?})")]
    UnexpectedReply(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
pub struct Pong;
