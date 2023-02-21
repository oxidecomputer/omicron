// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! This module contains types shared between the real (illumos-only)
//! `crate::ioctl` module and the generic `crate::ioctl_stub` module.

use std::io;
use thiserror::Error;

/// IPCC keys; the source of truth for these is RFD 316 + the
/// `host-sp-messages` crate in hubris.
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum IpccKey {
    Ping = 0,
    InstallinatorImageId = 1,
}

#[derive(Debug, Error)]
pub enum IpccKeyLookupError {
    #[error("IPCC key lookup ioctl failed for key {key:?}: {error}")]
    IoctlFailed { key: IpccKey, error: io::Error },
    #[error("IPCC key lookup failed for key {key:?}: unknown key")]
    UnknownKey { key: IpccKey },
    #[error("IPCC key lookup failed for key {key:?}: no value for key")]
    NoValueForKey { key: IpccKey },
    #[error(
        "IPCC key lookup failed for key {key:?}: buffer too small for value"
    )]
    BufferTooSmallForValue { key: IpccKey },
    #[error(
        "IPCC key lookup failed for key {key:?}: unknown result value {result}"
    )]
    UnknownResultValue { key: IpccKey, result: u8 },
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
