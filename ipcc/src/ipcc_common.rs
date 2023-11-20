// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! This module contains types shared between the real (illumos-only)
//! `crate::ioctl` module and the generic `crate::ioctl_stub` module.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum IpccError {
    #[error("Memory allocation error")]
    NoMem(#[source] IpccErrorInner),
    #[error("Invalid parameter")]
    InvalidParam(#[source] IpccErrorInner),
    #[error("Internal error occurred")]
    Internal(#[source] IpccErrorInner),
    #[error("Requested lookup key was not known to the SP")]
    KeyUnknown(#[source] IpccErrorInner),
    #[error("Value for the requested lookup key was too large for the supplied buffer")]
    KeyBufTooSmall(#[source] IpccErrorInner),
    #[error("Attempted to write to read-only key")]
    KeyReadonly(#[source] IpccErrorInner),
    #[error("Attempted write to key failed because the value is too long")]
    KeyValTooLong(#[source] IpccErrorInner),
    #[error("Compression or decompression failed")]
    KeyZerr(#[source] IpccErrorInner),
    #[error("Unknown library error")]
    UnknownErr(#[source] IpccErrorInner),
}

#[derive(Error, Debug)]
#[error("{context}: {errmsg} ({syserr})")]
pub struct IpccErrorInner {
    pub(crate) context: String,
    pub(crate) errmsg: String,
    pub(crate) syserr: String,
}

/// IPCC keys; the source of truth for these is RFD 316 + the
/// `host-sp-messages` crate in hubris.
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum IpccKey {
    Ping = 0,
    InstallinatorImageId = 1,
    Inventory = 2,
    System = 3,
    Dtrace = 4,
}

#[derive(Debug, Error)]
pub enum InstallinatorImageIdError {
    #[error(transparent)]
    Ipcc(#[from] IpccError),
    #[error("deserializing installinator image ID failed: {0}")]
    DeserializationFailed(String),
}
