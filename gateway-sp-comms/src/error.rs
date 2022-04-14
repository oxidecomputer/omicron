// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::SpIdentifier;
use gateway_messages::ResponseError;
use std::io;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("error binding to UDP address {addr}: {err}")]
    UdpBind { addr: SocketAddr, err: io::Error },
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("nonexistent SP (type {:?}, slot {})", .0.typ, .0.slot)]
    SpDoesNotExist(SpIdentifier),
    #[error(
        "unknown socket address for SP (type {:?}, slot {})",
        .0.typ,
        .0.slot,
    )]
    SpAddressUnknown(SpIdentifier),
    #[error("timeout elapsed")]
    Timeout,
    #[error("error communicating with SP: {0}")]
    SpCommunicationFailed(#[from] SpCommunicationError),
}

#[derive(Debug, Error)]
pub enum SpCommunicationError {
    #[error("failed to send UDP packet to {addr}: {err}")]
    UdpSend { addr: SocketAddr, err: io::Error },
    #[error("error reported by SP: {0}")]
    SpError(#[from] ResponseError),
    #[error("bogus SP response type: expected {expected:?} but got {got:?}")]
    BadResponseType { expected: &'static str, got: &'static str },
    #[error("bogus SP response: specified unknown ignition target {0}")]
    BadIgnitionTarget(usize),
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}
