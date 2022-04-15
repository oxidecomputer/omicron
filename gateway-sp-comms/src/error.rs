// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::SpIdentifier;
use gateway_messages::ResponseError;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("error binding to UDP address {addr}: {err}")]
    UdpBind { addr: SocketAddr, err: io::Error },
    #[error("invalid configuration file: {reason}")]
    InvalidConfig { reason: String },
    #[error("error communicating with SP: {0}")]
    SpCommunicationFailed(#[from] SpCommunicationError),
    #[error("location discovery failed: {reason}")]
    DiscoveryFailed { reason: String },
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("nonexistent SP (type {:?}, slot {})", .0.typ, .0.slot)]
    SpDoesNotExist(SpIdentifier),
    #[error("unknown socket address for local ignition controller")]
    LocalIgnitionControllerAddressUnknown,
    #[error(
        "unknown socket address for SP (type {:?}, slot {})",
        .0.typ,
        .0.slot,
    )]
    SpAddressUnknown(SpIdentifier),
    #[error("timeout ({timeout:?}) elapsed communicating with {sp:?}")]
    Timeout { timeout: Duration, sp: SpIdentifier },
    #[error("error communicating with SP: {0}")]
    SpCommunicationFailed(#[from] SpCommunicationError),
    #[error("serial console is already attached")]
    SerialConsoleAttached,
    #[error("websocket connection failure: {0}")]
    BadWebsocketConnection(&'static str),
}

#[derive(Debug, Error)]
pub enum SpCommunicationError {
    #[error("failed to send UDP packet to {addr}: {err}")]
    UdpSend { addr: SocketAddr, err: io::Error },
    #[error("error reported by SP: {0}")]
    SpError(#[from] ResponseError),
    #[error(transparent)]
    BadResponseType(#[from] BadResponseType),
    #[error("bogus SP response: specified unknown ignition target {0}")]
    BadIgnitionTarget(usize),
}

#[derive(Debug, Error)]
#[error("bogus SP response type: expected {expected:?} but got {got:?}")]
pub struct BadResponseType {
    pub expected: &'static str,
    pub got: &'static str,
}
