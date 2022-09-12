// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::SpIdentifier;
use gateway_messages::ResponseError;
use std::io;
use std::net::SocketAddrV6;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SpCommunicationError {
    #[error("failed to send UDP packet to {addr}: {err}")]
    UdpSendTo { addr: SocketAddrV6, err: io::Error },
    #[error("failed to recv UDP packet: {0}")]
    UdpRecv(io::Error),
    #[error("failed to deserialize SP message from {peer}: {err}")]
    Deserialize { peer: SocketAddrV6, err: gateway_messages::HubpackError },
    #[error("RPC call failed (gave up after {0} attempts)")]
    ExhaustedNumAttempts(usize),
    #[error(transparent)]
    BadResponseType(#[from] BadResponseType),
    #[error("Error response from SP: {0}")]
    SpError(#[from] ResponseError),
    #[error("Bogus serial console state; detach and reattach")]
    BogusSerialConsoleState,
}

#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("update image cannot be empty")]
    ImageEmpty,
    #[error("update image is too large")]
    ImageTooLarge,
    #[error("failed to send update message to SP: {0}")]
    Communication(#[from] SpCommunicationError),
}

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("error binding to UDP address {addr}: {err}")]
    UdpBind { addr: SocketAddrV6, err: io::Error },
    #[error("invalid configuration file: {}", .reasons.join(", "))]
    InvalidConfig { reasons: Vec<String> },
    #[error("error communicating with SP: {0}")]
    SpCommunicationFailed(#[from] SpCommunicationError),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("discovery process not yet complete")]
    DiscoveryNotYetComplete,
    #[error("location discovery failed: {reason}")]
    DiscoveryFailed { reason: String },
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
    #[error(
        "timeout ({timeout:?}) elapsed communicating with {sp:?} on port {port}"
    )]
    Timeout { timeout: Duration, port: usize, sp: Option<SpIdentifier> },
    #[error("bogus SP response: specified unknown ignition target {0}")]
    BadIgnitionTarget(usize),
    #[error("error communicating with SP: {0}")]
    SpCommunicationFailed(#[from] SpCommunicationError),
    #[error("updating SP failed: {0}")]
    UpdateFailed(#[from] UpdateError),
}

#[derive(Debug, Error)]
#[error("bogus SP response type: expected {expected:?} but got {got:?}")]
pub struct BadResponseType {
    pub expected: &'static str,
    pub got: &'static str,
}
