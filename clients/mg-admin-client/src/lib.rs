// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]
#![allow(rustdoc::broken_intra_doc_links)]
#![allow(rustdoc::invalid_html_tags)]

#[allow(dead_code)]
mod inner {
    include!(concat!(env!("OUT_DIR"), "/mg-admin-client.rs"));
}

pub use inner::types;
use inner::types::Prefix4;
pub use inner::Error;

use inner::Client as InnerClient;
use omicron_common::api::external::BgpPeerState;
use slog::Logger;
use std::hash::Hash;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use thiserror::Error;

// TODO-cleanup Is it okay to hardcode this port number here?
const MGD_PORT: u16 = 4676;

#[derive(Debug, Error)]
pub enum MgError {
    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("Failed making HTTP request to mgd: {0}")]
    MgApi(#[from] Error<types::Error>),
}

impl From<inner::types::FsmStateKind> for BgpPeerState {
    fn from(s: inner::types::FsmStateKind) -> BgpPeerState {
        use inner::types::FsmStateKind;
        match s {
            FsmStateKind::Idle => BgpPeerState::Idle,
            FsmStateKind::Connect => BgpPeerState::Connect,
            FsmStateKind::Active => BgpPeerState::Active,
            FsmStateKind::OpenSent => BgpPeerState::OpenSent,
            FsmStateKind::OpenConfirm => BgpPeerState::OpenConfirm,
            FsmStateKind::SessionSetup => BgpPeerState::SessionSetup,
            FsmStateKind::Established => BgpPeerState::Established,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    pub inner: InnerClient,
    pub log: Logger,
}

impl Client {
    /// Creates a new [`Client`] that points to localhost
    pub fn localhost(log: &Logger) -> Result<Self, MgError> {
        Self::new(log, SocketAddr::new(Ipv6Addr::LOCALHOST.into(), MGD_PORT))
    }

    pub fn new(log: &Logger, mgd_addr: SocketAddr) -> Result<Self, MgError> {
        let dur = std::time::Duration::from_secs(60);
        let log = log.new(slog::o!("MgAdminClient" => mgd_addr));

        let inner = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()?;
        let inner = InnerClient::new_with_client(
            &format!("http://{mgd_addr}"),
            inner,
            log.clone(),
        );
        Ok(Self { inner, log })
    }
}

impl Eq for Prefix4 {}

impl PartialEq for Prefix4 {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.length == other.length
    }
}

impl Hash for Prefix4 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.length.hash(state);
    }
}
