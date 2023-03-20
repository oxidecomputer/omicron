// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Stubs for the Nexus interface.
//!
//! Defined to make testing easier.

use nexus_client::{
    types as NexusTypes, Client as NexusClient, Error as NexusError,
};
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service_aggressive, BackoffError,
};
use slog::Logger;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error making HTTP request to Nexus: {0}")]
    NexusApi(#[from] NexusError<NexusTypes::Error>),

    #[error("Failed to construct an HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
}

#[async_trait::async_trait]
pub(crate) trait NexusInterface: Send + Sync + 'static {
    async fn rack_initialization_complete(
        &self,
        log: &Logger,
        address: SocketAddr,
        rack_id: Uuid,
        request: &NexusTypes::RackInitializationRequest,
    ) -> Result<(), Error>;
}

pub(crate) struct RealNexusAccess {}

#[async_trait::async_trait]
impl NexusInterface for RealNexusAccess {
    async fn rack_initialization_complete(
        &self,
        log: &Logger,
        address: SocketAddr,
        rack_id: Uuid,
        request: &NexusTypes::RackInitializationRequest,
    ) -> Result<(), Error> {
        let nexus_client = NexusClient::new(
            &format!("http://{}", address),
            log.new(o!("component" => "NexusClient")),
        );
        let notify_nexus = || async {
            nexus_client
                .rack_initialization_complete(&rack_id, &request)
                .await
                .map_err(BackoffError::transient)
        };
        let log_failure = |err, _| {
            info!(log, "Failed to handoff to nexus: {err}");
        };

        retry_notify(
            retry_policy_internal_service_aggressive(),
            notify_nexus,
            log_failure,
        )
        .await?;
        Ok(())
    }
}
