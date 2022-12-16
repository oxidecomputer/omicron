// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::communicator::Communicator;
use crate::error::ConfigError;
use crate::management_switch::SwitchConfig;
use slog::Logger;
use std::sync::Arc;

/// Shared state used by API request handlers
pub struct ServerContext {
    pub sp_comms: Arc<Communicator>,
    pub log: Logger,
}

impl ServerContext {
    pub async fn new(
        switch_config: SwitchConfig,
        log: &Logger,
    ) -> Result<Arc<Self>, ConfigError> {
        let comms = Arc::new(
            Communicator::new(
                switch_config,
                TempNoopHostPhase2RecoveryProvider,
                log,
            )
            .await?,
        );
        Ok(Arc::new(ServerContext {
            sp_comms: Arc::clone(&comms),
            log: log.clone(),
        }))
    }
}

// TODO: Delete this and replace with real host phase 2 recovery provider
// (probably hooked up to a new dropshot endpoint to allow wicketd to send us
// recovery images to serve).
#[derive(Debug, Clone)]
struct TempNoopHostPhase2RecoveryProvider;

#[async_trait::async_trait]
impl gateway_sp_comms::HostPhase2Provider
    for TempNoopHostPhase2RecoveryProvider
{
    async fn read_phase2_data(
        &self,
        hash: [u8; 32],
        _offset: u64,
        _out: &mut [u8],
    ) -> Result<usize, gateway_sp_comms::error::HostPhase2Error> {
        Err(gateway_sp_comms::error::HostPhase2Error::NoImage {
            hash: hex::encode(hash),
        })
    }
}
