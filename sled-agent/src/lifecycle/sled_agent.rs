// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO explanatory comment

use std::borrow::Cow;

use crate::bootstrap::agent::PersistentSledAgentRequest;
use crate::bootstrap::params::StartSledAgentRequest;
use crate::bootstrap::secret_retriever::LrtqOrHardcodedSecretRetriever;
use crate::config::Config;
use crate::server::Server;
use crate::services::ServiceManager;
use crate::storage_manager::StorageManager;
use bootstore::schemes::v0 as bootstore;
use ddm_admin_client::Client as DdmAdminClient;
use omicron_common::ledger;
use omicron_common::ledger::Ledger;
use slog::Logger;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SledAgentServerStartError {
    #[error("Failed to start sled-agent server: {0}")]
    FailedStartingServer(String),

    #[error("Missing M.2 Paths for dataset: {0}")]
    MissingM2Paths(&'static str),

    #[error("Failed to commit sled agent request to ledger")]
    CommitToLedger(#[from] ledger::Error),
}

impl From<super::MissingM2Paths> for SledAgentServerStartError {
    fn from(value: super::MissingM2Paths) -> Self {
        Self::MissingM2Paths(value.0)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn start(
    config: &Config,
    request: &StartSledAgentRequest,
    bootstore: &bootstore::NodeHandle,
    service_manager: &ServiceManager,
    storage_manager: &StorageManager,
    ddmd_client: &DdmAdminClient,
    base_log: &Logger,
    log: &Logger,
) -> Result<Server, SledAgentServerStartError> {
    info!(log, "Loading Sled Agent: {:?}", request);

    // TODO-correctness: If we fail partway through, we do not cleanly roll back
    // all the changes we've made (e.g., initializing LRTQ, informing the
    // storage manager about keys, advertising prefixes, ...).

    // Initialize the secret retriever used by the `KeyManager`
    if request.use_trust_quorum {
        info!(log, "KeyManager: using lrtq secret retriever");
        let salt = request.hash_rack_id();
        LrtqOrHardcodedSecretRetriever::init_lrtq(salt, bootstore.clone())
    } else {
        info!(log, "KeyManager: using hardcoded secret retriever");
        LrtqOrHardcodedSecretRetriever::init_hardcoded();
    }

    // Inform the storage service that the key manager is available
    storage_manager.key_manager_ready().await;

    // Start trying to notify ddmd of our sled prefix so it can
    // advertise it to other sleds.
    //
    // TODO-security This ddmd_client is used to advertise both this
    // (underlay) address and our bootstrap address. Bootstrap addresses are
    // unauthenticated (connections made on them are auth'd via sprockets),
    // but underlay addresses should be exchanged via authenticated channels
    // between ddmd instances. It's TBD how that will work, but presumably
    // we'll need to do something different here for underlay vs bootstrap
    // addrs (either talk to a differently-configured ddmd, or include info
    // indicating which kind of address we're advertising).
    ddmd_client.advertise_prefix(request.subnet);

    // Server does not exist, initialize it.
    let server = Server::start(
        config,
        base_log.clone(),
        request.clone(),
        service_manager.clone(),
        storage_manager.clone(),
        bootstore.clone(),
    )
    .await
    .map_err(SledAgentServerStartError::FailedStartingServer)?;

    info!(log, "Sled Agent loaded; recording configuration");

    // Record this request so the sled agent can be automatically
    // initialized on the next boot.
    let paths = super::sled_config_paths(storage_manager.resources()).await?;

    let mut ledger = Ledger::new_with(
        &log,
        paths,
        PersistentSledAgentRequest { request: Cow::Borrowed(request) },
    );
    ledger.commit().await?;

    Ok(server)
}
