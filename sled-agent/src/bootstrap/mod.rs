// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related utilities

use ::bootstore::schemes::v0 as bootstore_v0;
use omicron_common::ledger;

mod bootstore;
pub mod client;
pub mod config;
pub mod early_networking;
mod http_entrypoints;
mod maghemite;
pub(crate) mod params;
mod pre_server;
mod rack_ops;
pub(crate) mod rss_handle;
mod secret_retriever;
pub mod server;
mod sprockets_server;
mod views;

pub(crate) use pre_server::BootstrapNetworking;
pub use rack_ops::RssAccessError;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(thiserror::Error, Debug)]
pub enum BootstrapError {
    #[error("IO error: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Error cleaning up old state: {0}")]
    Cleanup(anyhow::Error),

    #[error("Failed to enable routing: {0}")]
    EnablingRouting(illumos_utils::ExecutionError),

    #[error("Error contacting ddmd: {0}")]
    DdmError(#[from] ddm_admin_client::DdmError),

    #[error("Failed to access ledger: {0}")]
    Ledger(#[from] ledger::Error),

    #[error("Error managing sled agent: {0}")]
    SledError(String),

    #[error("Error collecting peer addresses: {0}")]
    PeerAddresses(String),

    #[error("Failed to initialize bootstrap address: {err}")]
    BootstrapAddress { err: illumos_utils::zone::EnsureGzAddressError },

    #[error("Failed to get bootstrap address: {err}")]
    GetBootstrapAddress { err: illumos_utils::zone::GetAddressError },

    #[error(transparent)]
    GetMacError(#[from] illumos_utils::dladm::GetMacError),

    #[error("Failed to lookup VNICs on boot: {0}")]
    GetVnics(#[from] illumos_utils::dladm::GetVnicError),

    #[error("Failed to delete VNIC on boot: {0}")]
    DeleteVnic(#[from] illumos_utils::dladm::DeleteVnicError),

    #[error("Failed to get all datasets: {0}")]
    ZfsDatasetsList(anyhow::Error),

    #[error("Failed to destroy dataset: {0}")]
    ZfsDestroy(#[from] illumos_utils::zfs::DestroyDatasetError),

    #[error("Failed to ensure ZFS filesystem: {0}")]
    ZfsEnsureFilesystem(#[from] illumos_utils::zfs::EnsureFilesystemError),

    #[error("Failed to perform Zone operation: {0}")]
    ZoneOperation(#[from] illumos_utils::zone::AdmError),

    #[error("Error managing guest networking: {0}")]
    Opte(#[from] illumos_utils::opte::Error),

    #[error("Error accessing version information: {0}")]
    Version(#[from] crate::updates::Error),

    #[error("Bootstore Error: {0}")]
    Bootstore(#[from] bootstore_v0::NodeRequestError),

    #[error("Missing M.2 Paths for dataset: {0}")]
    MissingM2Paths(&'static str),
}

impl From<BootstrapError> for omicron_common::api::external::Error {
    fn from(err: BootstrapError) -> Self {
        Self::internal_error(&err.to_string())
    }
}
