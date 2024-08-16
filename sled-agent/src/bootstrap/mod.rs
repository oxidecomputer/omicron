// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bootstrap-related utilities

pub(crate) mod bootstore_setup;
pub mod client;
pub mod config;
pub mod early_networking;
mod http_entrypoints;
mod maghemite;
pub(crate) mod params;
mod pre_server;
mod pumpkind;
mod rack_ops;
pub(crate) mod rss_handle;
pub mod secret_retriever;
pub mod server;
mod sprockets_server;
mod views;

pub(crate) use pre_server::BootstrapNetworking;
pub use rack_ops::RssAccessError;

/// Describes errors which may occur while operating the bootstrap service.
#[derive(thiserror::Error, Debug)]
pub enum BootstrapError {
    #[error("IO error: {message}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Error cleaning up old state")]
    Cleanup(#[source] anyhow::Error),

    #[error("Failed to get all datasets")]
    ZfsDatasetsList(#[source] anyhow::Error),

    #[error("Failed to destroy dataset")]
    ZfsDestroy(#[from] illumos_utils::zfs::DestroyDatasetError),

    #[error("Failed to perform Zone operation")]
    ZoneOperation(#[from] illumos_utils::zone::AdmError),

    #[error("Error managing guest networking")]
    Opte(#[from] illumos_utils::opte::Error),
}

impl From<BootstrapError> for omicron_common::api::external::Error {
    fn from(err: BootstrapError) -> Self {
        Self::internal_error(&format!("{err:#}"))
    }
}
