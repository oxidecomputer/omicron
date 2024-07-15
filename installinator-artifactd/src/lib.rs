// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

mod context;
mod http_entrypoints;
mod server;
mod store;

pub use context::ServerContext;
pub use server::ArtifactServer;
pub use store::{ArtifactGetter, EventReportStatus};

use anyhow::Result;

/// Run the OpenAPI generator for the API; which emits the OpenAPI spec
/// to stdout.
pub fn run_openapi() -> Result<()> {
    http_entrypoints::api()
        .openapi("Oxide Installinator Artifact Server", "0.0.1")
        .description("API for use by the installinator to retrieve artifacts")
        .contact_url("https://oxide.computer")
        .contact_email("api@oxide.computer")
        .write(&mut std::io::stdout())?;

    Ok(())
}
