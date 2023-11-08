// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable that generates OpenAPI definitions for the installinator artifact server.

use anyhow::Result;
use clap::Parser;
use omicron_common::cmd::CmdError;

#[derive(Debug, Parser)]
#[clap(name = "installinator-artifactd")]
enum Args {
    /// Print the external OpenAPI Spec document and exit
    Openapi,
    // NOTE: this server is not intended to be run as a standalone service. Instead, it should be
    // embedded as part of other servers (e.g. wicketd).
}

fn main() {
    if let Err(cmd_error) = do_run() {
        omicron_common::cmd::fatal(cmd_error);
    }
}

fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    match args {
        Args::Openapi => {
            installinator_artifactd::run_openapi().map_err(|error| {
                CmdError::Failure(
                    error.context("failed to generate OpenAPI spec"),
                )
            })
        }
    }
}
