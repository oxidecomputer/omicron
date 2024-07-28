// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer tool for easily running bits of Omicron.

use anyhow::Result;
use clap::{Args, Subcommand};

use crate::external;

#[derive(Args)]
pub(crate) struct OmicronDevArgs {
    /// Run the Omicron dev tool.
    #[clap(subcommand)]
    cmd: OmicronDevCmd,
}

#[derive(Subcommand)]
enum OmicronDevCmd {
    /// Start a CockroachDB cluster for development
    DbRun(external::External),

    /// Populate an existing CockroachDB cluster with the Omicron schema
    DbPopulate(external::External),

    /// Wipe the Omicron schema (and all data) from an existing CockroachDB
    /// cluster
    DbWipe(external::External),

    /// Run a ClickHouse database server for development
    ChRun(external::External),

    /// Run a simulated Management Gateway Service for development
    MgsRun(external::External),

    /// Run a full simulated control plane
    RunAll(external::External),

    /// Create a self-signed certificate for use with Omicron
    CertCreate(external::External),
}

pub(crate) async fn run_cmd(args: OmicronDevArgs) -> Result<()> {
    match args.cmd {
        OmicronDevCmd::DbRun(args) => {
            run_omicron_dev(args, "include-db", "db-run")
        }
        OmicronDevCmd::DbPopulate(args) => {
            run_omicron_dev(args, "include-db", "db-populate")
        }
        OmicronDevCmd::DbWipe(args) => {
            run_omicron_dev(args, "include-db", "db-wipe")
        }
        OmicronDevCmd::ChRun(args) => {
            run_omicron_dev(args, "include-clickhouse", "ch-run")
        }
        OmicronDevCmd::MgsRun(args) => {
            run_omicron_dev(args, "include-mgs", "mgs-run")
        }
        OmicronDevCmd::RunAll(args) => {
            run_omicron_dev(args, "include-nexus", "run-all")
        }
        OmicronDevCmd::CertCreate(args) => {
            run_omicron_dev(args, "include-cert", "cert-create")
        }
    }
}

fn run_omicron_dev(
    args: external::External,
    feature: impl AsRef<str>,
    command: impl AsRef<str>,
) -> Result<()> {
    args.cargo_args(["--no-default-features", "--features", feature.as_ref()])
        .external_args([command.as_ref()])
        .exec_bin("omicron-dev")
}
