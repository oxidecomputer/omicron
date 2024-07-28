// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Main entry point for the Omicron developer tool.

use anyhow::Result;
use clap::{Parser, Subcommand};

/// Tools for working with a local Omicron deployment
#[derive(Debug, Parser)]
#[clap(version)]
pub struct OmicronDevApp {
    #[clap(subcommand)]
    command: OmicronDevCmd,
}

impl OmicronDevApp {
    pub async fn exec(self) -> Result<()> {
        match self.command {
            #[cfg(feature = "include-db")]
            OmicronDevCmd::DbRun { args } => {
                print_xtask_nexus_tip();
                args.exec().await
            }
            #[cfg(feature = "include-db")]
            OmicronDevCmd::DbPopulate { args } => {
                print_xtask_nexus_tip();
                args.exec().await
            }
            #[cfg(feature = "include-db")]
            OmicronDevCmd::DbWipe { args } => {
                print_xtask_nexus_tip();
                args.exec().await
            }
            #[cfg(feature = "include-clickhouse")]
            OmicronDevCmd::ChRun { args } => {
                print_xtask_nexus_tip();
                args.exec().await
            }
            #[cfg(feature = "include-mgs")]
            OmicronDevCmd::MgsRun { args } => {
                print_xtask_nexus_tip();
                args.exec().await
            }
            #[cfg(feature = "include-nexus")]
            OmicronDevCmd::RunAll { args } => {
                // This does build Nexus anyway, so there's no point in
                // printing the tip.
                args.exec().await
            }
            #[cfg(feature = "include-cert")]
            OmicronDevCmd::CertCreate { args } => {
                print_xtask_nexus_tip();
                args.exec().await
            }
        }
    }
}

// A default build implies that omicron-dev was run via `cargo run -p
// omicron-dev`, which can be slow if it has to build all of Nexus. In that
// case, suggest using `cargo xtask omicron-dev` to avoid building Nexus.
#[cfg(feature = "default")]
#[allow(dead_code)]
fn print_xtask_nexus_tip() {
    eprintln!("omicron-dev: tip: use `cargo xtask omicron-dev` to avoid building Nexus");
}

#[cfg(not(feature = "default"))]
#[allow(dead_code)]
fn print_xtask_nexus_tip() {}

// NOTE: This enum should stay in sync with dev-tools/xtask/src/omicron_dev.rs.
#[derive(Debug, Subcommand)]
pub(crate) enum OmicronDevCmd {
    #[cfg(feature = "include-db")]
    /// Start a CockroachDB cluster for development
    DbRun {
        #[clap(flatten)]
        args: crate::db::DbRunArgs,
    },

    #[cfg(feature = "include-db")]
    /// Populate an existing CockroachDB cluster with the Omicron schema
    DbPopulate {
        #[clap(flatten)]
        args: crate::db::DbPopulateArgs,
    },

    #[cfg(feature = "include-db")]
    /// Wipe the Omicron schema (and all data) from an existing CockroachDB
    /// cluster
    DbWipe {
        #[clap(flatten)]
        args: crate::db::DbWipeArgs,
    },

    #[cfg(feature = "include-clickhouse")]
    /// Run a ClickHouse database server for development
    ChRun {
        #[clap(flatten)]
        args: crate::clickhouse::ChRunArgs,
    },

    #[cfg(feature = "include-mgs")]
    /// Run a simulated Management Gateway Service for development
    MgsRun {
        #[clap(flatten)]
        args: crate::mgs::MgsRunArgs,
    },

    #[cfg(feature = "include-nexus")]
    /// Run a full simulated control plane
    RunAll {
        #[clap(flatten)]
        args: crate::nexus::RunAllArgs,
    },

    #[cfg(feature = "include-cert")]
    /// Create a self-signed certificate for use with Omicron
    CertCreate {
        #[clap(flatten)]
        args: crate::cert::CertCreateArgs,
    },
}
