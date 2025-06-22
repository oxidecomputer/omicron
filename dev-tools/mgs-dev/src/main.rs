// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer tool for running MGS.

use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use libc::SIGINT;
use signal_hook_tokio::Signals;
use std::net::SocketAddr;

fn main() -> anyhow::Result<()> {
    omicron_runtime::run(async {
        let args = MgsDevApp::parse();
        args.exec().await
    })
}

#[derive(Clone, Debug, Parser)]
struct MgsDevApp {
    #[clap(subcommand)]
    command: MgsDevCmd,
}

impl MgsDevApp {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        match &self.command {
            MgsDevCmd::Run(args) => args.exec().await,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum MgsDevCmd {
    /// Run a simulated Management Gateway Service for development.
    Run(MgsRunArgs),
}

#[derive(Clone, Debug, Args)]
struct MgsRunArgs {
    /// Override the address of the Nexus instance to use when registering the
    /// Oximeter producer.
    #[clap(long)]
    nexus_address: Option<SocketAddr>,
}

impl MgsRunArgs {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        // Start a stream listening for SIGINT
        let signals =
            Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
        let mut signal_stream = signals.fuse();

        println!("mgs-dev: setting up MGS ... ");
        let (mut mgs_config, sp_sim_config) =
            gateway_test_utils::setup::load_test_config();
        if let Some(addr) = self.nexus_address {
            mgs_config.metrics =
                Some(gateway_test_utils::setup::MetricsConfig {
                    disabled: false,
                    dev_nexus_address: Some(addr),
                    dev_bind_loopback: true,
                });
        }

        let gwtestctx = gateway_test_utils::setup::test_setup_with_config(
            "mgs-dev",
            gateway_messages::SpPort::One,
            mgs_config,
            &sp_sim_config,
            None,
        )
        .await;
        println!("mgs-dev: MGS is running.");

        let addr = gwtestctx.client.bind_address;
        println!("mgs-dev: MGS API: http://{:?}", addr);

        // Wait for a signal.
        let caught_signal = signal_stream.next().await;
        assert_eq!(caught_signal.unwrap(), SIGINT);
        eprintln!(
            "mgs-dev: caught signal, shutting down and removing \
        temporary directory"
        );

        gwtestctx.teardown().await;
        Ok(())
    }
}
