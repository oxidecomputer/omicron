// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer tool for running MGS.

use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use libc::SIGINT;
use signal_hook_tokio::Signals;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = MgsDevApp::parse();
    args.exec().await
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
    #[clap(flatten)]
    mgs_metrics_args: gateway_test_utils::setup::MgsMetricsArgs,
}

impl MgsRunArgs {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        // Start a stream listening for SIGINT
        let signals =
            Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
        let mut signal_stream = signals.fuse();

        println!("mgs-dev: setting up MGS ... ");
        let gwtestctx =
            gateway_test_utils::setup::test_setup_with_metrics_args(
                "mgs-dev",
                gateway_messages::SpPort::One,
                self.mgs_metrics_args,
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
