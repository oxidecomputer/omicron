// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::Args;
use futures::StreamExt;
use libc::SIGINT;
use signal_hook_tokio::Signals;

#[derive(Clone, Debug, Args)]
pub(crate) struct MgsRunArgs {}

impl MgsRunArgs {
    pub(crate) async fn exec(&self) -> Result<(), anyhow::Error> {
        // Start a stream listening for SIGINT
        let signals =
            Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
        let mut signal_stream = signals.fuse();

        println!("omicron-dev: setting up MGS ... ");
        let gwtestctx = gateway_test_utils::setup::test_setup(
            "omicron-dev",
            gateway_messages::SpPort::One,
        )
        .await;
        println!("omicron-dev: MGS is running.");

        let addr = gwtestctx.client.bind_address;
        println!("omicron-dev: MGS API: http://{:?}", addr);

        // Wait for a signal.
        let caught_signal = signal_stream.next().await;
        assert_eq!(caught_signal.unwrap(), SIGINT);
        eprintln!(
            "omicron-dev: caught signal, shutting down and removing \
        temporary directory"
        );

        gwtestctx.teardown().await;
        Ok(())
    }
}
