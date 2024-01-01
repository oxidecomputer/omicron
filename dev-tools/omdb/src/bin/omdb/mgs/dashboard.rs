// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code for the MGS dashboard subcommand

use clap::Args;

#[derive(Debug, Args)]
pub(crate) struct DashboardArgs {}

///
/// Runs `omdb mgs dashboard`
///
pub(crate) async fn cmd_mgs_dashboard(
    _mgs_client: &gateway_client::Client,
    _args: &DashboardArgs,
) -> Result<(), anyhow::Error> {
    anyhow::bail!("not yet");
}
