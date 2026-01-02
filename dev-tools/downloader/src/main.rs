// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::Parser;
use xtask_downloader::{DownloadArgs, run_cmd};

#[expect(
    clippy::disallowed_macros,
    reason = "this is a dev-tool, and avoiding a dependency on \
     `oxide-tokio-rt` helps minimize compile time."
)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = DownloadArgs::parse();
    run_cmd(args).await
}
