// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use wicket_dbg::Runner;

fn main() -> Result<()> {
    let drain = slog::Discard;
    let root = slog::Logger::root(drain, slog::o!());
    let runner = Runner::new(root);
    // TODO: Allow port configuration
    wicket_dbg::Server::run("::1:9010", runner)
}
