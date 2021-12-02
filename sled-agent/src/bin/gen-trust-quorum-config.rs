// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;

use omicron_sled_agent::bootstrap::trust_quorum::Config;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "gen_trust_quorum_config",
    about = "Generate trust quorum configuration"
)]
struct Args {
    #[structopt(name = "THRESHOLD", parse(try_from_str))]
    threshold: usize,

    #[structopt(name = "TOTAL_SHARES", parse(try_from_str))]
    total_shares: usize,

    #[structopt(
        name = "FILENAME",
        parse(try_from_str),
        default_value = "config.json"
    )]
    filename: PathBuf,
}

fn main() {
    let args = Args::from_args();
    let config = Config::new(args.threshold, args.total_shares);
    config.write(&args.filename);
}
