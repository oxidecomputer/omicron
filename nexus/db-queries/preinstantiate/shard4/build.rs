// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use preinstantiate::build::{Config, ShardIndex, run_build_script_or_exit};

include!("../preinstantiate_config.rs");

fn main() {
    let shard = ShardIndex::new(4, 8).expect("shard 4 of 8 is valid");
    run_build_script_or_exit(&config(shard));
}
