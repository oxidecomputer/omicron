// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use vergen::EmitBuilder;

fn main() {
    // See omicron-rpaths for documentation. NOTE: This file MUST be kept in
    // sync with the other build.rs files in this repository.
    omicron_rpaths::configure_default_omicron_rpaths();

    EmitBuilder::builder()
        .all_git()
        .emit()
        .expect("failed to write vergen info");
}
