// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use vergen_gitcl::Emitter;
use vergen_gitcl::GitclBuilder;

fn main() {
    // See omicron-rpaths for documentation. NOTE: This file MUST be kept in
    // sync with the other build.rs files in this repository.
    omicron_rpaths::configure_default_omicron_rpaths();

    // Define the `VERGEN_GIT_SHA` and `VERGEN_GIT_DIRTY` environment variables
    // (accessible via `env!()`) that note the current git commit and whether
    // the working tree is dirty at the time of this build.
    //
    // We use this to check our own git SHA against the git SHA of the Nexus
    // that generated blueprint planner debug logs.
    let gitcl = GitclBuilder::default()
        .sha(/* short= */ false)
        .dirty(/* include_untracked= */ false)
        .build()
        .expect("valid GitclBuilder configuration");
    Emitter::default()
        .add_instructions(&gitcl)
        .expect("valid instructions")
        .emit()
        .expect("emitted version information");
}
