// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use git_stub_vcs::Materializer;

fn main() {
    // The repo root is one level up from this crate's directory.
    let materializer = Materializer::for_build_script("..")
        .expect("detected VCS at repo root");

    materializer
        .materialize(
            "openapi/dns-server/dns-server-1.0.0-49359e.json.gitstub",
        )
        .expect("materialized dns-server v1 git stub");
}
