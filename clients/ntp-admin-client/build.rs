// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use git_stub_vcs::Materializer;

fn main() {
    let materializer = Materializer::for_build_script("../../")
        .expect("detected VCS at repo root");
    materializer
        .materialize("openapi/ntp-admin/ntp-admin-1.0.0-aeffc2.json.gitstub")
        .expect("materialized ntp-admint-client v1 git stub");
}
