// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

fn main() {
    // Set RPATH for native libraries. Required if this crate depends (even
    // transitively) on a -sys crate listed in omicron-rpaths.
    omicron_rpaths::configure_default_omicron_rpaths();
}
