// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

fn main() {
    // libtopo lives outside the default library path.  libtopo-sys adds the
    // link search path, but the runtime path has to be set on our binary.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("illumos") {
        println!("cargo:rustc-link-arg=-Wl,-R/usr/lib/fm/amd64");
    }
}
