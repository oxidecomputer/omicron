// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

fn main() {
    // Set RPATH for native libraries. Required if this crate depends (even
    // transitively) on a -sys crate listed in omicron-rpaths.
    omicron_rpaths::configure_default_omicron_rpaths();

    // Expose the release number from tools/pins.toml as SYSTEM_RELEASE_NUMBER.
    println!("cargo:rerun-if-changed=../tools/pins.toml");
    let pins = omicron_pins::Pins::read_from_dir("..".into())
        .expect("tools/pins.toml is valid");
    println!(
        "cargo:rustc-env=SYSTEM_RELEASE_NUMBER={}",
        pins.system_version.major
    );
}
