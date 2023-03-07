// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::Path;

use tufaceous_lib::assemble::ArtifactManifest;

fn check_manifest(path: &Path) -> datatest_stable::Result<()> {
    let path = path.try_into()?;
    let manifest =
        ArtifactManifest::from_path(path).expect("failed to load manifest");
    manifest.verify_all_present()?;

    Ok(())
}

datatest_stable::harness!(check_manifest, "manifests", r"^.*\.toml");
