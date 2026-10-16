// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

/// The format for `.config/xtask.toml`.
#[derive(Deserialize, Debug)]
pub struct XtaskConfig {
    pub libraries: BTreeMap<String, LibraryConfig>,
}

#[derive(Deserialize, Debug)]
pub struct LibraryConfig {
    /// If `Some`, this library is only allowed in the given list of binaries.
    /// If `None`, this library is allowed in any binary.
    pub binary_allow_list: Option<BTreeSet<String>>,
}
