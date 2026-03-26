// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;

/// Places to look for a resolvable file's source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvableFileSource {
    /// The file name to look for.
    pub file_name: String,

    /// The paths to look for the file in. This file may be a runnable
    /// zone image or a regular file.
    ///
    /// This represents a high-confidence belief, but not a guarantee, that the
    /// file will be found in one of these locations.
    pub search_paths: Vec<Utf8PathBuf>,
}
