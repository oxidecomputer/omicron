// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;

/// Places to look for a zone's image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZoneImageFileSource {
    /// The file name to look for.
    pub file_name: String,

    /// The paths to look for the zone image in.
    ///
    /// This represents a high-confidence belief, but not a guarantee, that the
    /// zone image will be found in one of these locations.
    pub search_paths: Vec<Utf8PathBuf>,
}
