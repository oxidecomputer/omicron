// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8PathBuf;

/// The index, or list of files, within a support bundle.
pub struct SupportBundleIndex(Vec<Utf8PathBuf>);

impl SupportBundleIndex {
    pub fn new(s: &str) -> Self {
        let mut all_file_names: Vec<_> = s.lines().map(|line| {
            Utf8PathBuf::from(line)
        }).collect();
        all_file_names.sort();
        SupportBundleIndex(all_file_names)
    }

    /// Returns all files in the index, sorted by path
    pub fn files(&self) -> &Vec<Utf8PathBuf> {
        &self.0
    }
}
