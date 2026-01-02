// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The storage manager task

use camino::Utf8Path;
use camino::Utf8PathBuf;
use omicron_common::disk::DatasetName;
use omicron_common::disk::SharedDatasetConfig;

#[derive(Debug)]
pub enum NestedDatasetListOptions {
    /// Returns children of the requested dataset, but not the dataset itself.
    ChildrenOnly,
    /// Returns both the requested dataset as well as all children.
    SelfAndChildren,
}

/// Configuration information necessary to request a single nested dataset.
///
/// These datasets must be placed within one of the top-level datasets
/// managed directly by Nexus.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NestedDatasetConfig {
    /// Location of this nested dataset
    pub name: NestedDatasetLocation,

    /// Configuration of this dataset
    pub inner: SharedDatasetConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NestedDatasetLocation {
    /// A path, within the dataset root, which is being requested.
    pub path: String,

    /// The root in which this dataset is being requested
    pub root: DatasetName,
}

impl NestedDatasetLocation {
    /// Returns the desired mountpoint of this dataset.
    ///
    /// Does not ensure that the dataset is mounted.
    pub fn mountpoint(&self, mount_root: &Utf8Path) -> Utf8PathBuf {
        let mut path = Utf8Path::new(&self.path);

        // This path must be nested, so we need it to be relative to
        // "self.root". However, joining paths in Rust is quirky,
        // as it chooses to replace the path entirely if the argument
        // to `.join(...)` is absolute.
        //
        // Here, we "fix" the path to make non-absolute before joining
        // the paths.
        while path.is_absolute() {
            path = path
                .strip_prefix("/")
                .expect("Path is absolute, but we cannot strip '/' character");
        }

        // mount_root: Usually "/", but can be a tmp dir for tests
        // self.root:  Parent dataset mountpoint
        // path:       Path to nested dataset within parent dataset
        self.root.mountpoint(mount_root).join(path)
    }

    /// Returns the full name of the nested dataset.
    ///
    /// This is a combination of the parent and child dataset names.
    pub fn full_name(&self) -> String {
        if self.path.is_empty() {
            self.root.full_name().to_string()
        } else {
            format!("{}/{}", self.root.full_name(), self.path)
        }
    }
}
