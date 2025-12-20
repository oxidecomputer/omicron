// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8Path;
use chrono::DateTime;
use chrono::Utc;
use derive_more::AsRef;
use thiserror::Error;

/// Describes the final component of a path name (that has no `/` in it)
#[derive(AsRef, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Filename(String);
#[derive(Debug, Error)]
#[error("string is not a valid filename (has slashes or is '.' or '..')")]
pub(crate) struct BadFilename;
impl TryFrom<String> for Filename {
    type Error = BadFilename;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == "." || value == ".." || value.contains('/') {
            Err(BadFilename)
        } else {
            Ok(Filename(value))
        }
    }
}

/// Helper trait used to swap out basic filesystem functionality for testing
pub(crate) trait FileLister {
    /// List the files within a directory
    ///
    /// This should return an empty vec when the directory does not exist,
    /// rather than an error.
    fn list_files(
        &self,
        path: &Utf8Path,
    ) -> Vec<Result<Filename, anyhow::Error>>;

    /// Return the modification time of a file
    fn file_mtime(
        &self,
        path: &Utf8Path,
    ) -> Result<Option<DateTime<Utc>>, anyhow::Error>;

    /// Return whether a file exists
    fn file_exists(&self, path: &Utf8Path) -> Result<bool, anyhow::Error>;
}

/// `FileLister` implementation that uses the real filesystem
pub(crate) struct FilesystemLister;
impl FileLister for FilesystemLister {
    fn list_files(
        &self,
        path: &Utf8Path,
    ) -> Vec<Result<Filename, anyhow::Error>> {
        let entry_iter = match path.read_dir_utf8() {
            Ok(iter) => iter,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    // This interface is more useful if we swallow ENOTFOUND
                    // rather than propagate it since the caller will treat
                    // this the same as an empty directory.
                    return vec![];
                } else {
                    return vec![Err(
                        anyhow!(error).context("readdir {path:?}")
                    )];
                }
            }
        };

        entry_iter
            .map(|entry| {
                entry.context("reading directory entry").and_then(|entry| {
                    // It should be impossible for this `try_from()` to fail,
                    // but it's easy enough to handle gracefully.
                    Filename::try_from(entry.file_name().to_owned())
                        .with_context(|| {
                            format!(
                                "processing as a file name: {:?}",
                                entry.file_name(),
                            )
                        })
                })
            })
            .collect()
    }

    fn file_mtime(
        &self,
        path: &Utf8Path,
    ) -> Result<Option<DateTime<Utc>>, anyhow::Error> {
        let metadata = path
            .symlink_metadata()
            .with_context(|| format!("loading metadata for {path:?}"))?;

        Ok(metadata
            .modified()
            // This `ok()` ignores an error fetching the mtime.  We could
            // probably just handle it, since it shouldn't come up.  But this
            // preserves historical behavior.
            .ok()
            .map(|m| m.into()))
    }

    fn file_exists(&self, path: &Utf8Path) -> Result<bool, anyhow::Error> {
        path.try_exists()
            .with_context(|| format!("checking existence of {path:?}"))
    }
}
