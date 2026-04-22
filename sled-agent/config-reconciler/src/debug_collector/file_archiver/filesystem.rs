// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8Path;
use chrono::DateTime;
use chrono::Utc;
use derive_more::AsRef;
use std::fs::FileType;
use thiserror::Error;

/// Describes the final component of a path name (that has no `/` in it)
#[derive(AsRef, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Filename(String);

#[derive(Debug, Error)]
#[error("string is not a valid filename (has slashes or is '.' or '..'): {0}")]
pub(crate) struct BadFilename(String);

impl TryFrom<String> for Filename {
    type Error = BadFilename;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == "." || value == ".." || value.contains('/') {
            Err(BadFilename(value))
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

    /// List the immediate subdirectories within a directory
    ///
    /// This should return an empty vec when the directory does not exist,
    /// rather than an error.
    fn list_directories(
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
        list_files(path, &|filetype| filetype.is_file())
    }

    fn list_directories(
        &self,
        path: &Utf8Path,
    ) -> Vec<Result<Filename, anyhow::Error>> {
        list_files(path, &|filetype| filetype.is_dir())
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

fn list_files(
    path: &Utf8Path,
    filter: &dyn Fn(&FileType) -> bool,
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
                return vec![Err(anyhow!(error).context("readdir {path:?}"))];
            }
        }
    };

    entry_iter
        .filter_map(|entry| {
            // entry: a Result<directory entry>
            entry
                .context("reading directory entry")
                .and_then(|entry| {
                    // entry: directory entry
                    // Assemble both the filename and file type.
                    let filename = entry.file_name().to_owned();
                    entry
                        .file_type()
                        .map(|filetype| (filename, filetype))
                        .with_context(|| {
                            format!(
                                "determining file type of {:?}",
                                entry.file_name(),
                            )
                        })
                })
                // now a Result<(filename, filetype)>
                .map(|(filename, filetype)| {
                    filter(&filetype).then_some(filename)
                })
                // now a Result<Option<filename>>
                .transpose()
                // now an Option<Result<filename>>
                .map(|maybe_filename| {
                    // maybe_filename: Result<filename>
                    maybe_filename.and_then(|filename| {
                        // It should be impossible for this `try_from()` to
                        // fail, but it's easy enough to handle gracefully.
                        Filename::try_from(filename)
                            .context("processing file name")
                    })
                    // now a Result<Filename>
                })
            // now an Option<Result<Filename>>
            // We'll filter only the Some values.
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::FileLister;
    use super::Filename;
    use super::FilesystemLister;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;

    /// Temporary directory that is preserved on test failure.
    ///
    /// The path is printed to stderr on creation.  Call `cleanup()` at the end
    /// of a successful test to delete it.  If `cleanup()` is never called
    /// (e.g., the test panicked), the directory is preserved for inspection.
    struct TestDir {
        dir: Option<Utf8TempDir>,
    }

    impl TestDir {
        fn new() -> Self {
            let dir =
                camino_tempfile::tempdir().expect("failed to create temp dir");
            eprintln!("test directory: {}", dir.path());
            TestDir { dir: Some(dir) }
        }

        fn path(&self) -> &Utf8Path {
            // unwrap(): this is only `None` after `cleanup()`, but it's
            // immediately dropped at that point.
            self.dir.as_ref().unwrap().path()
        }

        fn cleanup(mut self) {
            drop(self.dir.take());
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if let Some(dir) = self.dir.take() {
                let path = dir.keep();
                eprintln!(
                    "test directory preserved (test may have failed): {path}"
                );
            }
        }
    }

    #[test]
    fn test_filename() {
        assert_eq!(
            Filename::try_from(String::from("foo")).unwrap().as_ref(),
            "foo"
        );
        assert!(Filename::try_from(String::from(".")).is_err());
        assert!(Filename::try_from(String::from("..")).is_err());
        assert!(Filename::try_from(String::from("foo/bar")).is_err());
        assert!(Filename::try_from(String::from("foo/")).is_err());
        assert!(Filename::try_from(String::from("/bar")).is_err());
    }

    // Returns a temp dir containing a regular file ("regular.txt"), a
    // subdirectory ("subdir"), and a symlink ("link" -> "regular.txt"), to
    // allow verifying that listing functions filter by entry type.
    fn setup_mixed_dir() -> TestDir {
        let dir = TestDir::new();
        std::fs::write(dir.path().join("regular.txt"), "contents")
            .expect("failed to write regular file");
        std::fs::create_dir(dir.path().join("subdir"))
            .expect("failed to create subdir");
        std::os::unix::fs::symlink("regular.txt", dir.path().join("link"))
            .expect("failed to create symlink");
        dir
    }

    /// Tests filtering on file type: regular files
    #[test]
    fn test_list_files_type_filtering() {
        let dir = setup_mixed_dir();
        let lister = FilesystemLister;
        let mut results: Vec<Filename> = lister
            .list_files(dir.path())
            .into_iter()
            .map(|r| r.expect("unexpected error in list_files"))
            .collect();
        results.sort();
        assert_eq!(
            results,
            [Filename::try_from("regular.txt".to_owned()).unwrap()]
        );
        dir.cleanup();
    }

    /// Tests filtering on file type: directories
    #[test]
    fn test_list_directories_type_filtering() {
        let dir = setup_mixed_dir();
        let lister = FilesystemLister;
        let mut results: Vec<Filename> = lister
            .list_directories(dir.path())
            .into_iter()
            .map(|r| r.expect("unexpected error in list_directories"))
            .collect();
        results.sort();
        assert_eq!(results, [Filename::try_from("subdir".to_owned()).unwrap()]);
        dir.cleanup();
    }

    /// Verifies that listing a non-existent directory produces an empty listing
    /// rather than an error.
    #[test]
    fn test_list_files_nonexistent_dir() {
        let dir = TestDir::new();
        let lister = FilesystemLister;
        let results = lister.list_files(&dir.path().join("nonexistent"));
        assert!(
            results.is_empty(),
            "expected empty vec for nonexistent dir, got: {results:?}",
        );
        dir.cleanup();
    }

    /// Verify basic behavior of `FilesystemLister::file_exists()`.
    #[test]
    fn test_file_exists() {
        let dir = TestDir::new();
        let file_path = dir.path().join("file.txt");
        std::fs::write(&file_path, "hello").expect("failed to write file");
        let lister = FilesystemLister;
        assert!(
            lister.file_exists(&file_path).expect("file_exists failed"),
            "expected true for existing file",
        );
        let nonexistent = dir.path().join("nonexistent.txt");
        assert!(
            !lister.file_exists(&nonexistent).expect("file_exists failed"),
            "expected false for nonexistent file",
        );
        dir.cleanup();
    }

    /// Verify basic behavior of `FilesystemLister::file_mtime()`.
    #[test]
    fn test_file_mtime() {
        let dir = TestDir::new();
        let file_path = dir.path().join("file.txt");
        std::fs::write(&file_path, "hello").expect("failed to write file");
        let lister = FilesystemLister;
        let mtime = lister.file_mtime(&file_path).expect("file_mtime failed");
        assert!(mtime.is_some(), "expected Some mtime for existing file",);
        dir.cleanup();
    }
}
