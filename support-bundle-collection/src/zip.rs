// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for converting a collected bundle directory into a zipfile.
//!
//! These are used by callers that need to produce a single archive from
//! the directory of collected data — both Nexus (for storing on a sled
//! agent) and omdb (for writing to local storage).

use ::zip::ZipWriter;
use ::zip::write::FullFileOptions;
use anyhow::Result;
use camino::Utf8DirEntry;
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use camino_tempfile::tempfile_in;

/// Takes the contents of `dir`, and zips them into a single zipfile
/// stored as a tempfile under `tempdir`.
pub fn bundle_to_zipfile(
    dir: &Utf8TempDir,
    tempdir: &Utf8Path,
) -> Result<std::fs::File> {
    let tempfile = tempfile_in(tempdir)?;
    let mut zip = ZipWriter::new(tempfile);

    recursively_add_directory_to_zipfile(&mut zip, dir.path(), dir.path())?;

    Ok(zip.finish()?)
}

fn recursively_add_directory_to_zipfile(
    zip: &mut ZipWriter<std::fs::File>,
    root_path: &Utf8Path,
    dir_path: &Utf8Path,
) -> Result<()> {
    // Readdir might return entries in a non-deterministic order.
    // Let's sort it for the zipfile, to be nice.
    let mut entries = dir_path
        .read_dir_utf8()?
        .filter_map(Result::ok)
        .collect::<Vec<Utf8DirEntry>>();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    for entry in &entries {
        // Strip the tempdir prefix when storing the path in the zipfile.
        let dst = entry.path().strip_prefix(root_path)?;

        let file_type = entry.file_type()?;
        if file_type.is_file() {
            let src = entry.path();

            let zip_time = entry
                .path()
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|sys_time| jiff::Zoned::try_from(sys_time).ok())
                .and_then(|zoned| {
                    ::zip::DateTime::try_from(zoned.datetime()).ok()
                })
                .unwrap_or_else(::zip::DateTime::default);

            let opts = FullFileOptions::default()
                .last_modified_time(zip_time)
                .compression_method(::zip::CompressionMethod::Deflated)
                .large_file(true);

            zip.start_file_from_path(dst, opts)?;
            let mut file = std::fs::File::open(&src)?;
            std::io::copy(&mut file, zip)?;
        }
        if file_type.is_dir() {
            let opts = FullFileOptions::default();
            zip.add_directory_from_path(dst, opts)?;
            recursively_add_directory_to_zipfile(zip, root_path, entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use camino_tempfile::tempdir;

    // Ensure that we can convert a temporary directory into a zipfile
    #[test]
    fn test_zipfile_creation() {
        let dir = tempdir().unwrap();
        let tempdir_for_zip = tempdir().unwrap();

        std::fs::create_dir_all(dir.path().join("dir-a")).unwrap();
        std::fs::create_dir_all(dir.path().join("dir-b")).unwrap();
        std::fs::write(dir.path().join("dir-a").join("file-a"), "some data")
            .unwrap();
        std::fs::write(dir.path().join("file-b"), "more data").unwrap();

        let zipfile = bundle_to_zipfile(&dir, tempdir_for_zip.path())
            .expect("Should have been able to bundle zipfile");
        let archive = ::zip::read::ZipArchive::new(zipfile).unwrap();

        // We expect the order to be deterministically alphabetical
        let mut names = archive.file_names();
        assert_eq!(names.next(), Some("dir-a/"));
        assert_eq!(names.next(), Some("dir-a/file-a"));
        assert_eq!(names.next(), Some("dir-b/"));
        assert_eq!(names.next(), Some("file-b"));
        assert_eq!(names.next(), None);
    }
}
