// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for converting a collected bundle directory into a zip archive.
//!
//! Three entry points:
//!
//! - [`bundle_to_writer`] writes a standard zip into any `Write + Seek`
//!   sink. Used by omdb when `--output` is a regular file.
//! - [`bundle_to_stream`] writes a zip with data descriptors into a
//!   non-seekable sink. Used by omdb when streaming the bundle to stdout
//!   (e.g. to pipe over ssh from a switch zone).
//! - [`bundle_to_zipfile`] is a thin convenience that allocates a tempfile
//!   and delegates to [`bundle_to_writer`]. Retained for Nexus's
//!   chunked-upload path, which needs an owned seekable `File` for
//!   hashing + per-chunk `try_clone` / `seek`.

use ::zip::ZipWriter;
use ::zip::write::FullFileOptions;
use anyhow::Result;
use camino::Utf8DirEntry;
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use camino_tempfile::tempfile_in;
use std::io::Write;

/// Write a bundle zip into a seekable destination. Produces a standard
/// zip (no data descriptors).
pub fn bundle_to_writer<W: Write + std::io::Seek>(
    dir: &Utf8TempDir,
    writer: W,
) -> Result<()> {
    write_zip(dir, ZipWriter::new(writer))
}

/// Write a bundle zip into a non-seekable destination. The resulting
/// archive uses zip data descriptors (~16 bytes of overhead per entry)
/// and is readable by any standard unzip tool.
pub fn bundle_to_stream<W: Write>(dir: &Utf8TempDir, writer: W) -> Result<()> {
    write_zip(dir, ZipWriter::new_stream(writer))
}

/// Zip the contents of `dir` into a tempfile under `tempdir` and return
/// the owned file handle. Used by Nexus's chunked-upload path.
pub fn bundle_to_zipfile(
    dir: &Utf8TempDir,
    tempdir: &Utf8Path,
) -> Result<std::fs::File> {
    let mut tempfile = tempfile_in(tempdir)?;
    bundle_to_writer(dir, &mut tempfile)?;
    Ok(tempfile)
}

fn write_zip<W: Write + std::io::Seek>(
    dir: &Utf8TempDir,
    mut zip: ZipWriter<W>,
) -> Result<()> {
    recursively_add_directory_to_zipfile(&mut zip, dir.path(), dir.path())?;
    zip.finish()?;
    Ok(())
}

fn recursively_add_directory_to_zipfile<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
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
    use std::io::Cursor;

    fn make_sample_bundle() -> Utf8TempDir {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("dir-a")).unwrap();
        std::fs::create_dir_all(dir.path().join("dir-b")).unwrap();
        std::fs::write(dir.path().join("dir-a").join("file-a"), "some data")
            .unwrap();
        std::fs::write(dir.path().join("file-b"), "more data").unwrap();
        dir
    }

    fn assert_expected_entries<R: std::io::Read + std::io::Seek>(
        archive: ::zip::read::ZipArchive<R>,
    ) {
        let mut names = archive.file_names();
        assert_eq!(names.next(), Some("dir-a/"));
        assert_eq!(names.next(), Some("dir-a/file-a"));
        assert_eq!(names.next(), Some("dir-b/"));
        assert_eq!(names.next(), Some("file-b"));
        assert_eq!(names.next(), None);
    }

    // Ensure that bundle_to_writer produces a deterministically-ordered
    // archive when given a seekable destination.
    #[test]
    fn test_bundle_to_writer() {
        let dir = make_sample_bundle();
        let mut buf = Cursor::new(Vec::new());
        bundle_to_writer(&dir, &mut buf).unwrap();
        let archive = ::zip::read::ZipArchive::new(buf).unwrap();
        assert_expected_entries(archive);
    }

    // Ensure that bundle_to_stream produces the same archive contents
    // when given a non-seekable destination (using data descriptors).
    #[test]
    fn test_bundle_to_stream() {
        let dir = make_sample_bundle();
        let mut buf: Vec<u8> = Vec::new();
        bundle_to_stream(&dir, &mut buf).unwrap();
        let archive = ::zip::read::ZipArchive::new(Cursor::new(buf)).unwrap();
        assert_expected_entries(archive);
    }

    // Ensure that the tempfile-returning convenience still works for the
    // Nexus chunked-upload path.
    #[test]
    fn test_bundle_to_zipfile() {
        let dir = make_sample_bundle();
        let tempdir_for_zip = tempdir().unwrap();
        let zipfile = bundle_to_zipfile(&dir, tempdir_for_zip.path()).unwrap();
        let archive = ::zip::read::ZipArchive::new(zipfile).unwrap();
        assert_expected_entries(archive);
    }
}
