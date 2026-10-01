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
//!
//! # Bundle contents
//!
//! The bundle mirrors the collected directory: each file and directory
//! within it becomes an entry of the same relative path, with one exception.
//! A file whose name ends in [`MERGE_ZIP_SUFFIX`] is expanded in place: the
//! file itself is not added, and its entries are copied, still compressed,
//! under the directory that contains it. The collected directory therefore
//! does not match the bundle's layout exactly.
//!
//! Merged entries may collide with each other or with files on disk. The
//! first entry of a given name is kept, and later ones are skipped.
//!
//! [`prepare_zone_log_zip`] runs earlier, during collection, on each zone's
//! log zip as it arrives from a sled agent. It marks the zip for merging, so
//! that the bundle copies its entries without decompressing them.

use ::zip::ZipWriter;
use ::zip::write::FullFileOptions;
use anyhow::Context;
use anyhow::Result;
use camino::Utf8DirEntry;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use camino_tempfile::tempfile_in;
use std::collections::BTreeSet;
use std::io::Write;

/// Suffix of a file, within a collected bundle directory, whose zip entries
/// are merged into the bundle.
///
/// The file itself is not added to the bundle. Instead, each of its entries is
/// copied into the bundle, still compressed, under the directory that contains
/// the file. For example, an entry named `svc/current/svc.log` within
/// `logs/zone/logs.merge.zip` becomes `logs/zone/svc/current/svc.log` in the
/// bundle.
pub const MERGE_ZIP_SUFFIX: &str = ".merge.zip";

/// Write a bundle zip of `dir` into a seekable destination. Produces a
/// standard zip (no data descriptors).
///
/// See the [module documentation](self#bundle-contents) for how `dir` maps to
/// the zip's entries.
pub fn bundle_to_writer<W: Write + std::io::Seek>(
    dir: &Utf8TempDir,
    writer: W,
) -> Result<()> {
    write_zip(dir, ZipWriter::new(writer))
}

/// Write a bundle zip into a non-seekable destination. The resulting
/// archive uses zip data descriptors (~16 bytes of overhead per entry)
/// and is readable by any standard unzip tool.
///
/// See the [module documentation](self#bundle-contents) for how `dir` maps to
/// the zip's entries.
pub fn bundle_to_stream<W: Write>(dir: &Utf8TempDir, writer: W) -> Result<()> {
    write_zip(dir, ZipWriter::new_stream(writer))
}

/// Zip the contents of `dir` into a tempfile under `tempdir` and return
/// the owned file handle. Used by Nexus's chunked-upload path.
///
/// See the [module documentation](self#bundle-contents) for how `dir` maps to
/// the zip's entries.
pub fn bundle_to_zipfile(
    dir: &Utf8TempDir,
    tempdir: &Utf8Path,
) -> Result<std::fs::File> {
    let mut tempfile = tempfile_in(tempdir)?;
    bundle_to_writer(dir, &mut tempfile)?;
    Ok(tempfile)
}

/// Prepare a zone's log zip, downloaded from a sled agent to `zip_path`, for
/// inclusion in the bundle.
///
/// The zip is renamed to end in [`MERGE_ZIP_SUFFIX`], so that its entries are
/// copied into the bundle without being decompressed. It is first checked to
/// be a readable zip; if it is not, it keeps its name and an error is
/// returned.
pub fn prepare_zone_log_zip(zip_path: &Utf8Path) -> Result<()> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("failed to open zip file: {zip_path}"))?;
    ::zip::ZipArchive::new(file)
        .with_context(|| format!("failed to read log zip file: {zip_path}"))?;

    let file_stem = zip_path
        .file_stem()
        .with_context(|| format!("log zip has no file name: {zip_path}"))?;
    let merge_path =
        zip_path.with_file_name(format!("{file_stem}{MERGE_ZIP_SUFFIX}"));
    std::fs::rename(zip_path, &merge_path).with_context(|| {
        format!("failed to rename log zip file to: {merge_path}")
    })?;
    Ok(())
}

fn write_zip<W: Write + std::io::Seek>(
    dir: &Utf8TempDir,
    mut zip: ZipWriter<W>,
) -> Result<()> {
    let mut names = BTreeSet::new();
    recursively_add_directory_to_zipfile(
        &mut zip,
        &mut names,
        dir.path(),
        dir.path(),
    )?;
    zip.finish()?;
    Ok(())
}

/// Adds the contents of `dir_path` to `zip`, recording the name of each entry
/// added in `names`.
///
/// `ZipWriter` rejects duplicate names, and merged zips can produce names that
/// collide with each other or with files on disk. Callers share one `names`
/// across the whole bundle so that the first entry of a name wins and later
/// ones are skipped. Directory names are recorded with a trailing `/`.
fn recursively_add_directory_to_zipfile<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    names: &mut BTreeSet<String>,
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
        if file_type.is_file() && entry.file_name().ends_with(MERGE_ZIP_SUFFIX)
        {
            let dst_dir = dst.parent().unwrap_or(Utf8Path::new(""));
            merge_zip_entries(zip, names, entry.path(), dst_dir)?;
        } else if file_type.is_file() {
            // A merged zip may already have added an entry of this name.
            if !names.insert(dst.to_string()) {
                continue;
            }
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
                .compression_method(compression_method_for(src))
                .large_file(true);

            zip.start_file_from_path(dst, opts)?;
            let mut file = std::fs::File::open(&src)?;
            std::io::copy(&mut file, zip)?;
        }
        if file_type.is_dir() {
            if names.insert(format!("{dst}/")) {
                let opts = FullFileOptions::default();
                zip.add_directory_from_path(dst, opts)?;
            }
            recursively_add_directory_to_zipfile(
                zip,
                names,
                root_path,
                entry.path(),
            )?;
        }
    }
    Ok(())
}

/// Copies each entry of the zip at `src` into `zip` under `dst_dir`, without
/// decompressing it.
///
/// Directory entries are added for any directories between `dst_dir` and each
/// copied entry. Entries are skipped if their names would place them outside
/// of `dst_dir`, or if an entry of the same name is already in the bundle.
fn merge_zip_entries<W: Write + std::io::Seek>(
    zip: &mut ZipWriter<W>,
    names: &mut BTreeSet<String>,
    src: &Utf8Path,
    dst_dir: &Utf8Path,
) -> Result<()> {
    let file = std::fs::File::open(src)
        .with_context(|| format!("failed to open zip file: {src}"))?;
    let mut archive = ::zip::ZipArchive::new(file)
        .with_context(|| format!("failed to read zip file: {src}"))?;
    for i in 0..archive.len() {
        let entry = archive.by_index_raw(i)?;
        let Some(relative) = entry
            .enclosed_name()
            .and_then(|path| Utf8PathBuf::try_from(path).ok())
            .filter(|path| !path.as_str().is_empty())
        else {
            continue;
        };
        let path = dst_dir.join(&relative);

        // Add the directories leading to this entry, from the outermost in.
        let mut dirs: Vec<_> = path
            .ancestors()
            .skip(1)
            .take(relative.components().count() - 1)
            .collect();
        if entry.is_dir() {
            dirs.insert(0, &path);
        }
        for dir in dirs.into_iter().rev() {
            if names.insert(format!("{dir}/")) {
                zip.add_directory_from_path(dir, FullFileOptions::default())?;
            }
        }
        if entry.is_dir() {
            continue;
        }

        if names.insert(path.to_string()) {
            zip.raw_copy_file_rename(entry, path.as_str())?;
        }
    }
    Ok(())
}

/// Chooses how to compress a file within the bundle.
///
/// Files that are already compressed, such as the SP task dump zips, are
/// stored as-is: deflating them again costs CPU and does not make them
/// smaller.
fn compression_method_for(path: &Utf8Path) -> ::zip::CompressionMethod {
    match path.extension() {
        Some("zip" | "gz" | "zst") => ::zip::CompressionMethod::Stored,
        _ => ::zip::CompressionMethod::Deflated,
    }
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

    // Ensure that already-compressed files are stored without being deflated
    // again, and that everything else is still deflated.
    #[test]
    fn test_compressed_files_are_stored() {
        let dir = tempdir().unwrap();
        for name in ["dump-0.zip", "fake.gz", "fake.zst", "plain.txt"] {
            std::fs::write(dir.path().join(name), "not really compressed")
                .unwrap();
        }

        let mut seekable = Cursor::new(Vec::new());
        bundle_to_writer(&dir, &mut seekable).unwrap();
        let mut streamed: Vec<u8> = Vec::new();
        bundle_to_stream(&dir, &mut streamed).unwrap();

        for buf in [seekable.into_inner(), streamed] {
            let mut archive =
                ::zip::read::ZipArchive::new(Cursor::new(buf)).unwrap();
            for (name, expected) in [
                ("dump-0.zip", ::zip::CompressionMethod::Stored),
                ("fake.gz", ::zip::CompressionMethod::Stored),
                ("fake.zst", ::zip::CompressionMethod::Stored),
                ("plain.txt", ::zip::CompressionMethod::Deflated),
            ] {
                let mut entry = archive.by_name(name).unwrap();
                assert_eq!(entry.compression(), expected, "{name}");
                let mut contents = String::new();
                std::io::Read::read_to_string(&mut entry, &mut contents)
                    .unwrap();
                assert_eq!(contents, "not really compressed", "{name}");
            }
        }
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

    /// Builds a zip whose entries are zstd-compressed, like the log zips from
    /// sled agents. Names are used as-is, without sanitizing them.
    fn zstd_zip(entries: &[(&str, &str)]) -> Vec<u8> {
        let options = FullFileOptions::default()
            .compression_method(::zip::CompressionMethod::Zstd);
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        for (name, contents) in entries {
            zip.start_file(*name, options.clone()).unwrap();
            zip.write_all(contents.as_bytes()).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    /// Zips `dir` with both the seekable and the streaming writer.
    fn bundle_both_ways(dir: &Utf8TempDir) -> [Vec<u8>; 2] {
        let mut seekable = Cursor::new(Vec::new());
        bundle_to_writer(dir, &mut seekable).unwrap();
        let mut streamed = Vec::new();
        bundle_to_stream(dir, &mut streamed).unwrap();
        [seekable.into_inner(), streamed]
    }

    fn read_entry(
        archive: &mut ::zip::read::ZipArchive<Cursor<Vec<u8>>>,
        name: &str,
    ) -> (::zip::CompressionMethod, String) {
        let mut entry = archive.by_name(name).unwrap();
        let mut contents = String::new();
        std::io::Read::read_to_string(&mut entry, &mut contents).unwrap();
        (entry.compression(), contents)
    }

    // Ensure that the entries of a merge zip are copied into the bundle,
    // still compressed, under the directory containing the merge zip.
    #[test]
    fn test_merge_zip_entries() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("file-a"), "plain data").unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(&zone_dir).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            zstd_zip(&[
                ("svc/current/svc.log", "current data"),
                ("svc/archive/svc.log.1", "archived data"),
            ]),
        )
        .unwrap();

        for buf in bundle_both_ways(&dir) {
            let mut archive =
                ::zip::read::ZipArchive::new(Cursor::new(buf)).unwrap();
            let names: Vec<_> = archive.file_names().collect();
            assert_eq!(
                names,
                [
                    "file-a",
                    "logs/",
                    "logs/zone-a/",
                    "logs/zone-a/svc/",
                    "logs/zone-a/svc/current/",
                    "logs/zone-a/svc/current/svc.log",
                    "logs/zone-a/svc/archive/",
                    "logs/zone-a/svc/archive/svc.log.1",
                ]
            );
            assert_eq!(
                read_entry(&mut archive, "file-a"),
                (::zip::CompressionMethod::Deflated, "plain data".to_string())
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/svc/current/svc.log"),
                (::zip::CompressionMethod::Zstd, "current data".to_string())
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/svc/archive/svc.log.1"),
                (::zip::CompressionMethod::Zstd, "archived data".to_string())
            );
        }
    }

    // Ensure that merging skips entries that would escape the merge zip's
    // directory, and entries whose names are already in the bundle, rather
    // than failing to build the bundle.
    #[test]
    fn test_merge_zip_skips_unsafe_and_duplicate_entries() {
        let dir = tempdir().unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(zone_dir.join("svc/current")).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            zstd_zip(&[
                ("../../escape.log", "escaped data"),
                ("svc/current/svc.log", "merged data"),
            ]),
        )
        .unwrap();
        // The directory walk reaches this file after the merge zip, which has
        // already added an entry of the same name.
        std::fs::write(zone_dir.join("svc/current/svc.log"), "on-disk data")
            .unwrap();

        for buf in bundle_both_ways(&dir) {
            let mut archive =
                ::zip::read::ZipArchive::new(Cursor::new(buf)).unwrap();
            let names: Vec<_> = archive.file_names().collect();
            assert_eq!(
                names,
                [
                    "logs/",
                    "logs/zone-a/",
                    "logs/zone-a/svc/",
                    "logs/zone-a/svc/current/",
                    "logs/zone-a/svc/current/svc.log",
                ]
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/svc/current/svc.log"),
                (::zip::CompressionMethod::Zstd, "merged data".to_string())
            );
        }
    }

    // Ensure that preparing a zone's log zip marks it for merging, and leaves
    // a file that is not a zip alone.
    #[test]
    fn test_prepare_zone_log_zip() {
        let dir = tempdir().unwrap();
        let zip_path = dir.path().join("logs.zip");
        let merge_path = dir.path().join(format!("logs{MERGE_ZIP_SUFFIX}"));

        std::fs::write(&zip_path, zstd_zip(&[("svc.log", "data")])).unwrap();
        prepare_zone_log_zip(&zip_path).unwrap();
        assert!(!zip_path.exists());
        assert!(merge_path.exists());
        std::fs::remove_file(&merge_path).unwrap();

        std::fs::write(&zip_path, "not a zip").unwrap();
        prepare_zone_log_zip(&zip_path)
            .expect_err("preparing a file that is not a zip should fail");
        assert!(zip_path.exists());
        assert!(!merge_path.exists());
    }
}
