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
//! file itself is not added, and its entries are copied as-is, without being
//! decompressed, under the directory that contains it. The collected directory therefore
//! does not match the bundle's layout exactly.
//!
//! Merged entries may collide with each other or with files on disk. The
//! first entry of a given name is kept, and later ones are skipped.
//!
//! A merge zip that cannot be read, or entries of it that cannot be read, are
//! skipped rather than failing the bundle. What was skipped, and why, is
//! recorded in the bundle in an entry named for the zip with `.err` appended:
//! `logs/zone/logs.merge.zip.err`, for example.

use ::zip::ZipWriter;
use ::zip::write::FullFileOptions;
use anyhow::Context;
use anyhow::Result;
use camino::Utf8Component;
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
/// copied into the bundle as-is, without being decompressed, under the
/// directory that contains the file. For example, an entry named `svc/current/svc.log` within
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

fn write_zip<W: Write + std::io::Seek>(
    dir: &Utf8TempDir,
    zip: ZipWriter<W>,
) -> Result<()> {
    let mut bundle = BundleZip::new(zip);
    recursively_add_directory_to_zipfile(&mut bundle, dir.path(), dir.path())?;
    bundle.finish()
}

/// A bundle zip being written, which skips any entry whose name it already
/// holds.
///
/// `ZipWriter` rejects duplicate names, and merged zips can produce names that
/// collide with each other or with files on disk. Writing every entry through
/// this type keeps the first entry of a name and skips later ones, rather than
/// failing to build the bundle.
struct BundleZip<W: Write + std::io::Seek> {
    zip: ZipWriter<W>,
    // Names of the entries in `zip`, with directories ending in `/`.
    // `ZipWriter` tracks these too, but does not expose them.
    //
    // Each name is built by `name_components`, and that same string is what
    // is written to `zip`, so that this set agrees with `ZipWriter` on which
    // names are duplicates.
    names: BTreeSet<String>,
}

/// Splits `path` into the components of its zip entry name, resolving `.` and
/// `..` and dropping repeated or trailing separators.
fn name_components(path: &Utf8Path) -> Vec<&str> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Utf8Component::Normal(component) => components.push(component),
            Utf8Component::ParentDir => {
                components.pop();
            }
            Utf8Component::CurDir
            | Utf8Component::RootDir
            | Utf8Component::Prefix(_) => {}
        }
    }
    components
}

impl<W: Write + std::io::Seek> BundleZip<W> {
    fn new(zip: ZipWriter<W>) -> Self {
        Self { zip, names: BTreeSet::new() }
    }

    /// Records the entry name for the file at `path`, returning it if no entry
    /// of that name is present.
    fn insert_file_name(&mut self, path: &Utf8Path) -> Option<String> {
        let name = name_components(path).join("/");
        (!name.is_empty() && self.names.insert(name.clone())).then_some(name)
    }

    /// Adds directory entries for `path` and each of its ancestors, from the
    /// outermost in, skipping any already present.
    fn add_dir_all(&mut self, path: &Utf8Path) -> Result<()> {
        let components = name_components(path);
        for i in 1..=components.len() {
            let name = format!("{}/", components[..i].join("/"));
            if self.names.insert(name.clone()) {
                self.zip.add_directory(name, FullFileOptions::default())?;
            }
        }
        Ok(())
    }

    /// Adds the file at `src` as `dst`, unless an entry named `dst` is
    /// present.
    fn add_file(&mut self, dst: &Utf8Path, src: &Utf8Path) -> Result<()> {
        let Some(name) = self.insert_file_name(dst) else {
            return Ok(());
        };

        let zip_time = src
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|sys_time| jiff::Zoned::try_from(sys_time).ok())
            .and_then(|zoned| ::zip::DateTime::try_from(zoned.datetime()).ok())
            .unwrap_or_else(::zip::DateTime::default);

        let opts = FullFileOptions::default()
            .last_modified_time(zip_time)
            .compression_method(compression_method_for(src))
            .large_file(true);

        self.zip.start_file(name, opts)?;
        let mut file = std::fs::File::open(&src)?;
        std::io::copy(&mut file, &mut self.zip)?;
        Ok(())
    }

    /// Copies `entry` as `dst` without decompressing it, unless an entry named
    /// `dst` is present.
    fn raw_copy<R: std::io::Read>(
        &mut self,
        entry: ::zip::read::ZipFile<'_, R>,
        dst: &Utf8Path,
    ) -> Result<()> {
        if let Some(name) = self.insert_file_name(dst) {
            self.zip.raw_copy_file_rename(entry, name)?;
        }
        Ok(())
    }

    /// Adds `contents` as a file named `dst`, unless an entry named `dst` is
    /// present.
    fn add_text(&mut self, dst: &Utf8Path, contents: &str) -> Result<()> {
        if let Some(name) = self.insert_file_name(dst) {
            self.zip.start_file(name, FullFileOptions::default())?;
            self.zip.write_all(contents.as_bytes())?;
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.zip.finish()?;
        Ok(())
    }
}

/// Adds the contents of `dir_path` to `bundle`.
fn recursively_add_directory_to_zipfile<W: Write + std::io::Seek>(
    bundle: &mut BundleZip<W>,
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
            merge_zip_entries(bundle, entry.path(), dst_dir)?;
        } else if file_type.is_file() {
            bundle.add_file(dst, entry.path())?;
        }
        if file_type.is_dir() {
            bundle.add_dir_all(dst)?;
            recursively_add_directory_to_zipfile(
                bundle,
                root_path,
                entry.path(),
            )?;
        }
    }
    Ok(())
}

/// Copies each entry of the zip at `src` into `bundle` under `dst_dir`,
/// without decompressing it.
///
/// Directory entries are added for any directories leading to each copied
/// entry. Entries are skipped if an entry of the same name is already in the
/// bundle.
///
/// The zip is skipped if it cannot be read, and so is each entry that cannot be
/// read or whose name would place it outside of `dst_dir`. Each is checked
/// before anything of it is written to the bundle, and what was skipped is
/// recorded in the bundle; see the [module documentation](self#bundle-contents).
/// An error is returned only if writing to the bundle fails.
fn merge_zip_entries<W: Write + std::io::Seek>(
    bundle: &mut BundleZip<W>,
    src: &Utf8Path,
    dst_dir: &Utf8Path,
) -> Result<()> {
    let mut skipped = Vec::new();
    match open_zip(src) {
        Ok((mut archive, zip_len)) => {
            for i in 0..archive.len() {
                let name =
                    archive.name_for_index(i).unwrap_or_default().to_string();
                let entry = match archive.by_index_raw(i) {
                    Ok(entry) => entry,
                    Err(err) => {
                        skipped.push(format!("entry {name:?}: {err}"));
                        continue;
                    }
                };
                // Raw copies write as many bytes as the entry claims, without
                // checking that the zip holds that many.
                let data_end =
                    entry.data_start().checked_add(entry.compressed_size());
                if data_end.is_none_or(|end| end > zip_len) {
                    skipped.push(format!(
                        "entry {name:?}: data extends past the end of the zip"
                    ));
                    continue;
                }
                let Some(relative) = entry
                    .enclosed_name()
                    .and_then(|path| Utf8PathBuf::try_from(path).ok())
                    .filter(|path| !path.as_str().is_empty())
                else {
                    skipped.push(format!(
                        "entry {name:?}: name is outside of the zip's directory"
                    ));
                    continue;
                };
                let path = dst_dir.join(&relative);

                if entry.is_dir() {
                    bundle.add_dir_all(&path)?;
                } else {
                    if let Some(parent) = path.parent() {
                        bundle.add_dir_all(parent)?;
                    }
                    bundle.raw_copy(entry, &path)?;
                }
            }
        }
        Err(err) => skipped.push(format!("{err:#}")),
    }

    if !skipped.is_empty() {
        let zip_name = dst_dir.join(src.file_name().unwrap_or_default());
        let mut note =
            format!("Skipped while merging {zip_name} into the bundle:\n");
        for line in skipped {
            note.push_str(&line);
            note.push('\n');
        }
        bundle
            .add_text(&Utf8PathBuf::from(format!("{zip_name}.err")), &note)?;
    }
    Ok(())
}

/// Opens the zip at `path`, returning it along with its length in bytes.
fn open_zip(
    path: &Utf8Path,
) -> Result<(::zip::ZipArchive<std::fs::File>, u64)> {
    let file = std::fs::File::open(path).context("failed to open zip")?;
    let len = file.metadata().context("failed to read zip metadata")?.len();
    let archive = ::zip::ZipArchive::new(file).context("failed to read zip")?;
    Ok((archive, len))
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

    /// Builds a zip whose entries are stored without zip compression, like
    /// the log zips from sled agents. Names are used as-is, without sanitizing
    /// them.
    fn stored_zip(entries: &[(&str, &str)]) -> Vec<u8> {
        let options = FullFileOptions::default()
            .compression_method(::zip::CompressionMethod::Stored);
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
    // as-is, under the directory containing the merge zip.
    #[test]
    fn test_merge_zip_entries() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("file-a"), "plain data").unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(&zone_dir).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            stored_zip(&[
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
                (::zip::CompressionMethod::Stored, "current data".to_string())
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/svc/archive/svc.log.1"),
                (::zip::CompressionMethod::Stored, "archived data".to_string())
            );
        }
    }

    // Ensure that logs which sled agents store as zstd files, in entries
    // without zip compression, are copied into the bundle byte for byte.
    #[test]
    fn test_merge_zip_stored_zstd_logs() {
        let log = "log data ".repeat(100);
        let compressed = zstd::encode_all(log.as_bytes(), 3).unwrap();
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        zip.start_file(
            "svc/current/svc.log.zst",
            FullFileOptions::default()
                .compression_method(::zip::CompressionMethod::Stored),
        )
        .unwrap();
        zip.write_all(&compressed).unwrap();
        let merge_zip = zip.finish().unwrap().into_inner();

        let dir = tempdir().unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(&zone_dir).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            merge_zip,
        )
        .unwrap();

        for buf in bundle_both_ways(&dir) {
            let mut archive =
                ::zip::read::ZipArchive::new(Cursor::new(buf)).unwrap();
            let mut entry =
                archive.by_name("logs/zone-a/svc/current/svc.log.zst").unwrap();
            assert_eq!(entry.compression(), ::zip::CompressionMethod::Stored);
            let mut contents = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut contents).unwrap();
            assert_eq!(contents, compressed);
            assert_eq!(
                zstd::decode_all(contents.as_slice()).unwrap(),
                log.as_bytes()
            );
        }
    }

    // Ensure that merging skips entries that would escape the merge zip's
    // directory, recording them, and entries whose names are already in the
    // bundle, rather than failing to build the bundle.
    #[test]
    fn test_merge_zip_skips_unsafe_and_duplicate_entries() {
        let dir = tempdir().unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(zone_dir.join("svc/current")).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            stored_zip(&[
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
                    "logs/zone-a/logs.merge.zip.err",
                ]
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/svc/current/svc.log"),
                (::zip::CompressionMethod::Stored, "merged data".to_string())
            );
            let (_, err) =
                read_entry(&mut archive, "logs/zone-a/logs.merge.zip.err");
            assert!(err.contains("\"../../escape.log\""), "{err}");
        }
    }

    // Ensure that a merge zip that cannot be read is skipped and recorded in
    // an `.err` entry, while a readable merge zip beside it is expanded, with
    // no `.err` entry of its own.
    #[test]
    fn test_merge_zip_unreadable_zip() {
        let dir = tempdir().unwrap();
        let zone_a = dir.path().join("logs/zone-a");
        let zone_b = dir.path().join("logs/zone-b");
        std::fs::create_dir_all(&zone_a).unwrap();
        std::fs::create_dir_all(&zone_b).unwrap();
        std::fs::write(
            zone_a.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            "not a zip",
        )
        .unwrap();
        std::fs::write(
            zone_b.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            stored_zip(&[("svc.log", "zone b data")]),
        )
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
                    "logs/zone-a/logs.merge.zip.err",
                    "logs/zone-b/",
                    "logs/zone-b/svc.log",
                ]
            );
            let (_, err) =
                read_entry(&mut archive, "logs/zone-a/logs.merge.zip.err");
            assert!(err.contains("failed to read zip"), "{err}");
            assert_eq!(
                read_entry(&mut archive, "logs/zone-b/svc.log").1,
                "zone b data"
            );
        }
    }

    // Ensure that entries of a merge zip that cannot be read are skipped and
    // recorded, and the rest are copied: one whose local header is damaged,
    // and one whose data would extend past the end of the zip.
    #[test]
    fn test_merge_zip_damaged_entries() {
        let mut merge_zip = stored_zip(&[
            ("a.log", "a data"),
            ("b.log", "b data"),
            ("c.log", "c data"),
        ]);
        let offsets = |signature: &[u8]| -> Vec<usize> {
            merge_zip
                .windows(signature.len())
                .enumerate()
                .filter(|(_, window)| *window == signature)
                .map(|(i, _)| i)
                .collect()
        };
        // Damage the signature of b.log's local header.
        let local_headers = offsets(b"PK\x03\x04");
        // Claim, in c.log's central directory header, more compressed data
        // than the zip holds.
        let central_headers = offsets(b"PK\x01\x02");
        merge_zip[local_headers[1]] = b'X';
        let compressed_size = central_headers[2] + 20;
        merge_zip[compressed_size..compressed_size + 4]
            .copy_from_slice(&0x7fff_ffffu32.to_le_bytes());

        let dir = tempdir().unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(&zone_dir).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            merge_zip,
        )
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
                    "logs/zone-a/a.log",
                    "logs/zone-a/logs.merge.zip.err",
                ]
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/a.log").1,
                "a data"
            );
            let (_, err) =
                read_entry(&mut archive, "logs/zone-a/logs.merge.zip.err");
            let lines: Vec<_> = err.lines().collect();
            assert_eq!(lines.len(), 3, "{err}");
            assert!(lines[1].starts_with("entry \"b.log\": "), "{err}");
            assert_eq!(
                lines[2],
                "entry \"c.log\": data extends past the end of the zip"
            );
        }
    }

    // Ensure that a merge zip's directory entries are added once each,
    // whether they come before or after the files within them.
    #[test]
    fn test_merge_zip_directory_entries() {
        let options = FullFileOptions::default()
            .compression_method(::zip::CompressionMethod::Stored);
        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        zip.add_directory("svc/", options.clone()).unwrap();
        zip.start_file("svc/current/svc.log", options.clone()).unwrap();
        zip.write_all(b"current data").unwrap();
        zip.start_file("svc/archive/svc.log.1", options.clone()).unwrap();
        zip.write_all(b"archived data").unwrap();
        zip.add_directory("svc/archive/", options).unwrap();
        let merge_zip = zip.finish().unwrap().into_inner();

        let dir = tempdir().unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(&zone_dir).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            merge_zip,
        )
        .unwrap();

        for buf in bundle_both_ways(&dir) {
            let archive =
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
                    "logs/zone-a/svc/archive/",
                    "logs/zone-a/svc/archive/svc.log.1",
                ]
            );
        }
    }

    // Ensure that merged entry names are normalized, so that names which
    // differ only in `.`, `..`, or repeated separators are treated as
    // duplicates.
    #[test]
    fn test_merge_zip_normalizes_names() {
        let dir = tempdir().unwrap();
        let zone_dir = dir.path().join("logs/zone-a");
        std::fs::create_dir_all(&zone_dir).unwrap();
        std::fs::write(
            zone_dir.join(format!("logs{MERGE_ZIP_SUFFIX}")),
            stored_zip(&[
                ("svc/./a.log", "first a"),
                ("svc/a.log", "second a"),
                ("svc//b.log", "first b"),
                ("svc/x/../b.log", "second b"),
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
                    "logs/",
                    "logs/zone-a/",
                    "logs/zone-a/svc/",
                    "logs/zone-a/svc/a.log",
                    "logs/zone-a/svc/b.log",
                ]
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/svc/a.log").1,
                "first a"
            );
            assert_eq!(
                read_entry(&mut archive, "logs/zone-a/svc/b.log").1,
                "first b"
            );
        }
    }
}
