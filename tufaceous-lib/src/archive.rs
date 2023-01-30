// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support for reading and writing zip archives.

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
use debug_ignore::DebugIgnore;
use fs_err::File;
use std::{
    fmt,
    io::{BufReader, BufWriter, Cursor, Read, Seek},
};
use zip::{write::FileOptions, CompressionMethod, ZipArchive, ZipWriter};

/// A builder for TUF repo archives.
#[derive(Debug)]
pub(crate) struct ArchiveBuilder {
    writer: DebugIgnore<ZipWriter<BufWriter<File>>>,
    // Stored for better error messages.
    output_path: Utf8PathBuf,
}

/// Defines the base directory for TUF repo archives created by this tool.
///
/// The usual convention is that the base dir is the name of the archive
/// (e.g. foo-1.0 for foo-1.0.zip). but just using a consistent name here
/// simplifies the code that extracts the archive.
pub const ZIP_BASE_DIR: &str = "repo";

impl ArchiveBuilder {
    /// Creates a new `ArchiveBuilder`, writing to the given path.
    pub fn new(output_path: Utf8PathBuf) -> Result<Self> {
        // The filename must end with "zip".
        if output_path.extension() != Some("zip") {
            bail!("output path `{output_path}` must end with .zip");
        }

        let file = File::create(&output_path)?;
        let writer = ZipWriter::new(BufWriter::new(file));
        Ok(Self { writer: writer.into(), output_path })
    }

    /// Writes the given path to the archive at the name `name`.
    ///
    /// The name has [`Self::ZIP_BASE_DIR`] prepended to it.
    pub fn write_file(
        &mut self,
        path: &Utf8Path,
        name: &Utf8Path,
    ) -> Result<()> {
        let name = Utf8Path::new(ZIP_BASE_DIR).join(name);

        self.writer.start_file(name.as_str(), Self::file_options())?;
        let mut reader = fs_err::File::open(path)?;
        std::io::copy(&mut reader, &mut *self.writer).with_context(|| {
            format!(
                "error writing `{path}` to archive at `{}`",
                self.output_path
            )
        })?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        let zip_file = self.writer.finish().with_context(|| {
            format!("error finalizing archive at `{}`", self.output_path)
        })?;
        zip_file.into_inner().with_context(|| {
            format!("error writing archive at `{}`", self.output_path)
        })?;

        Ok(())
    }

    fn file_options() -> FileOptions {
        // The main purpose of the zip archive is to transmit archives that are
        // already compressed, so there's no point trying to re-compress them.
        FileOptions::default().compression_method(CompressionMethod::Stored)
    }
}

/// An extractor for archives created by tufaceous.
///
/// Ideally we'd just be able to read the TUF repo out of a zip archive in
/// memory, but sadly that isn't possible today due to a missing lifetime
/// parameter on `Transport::fetch`. See
/// https://github.com/awslabs/tough/pull/563.
#[derive(Debug)]
pub struct ArchiveExtractor<R> {
    archive: ZipArchive<R>,
}

impl ArchiveExtractor<BufReader<File>> {
    /// Loads an archived repository from the given path.
    ///
    /// The archive must be a zip file generated by tufaceous.
    ///
    /// This method enforces expirations. To load without expiration enforcement, use
    /// [`Self::load_ignore_expiration`].
    pub fn from_path(zip_path: &Utf8Path) -> Result<Self> {
        let reader = BufReader::new(File::open(zip_path)?);
        Self::new(reader).with_context(|| {
            format!("error opening zip archive at `{zip_path}`")
        })
    }
}

impl<'a> ArchiveExtractor<Cursor<&'a [u8]>> {
    /// Loads an archived repository from memory as borrowed bytes.
    pub fn from_borrowed_bytes(archive: &'a [u8]) -> Result<Self> {
        let reader = Cursor::new(archive);
        Self::new(reader)
            .with_context(|| format!("error opening zip archive from memory"))
    }
}

impl ArchiveExtractor<Cursor<Bytes>> {
    /// Loads an archived repository from memory as owned bytes.
    pub fn from_owned_bytes(archive: impl Into<Bytes>) -> Result<Self> {
        let reader = Cursor::new(archive.into());
        Self::new(reader)
            .with_context(|| format!("error opening zip archive from memory"))
    }
}

impl<R> ArchiveExtractor<R>
where
    R: Read + Seek,
{
    fn new(reader: R) -> Result<ArchiveExtractor<R>> {
        // Validate the archive to ensure all paths are correctly formed.
        let archive = Self::validate(ZipArchive::new(reader)?)?;

        Ok(Self { archive })
    }

    fn validate(mut archive: ZipArchive<R>) -> Result<ZipArchive<R>> {
        for i in 0..archive.len() {
            let zip_file = archive.by_index(i).with_context(|| {
                format!("error reading file number `{i} from archive")
            })?;
            if !zip_file.is_file() {
                bail!("archive must consist only of files, not directories");
            }
            let path = Utf8Path::new(zip_file.name());
            validate_path(path).map_err(|error| {
                anyhow!("invalid path in archive `{path}`: {error}")
            })?;
        }

        Ok(archive)
    }

    /// Extracts this archive into the specified directory.
    ///
    /// Once this is completed, use [`OmicronRepo::load`] to load the archive
    /// from `output_dir`.
    ///
    /// `ZIP_BASE_DIR` will be stripped from the path.
    pub fn extract(&mut self, output_dir: &Utf8Path) -> Result<()> {
        for i in 0..self.archive.len() {
            let mut zip_file = self.archive.by_index(i).with_context(|| {
                format!("error reading file number `{i} from archive")
            })?;
            // SAFETY: file names have already been checked in `Self::validate`.
            let file_name = Utf8Path::new(zip_file.name()).to_owned();
            let dest_path = output_dir.join(
                file_name
                    .strip_prefix(ZIP_BASE_DIR)
                    .expect("checked in Self::validate"),
            );
            // The file is in a directory.
            fs_err::create_dir_all(
                dest_path.parent().expect("at least 1 component"),
            )?;

            let mut writer = File::create(&dest_path)?;
            std::io::copy(&mut zip_file, &mut writer).with_context(|| {
                format!(
                    "error writing `{file_name}` in archive to `{dest_path}`"
                )
            })?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum InvalidPath<'a> {
    AbsolutePath,
    ExactlyBaseDir,
    IncorrectBaseDir,
    InvalidComponent(Utf8Component<'a>),
}

impl<'a> fmt::Display for InvalidPath<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidPath::AbsolutePath => {
                write!(f, "path is absolute -- expected relative paths")
            }
            InvalidPath::ExactlyBaseDir => {
                write!(f, "path is exactly `{ZIP_BASE_DIR}` -- expected `{ZIP_BASE_DIR}/<foo>`")
            }
            InvalidPath::IncorrectBaseDir => {
                write!(f, "invalid base directory -- must be `{ZIP_BASE_DIR}`")
            }
            InvalidPath::InvalidComponent(component) => {
                write!(f, "invalid component `{component}`")
            }
        }
    }
}

fn validate_path(path: &Utf8Path) -> Result<(), InvalidPath<'_>> {
    if path.is_absolute() {
        return Err(InvalidPath::AbsolutePath);
    }
    if path == ZIP_BASE_DIR {
        return Err(InvalidPath::ExactlyBaseDir);
    }
    if !path.starts_with(ZIP_BASE_DIR) {
        return Err(InvalidPath::IncorrectBaseDir);
    }

    for component in path.components() {
        if !matches!(component, Utf8Component::Normal(_)) {
            return Err(InvalidPath::InvalidComponent(component));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_path() {
        let valid = ["repo/foo", "repo/foo/bar", "repo/foo/./bar", "repo//foo"];
        let invalid = [
            ("repo", InvalidPath::ExactlyBaseDir),
            ("repo/", InvalidPath::ExactlyBaseDir),
            ("repo/.", InvalidPath::ExactlyBaseDir),
            ("not-repo", InvalidPath::IncorrectBaseDir),
            ("not-repo/foo", InvalidPath::IncorrectBaseDir),
            (
                "repo/..",
                InvalidPath::InvalidComponent(Utf8Component::ParentDir),
            ),
            ("/repo/foo", InvalidPath::AbsolutePath),
        ];

        for path in valid {
            validate_path(Utf8Path::new(path)).unwrap_or_else(|err| {
                panic!("expected path `{path}` to be valid: {err}")
            });
        }

        for (path, expected) in invalid {
            eprintln!("testing invalid path: `{path}`");
            let actual = match validate_path(Utf8Path::new(path)) {
                Ok(()) => panic!("expected path `{path}` to be invalid"),
                Err(error) => error,
            };

            assert_eq!(
                actual, expected,
                "for path `{path}`, InvalidPath error should match"
            );
        }
    }
}
