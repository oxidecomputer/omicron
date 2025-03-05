// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the environment the command is running in, and particularly where
//! different sets of specifications are loaded from

use crate::apis::ManagedApis;
use crate::git::GitRevision;
use crate::output::Styles;
use crate::output::headers::GENERATING;
use crate::output::headers::HEADER_WIDTH;
use crate::spec_files_blessed::BlessedFiles;
use crate::spec_files_generated::GeneratedFiles;
use crate::spec_files_local::LocalFiles;
use crate::spec_files_local::walk_local_directory;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use owo_colors::OwoColorize;

pub(crate) struct Environment {
    /// Path to the root of this Omicron workspace
    ///
    /// This is currently only used for finding "extra" files that are
    /// enumerated by the validation process, which is currently just the file
    /// describing external API tag configuration.
    pub(crate) workspace_root: Utf8PathBuf,

    /// Location of the OpenAPI documents in this workspace
    pub(crate) local_source: LocalSource,
}

impl Environment {
    pub(crate) fn new(
        openapi_dir: Option<Utf8PathBuf>,
    ) -> anyhow::Result<Self> {
        let mut workspace_root = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // This crate is two levels down from the root of omicron, so go up
        // twice.
        workspace_root.pop();
        workspace_root.pop();

        let openapi_dir =
            openapi_dir.unwrap_or_else(|| workspace_root.join("openapi"));
        let local_source =
            LocalSource::Directory { local_directory: openapi_dir };

        Ok(Self { workspace_root, local_source })
    }

    /// Returns the path to the OpenAPI documents in this workspace
    pub(crate) fn openapi_dir(&self) -> &Utf8Path {
        match &self.local_source {
            LocalSource::Directory { local_directory } => &local_directory,
        }
    }
}

/// Specifies where to find blessed OpenAPI documents (the ones that are
/// considered immutable because they've been committed-to upstream)
#[derive(Debug)]
pub enum BlessedSource {
    /// Blessed OpenAPI documents come from the Git merge base between `HEAD`
    /// and the specified revision (default "main"), in the specified directory.
    GitRevisionMergeBase { revision: GitRevision, directory: Utf8PathBuf },

    /// Blessed OpenAPI documents come from this directory
    ///
    /// This is basically just for testing and debugging this tool.
    Directory { local_directory: Utf8PathBuf },
}

impl BlessedSource {
    /// Load the blessed OpenAPI documents
    pub fn load(
        &self,
        apis: &ManagedApis,
        styles: &Styles,
    ) -> anyhow::Result<(BlessedFiles, ErrorAccumulator)> {
        let mut errors = ErrorAccumulator::new();
        match self {
            BlessedSource::Directory { local_directory } => {
                eprintln!(
                    "{:>HEADER_WIDTH$} blessed OpenAPI documents from {:?}",
                    "Loading".style(styles.success_header),
                    local_directory,
                );
                let api_files =
                    walk_local_directory(local_directory, apis, &mut errors)?;
                Ok((BlessedFiles::from(api_files), errors))
            }
            BlessedSource::GitRevisionMergeBase { revision, directory } => {
                eprintln!(
                    "{:>HEADER_WIDTH$} blessed OpenAPI documents from git \
                     revision {:?} path {:?}",
                    "Loading".style(styles.success_header),
                    revision,
                    directory
                );
                Ok((
                    BlessedFiles::load_from_git_parent_branch(
                        &revision,
                        &directory,
                        apis,
                        &mut errors,
                    )?,
                    errors,
                ))
            }
        }
    }
}

/// Specifies how to find generated OpenAPI documents
#[derive(Debug)]
pub enum GeneratedSource {
    /// Generate OpenAPI documents from the API implementation (default)
    Generated,

    /// Load "generated" OpenAPI documents from the specified directory
    ///
    /// This is basically just for testing and debugging this tool.
    Directory { local_directory: Utf8PathBuf },
}

impl GeneratedSource {
    /// Load the generated OpenAPI documents (i.e., generating them as needed)
    pub fn load(
        &self,
        apis: &ManagedApis,
        styles: &Styles,
    ) -> anyhow::Result<(GeneratedFiles, ErrorAccumulator)> {
        let mut errors = ErrorAccumulator::new();
        match self {
            GeneratedSource::Generated => {
                eprintln!(
                    "{:>HEADER_WIDTH$} OpenAPI documents from API \
                     definitions ... ",
                    GENERATING.style(styles.success_header)
                );
                Ok((GeneratedFiles::generate(apis, &mut errors)?, errors))
            }
            GeneratedSource::Directory { local_directory } => {
                eprintln!(
                    "{:>HEADER_WIDTH$} \"generated\" OpenAPI documents from \
                     {:?} ... ",
                    "Loading".style(styles.success_header),
                    local_directory,
                );
                let api_files =
                    walk_local_directory(local_directory, apis, &mut errors)?;
                Ok((GeneratedFiles::from(api_files), errors))
            }
        }
    }
}

/// Specifies where to find local OpenAPI documents
pub enum LocalSource {
    /// Local OpenAPI documents come from this directory
    Directory { local_directory: Utf8PathBuf },
}

impl LocalSource {
    /// Load the local OpenAPI documents
    pub fn load(
        &self,
        apis: &ManagedApis,
        styles: &Styles,
    ) -> anyhow::Result<(LocalFiles, ErrorAccumulator)> {
        let mut errors = ErrorAccumulator::new();
        match self {
            LocalSource::Directory { local_directory } => {
                eprintln!(
                    "{:>HEADER_WIDTH$} local OpenAPI documents from \
                     {:?} ... ",
                    "Loading".style(styles.success_header),
                    local_directory,
                );
                Ok((
                    LocalFiles::load_from_directory(
                        local_directory,
                        apis,
                        &mut errors,
                    )?,
                    errors,
                ))
            }
        }
    }
}

/// Stores errors and warnings accumulated during loading
pub struct ErrorAccumulator {
    /// errors that reflect incorrectness or incompleteness of the loaded data
    errors: Vec<anyhow::Error>,
    /// problems that do not affect the correctness or completeness of the data
    warnings: Vec<anyhow::Error>,
}

impl ErrorAccumulator {
    pub fn new() -> ErrorAccumulator {
        ErrorAccumulator { errors: Vec::new(), warnings: Vec::new() }
    }

    /// Record an error
    pub fn error(&mut self, error: anyhow::Error) {
        self.errors.push(error);
    }

    /// Record a warning
    pub fn warning(&mut self, error: anyhow::Error) {
        self.warnings.push(error);
    }

    pub fn iter_errors(&self) -> impl Iterator<Item = &'_ anyhow::Error> + '_ {
        self.errors.iter()
    }

    pub fn iter_warnings(
        &self,
    ) -> impl Iterator<Item = &'_ anyhow::Error> + '_ {
        self.warnings.iter()
    }
}
