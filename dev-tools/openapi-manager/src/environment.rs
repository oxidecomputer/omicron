// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the environment the command is running in, and particularly where
//! different sets of specifications are loaded from

use crate::apis::ManagedApis;
use crate::git::GitRevision;
use crate::spec_files_blessed::BlessedFiles;
use crate::spec_files_generated::GeneratedFiles;
use crate::spec_files_local::walk_local_directory;
use crate::spec_files_local::LocalFiles;
use anyhow::Context;
use camino::Utf8Path;
use camino::Utf8PathBuf;

pub enum BlessedSource {
    Directory { local_directory: Utf8PathBuf },
    GitRevisionMergeBase { revision: GitRevision, directory: Utf8PathBuf },
}

impl BlessedSource {
    fn load(&self, apis: &ManagedApis) -> anyhow::Result<BlessedFiles> {
        match self {
            BlessedSource::Directory { local_directory } => {
                let api_files = walk_local_directory(local_directory, apis)?;
                Ok(BlessedFiles::from(api_files))
            }
            BlessedSource::GitRevisionMergeBase { revision, directory } => {
                BlessedFiles::load_from_git_parent_branch(
                    &revision, &directory, apis,
                )
            }
        }
    }
}

pub enum GeneratedSource {
    Generated,
    Directory { local_directory: Utf8PathBuf },
}

impl GeneratedSource {
    fn load(&self, apis: &ManagedApis) -> anyhow::Result<GeneratedFiles> {
        match self {
            GeneratedSource::Generated => GeneratedFiles::generate(apis),
            GeneratedSource::Directory { local_directory } => {
                let api_files = walk_local_directory(local_directory, apis)?;
                Ok(GeneratedFiles::from(api_files))
            }
        }
    }
}

pub enum LocalSource {
    Directory { local_directory: Utf8PathBuf },
}

impl LocalSource {
    fn load(&self, apis: &ManagedApis) -> anyhow::Result<LocalFiles> {
        match self {
            LocalSource::Directory { local_directory } => {
                Ok(LocalFiles::load_from_directory(local_directory, apis)?)
            }
        }
    }
}

pub(crate) struct Environment {
    /// Canonicalized path to the root of this Omicron workspace
    ///
    /// This is currently only used for finding "extra" files that are
    /// enumerated by the validation process, which is currently just the file
    /// describing external API tag configuration.
    pub(crate) workspace_root: Utf8PathBuf,

    pub(crate) blessed_source: BlessedSource,
    pub(crate) generated_source: GeneratedSource,
    pub(crate) local_source: LocalSource,
}

impl Environment {
    pub(crate) fn new(
        // XXX-dap need to figure out what these arguments are
        openapi_dir: Option<Utf8PathBuf>,
    ) -> anyhow::Result<Self> {
        let mut workspace_root = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // This crate is two levels down from the root of omicron, so go up twice.
        workspace_root.pop();
        workspace_root.pop();

        let openapi_dir =
            openapi_dir.unwrap_or_else(|| workspace_root.join("openapi"));
        let local_source =
            LocalSource::Directory { local_directory: openapi_dir };
        let blessed_source = BlessedSource::GitRevisionMergeBase {
            revision: GitRevision::from("main".to_string()),
            directory: Utf8PathBuf::from("openapi"),
        };
        let generated_source = GeneratedSource::Generated;

        Ok(Self {
            workspace_root,
            blessed_source,
            generated_source,
            local_source,
        })
    }

    // XXX-dap rip out
    pub(crate) fn openapi_dir(&self) -> &Utf8Path {
        match &self.local_source {
            LocalSource::Directory { local_directory } => &local_directory,
        }
    }
}
