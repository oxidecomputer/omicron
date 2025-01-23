// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files in the repository

use crate::apis::{ApiIdent, ManagedApis};
use anyhow::{anyhow, bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use debug_ignore::DebugIgnore;
use openapiv3::OpenAPI;
use std::fmt::Display;
use std::{collections::BTreeMap, ops::Deref};
use thiserror::Error;

/// Container for OpenAPI spec files found in the local filesystem
///
/// Most validation is not done at this point.
/// XXX-dap be more specific about what has and has not been validated at this
/// point.
// XXX-dap move to a separate module?
#[derive(Debug)]
pub struct LocalFiles {
    spec_files:
        BTreeMap<ApiIdent, BTreeMap<semver::Version, Vec<LocalApiSpecFile>>>,
    errors: Vec<anyhow::Error>,
    warnings: Vec<anyhow::Error>,
}

impl LocalFiles {
    // XXX-dap goofy that this can return a thing with errors or an error
    // itself.  but there are different layers of error here:
    // - error traversing the directory (that's what this returned error means)
    // - error with individual items found
    // - things that were skipped, etc.
    pub fn load_from_directory(
        dir: &Utf8Path,
        apis: &ManagedApis,
    ) -> anyhow::Result<LocalFiles> {
        let mut api_files = ApiSpecFilesBuilder::new(apis);
        let entry_iter = dir
            .read_dir_utf8()
            .with_context(|| format!("readdir {:?}", dir))?;
        for maybe_entry in entry_iter {
            let entry = maybe_entry
                .with_context(|| format!("readdir {:?} entry", dir))?;

            // If this entry is a file, then we'd expect it to be the JSON file
            // for one of our lockstep APIs.  Check and see.
            let path = entry.path();
            let file_name = entry.file_name();
            let file_type = entry
                .file_type()
                .with_context(|| format!("file type of {:?}", path))?;
            if file_type.is_file() {
                match fs_err::read(path) {
                    Ok(contents) => {
                        if let Some(file_name) =
                            api_files.lockstep_file_name(file_name)
                        {
                            api_files.load_contents(file_name, contents);
                        }
                    }
                    Err(error) => {
                        api_files.load_error(anyhow!(error));
                    }
                };
            } else if file_type.is_dir() {
                LocalFiles::load_versioned_directory(
                    &mut api_files,
                    path,
                    file_name,
                );
            } else {
                // This is not something the tool cares about, but it's not
                // obviously a problem, either.
                api_files.load_warning(anyhow!(
                    "ignored (not a file or directory): {:?}",
                    path
                ));
            };
        }

        let (spec_files, errors, warnings) = api_files.into_parts();
        Ok(LocalFiles { spec_files, errors, warnings })
    }

    fn load_versioned_directory(
        api_files: &mut ApiSpecFilesBuilder,
        path: &Utf8Path,
        basename: &str,
    ) {
        let Some(ident) = api_files.versioned_directory(basename) else {
            return;
        };

        let entries = match path
            .read_dir_utf8()
            .and_then(|entry_iter| entry_iter.collect::<Result<Vec<_>, _>>())
        {
            Ok(entries) => entries,
            Err(error) => {
                api_files.load_error(
                    anyhow!(error).context(format!("readdir {:?}", path)),
                );
                return;
            }
        };

        for entry in entries {
            let file_name = entry.file_name();
            let Some(file_name) =
                api_files.versioned_file_name(&ident, file_name)
            else {
                continue;
            };

            let contents = match fs_err::read(&entry.path()) {
                Ok(contents) => contents,
                Err(error) => {
                    api_files.load_error(anyhow!(error));
                    continue;
                }
            };

            api_files.load_contents(file_name, contents);
        }
    }
}

#[derive(Debug)]
pub struct LocalApiSpecFile(ApiSpecFile);
impl From<ApiSpecFile> for LocalApiSpecFile {
    fn from(value: ApiSpecFile) -> Self {
        LocalApiSpecFile(value)
    }
}

/// Describes the path to an OpenAPI document file
// XXX-dap spec -> document?
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ApiSpecFileName {
    ident: ApiIdent,
    kind: ApiSpecFileNameKind,
}

impl Display for ApiSpecFileName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.path().as_str())
    }
}

impl ApiSpecFileName {
    /// Attempts to parse the given file basename as an ApiSpecFileName of kind
    /// `Versioned`.  These look like:
    ///
    ///     ident-SEMVER-LABEL.json
    fn new_versioned(
        apis: &ManagedApis,
        ident: &str,
        basename: &str,
    ) -> anyhow::Result<ApiSpecFileName> {
        let ident = ApiIdent::from(ident.to_string());
        let Some(api) = apis.api(&ident) else {
            bail!("does not match a known API")
        };

        if !api.is_versioned() {
            bail!("{:?} is not a versioned API", ident);
        }

        let expected_prefix = format!("{}-", ident);
        let suffix =
            basename.strip_prefix(&expected_prefix).ok_or_else(|| {
                anyhow!(
                    "versioned API document filename did not start with {:?}",
                    expected_prefix
                )
            })?;

        let middle = suffix.strip_suffix(".json").ok_or_else(|| {
            anyhow!("versioned API document filename did not end in .json")
        })?;

        let (version_str, label) =
            middle.rsplit_once("-").ok_or_else(|| {
                anyhow!(
                    "extracting version and label from versioned API \
                     document filename"
                )
            })?;

        let version: semver::Version =
            version_str.parse().with_context(|| {
                format!("version string {:?} is not a semver", version_str)
            })?;

        // Dropshot does not support pre-release strings and we don't either.
        // This could probably be made to work, but it's easier to constrain
        // things for now and relax it later.
        if !version.pre.is_empty() {
            bail!(
                "version string {:?} has a prerelease field (not supported)",
                version_str
            );
        }

        if !version.build.is_empty() {
            bail!(
                "version string {:?} has a build field (not supported)",
                version_str
            );
        }

        Ok(ApiSpecFileName {
            ident: ident,
            kind: ApiSpecFileNameKind::Versioned {
                version,
                label: label.to_string(),
            },
        })
    }

    /// Attempts to parse the given file basename as an ApiSpecFileName of kind
    /// `Lockstep`
    fn new_lockstep(
        apis: &ManagedApis,
        basename: &str,
    ) -> Result<ApiSpecFileName, BadLockstepFileName> {
        let ident = ApiIdent::from(
            basename
                .strip_suffix(".json")
                .ok_or(BadLockstepFileName::MissingJsonSuffix)?
                .to_owned(),
        );
        let api = apis.api(&ident).ok_or(BadLockstepFileName::NoSuchApi)?;
        if !api.is_lockstep() {
            return Err(BadLockstepFileName::NotLockstep);
        }

        Ok(ApiSpecFileName { ident, kind: ApiSpecFileNameKind::Lockstep })
    }

    pub fn ident(&self) -> &ApiIdent {
        &self.ident
    }

    /// Returns the path of this file relative to the root of the OpenAPI specs
    pub fn path(&self) -> Utf8PathBuf {
        match &self.kind {
            ApiSpecFileNameKind::Lockstep => {
                Utf8PathBuf::from_iter([self.basename()])
            }
            ApiSpecFileNameKind::Versioned { .. } => Utf8PathBuf::from_iter([
                self.ident.deref().clone(),
                self.basename(),
            ]),
        }
    }

    fn basename(&self) -> String {
        match &self.kind {
            ApiSpecFileNameKind::Lockstep => format!("{}.json", self.ident),
            ApiSpecFileNameKind::Versioned { version, label } => {
                // XXX-dap the version number must not contain dashes
                // XXX-dap actually I think that's fine now?
                format!("{}-{}-{}.json", self.ident, version, label)
            }
        }
    }
}

/// Describes how this API's specification file is named
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
enum ApiSpecFileNameKind {
    Lockstep,
    Versioned { version: semver::Version, label: String },
}

#[derive(Debug, Error)]
enum BadLockstepFileName {
    #[error("expected lockstep API file name to end in \".json\"")]
    MissingJsonSuffix,
    #[error("does not match a known API")]
    NoSuchApi,
    #[error("this API is not a lockstep API")]
    NotLockstep,
}

/// Describes an OpenAPI document found on disk
#[derive(Debug)]
pub struct ApiSpecFile {
    name: ApiSpecFileName,
    contents: DebugIgnore<OpenAPI>,
    // XXX-dap do we have to store two whole copies of this?
    contents_buf: DebugIgnore<Vec<u8>>,
    version: semver::Version,
}

impl ApiSpecFile {
    fn load(
        name: ApiSpecFileName,
        path: &Utf8Path,
    ) -> anyhow::Result<ApiSpecFile> {
        let contents_buf = fs_err::read(path)
            .with_context(|| format!("read file {:?}", path))?;
        let contents: OpenAPI = serde_json::from_slice(&contents_buf)
            .with_context(|| format!("parse file {:?}", path))?;
        let parsed_version: semver::Version =
            contents.info.version.parse().with_context(|| {
                format!("version in {:?} was not a semver", path)
            })?;

        if let ApiSpecFileNameKind::Versioned { version, label: _ } = &name.kind
        {
            if *version != parsed_version {
                bail!(
                    "file {:?}: version in the file ({:?}) differs from \
                     the one in the filename",
                    path,
                    parsed_version
                );
            }
        }

        Ok(ApiSpecFile {
            name,
            contents: DebugIgnore(contents),
            contents_buf: DebugIgnore(contents_buf),
            version: parsed_version,
        })
    }

    pub fn for_contents(
        spec_file_name: &ApiSpecFileName,
        openapi: OpenAPI,
        contents_buf: Vec<u8>,
    ) -> anyhow::Result<ApiSpecFile> {
        let version: semver::Version = openapi
            .info
            .version
            .parse()
            .context("parsing version from generated spec")?;
        Ok(ApiSpecFile {
            name: spec_file_name.clone(),
            contents: DebugIgnore(openapi),
            contents_buf: DebugIgnore(contents_buf),
            version,
        })
    }

    pub fn spec_file_name(&self) -> &ApiSpecFileName {
        &self.name
    }

    pub fn version(&self) -> &semver::Version {
        &self.version
    }

    pub fn contents(&self) -> &[u8] {
        &self.contents_buf
    }
}

pub struct ApiSpecFilesBuilder<'a> {
    apis: &'a ManagedApis,
    spec_files: BTreeMap<ApiIdent, BTreeMap<semver::Version, Vec<ApiSpecFile>>>,
    errors: Vec<anyhow::Error>,
    warnings: Vec<anyhow::Error>,
}

impl<'a> ApiSpecFilesBuilder<'a> {
    pub fn new(apis: &'a ManagedApis) -> ApiSpecFilesBuilder<'a> {
        ApiSpecFilesBuilder {
            apis,
            spec_files: BTreeMap::new(),
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn load_error(&mut self, error: anyhow::Error) {
        self.errors.push(error);
    }

    pub fn load_warning(&mut self, error: anyhow::Error) {
        self.warnings.push(error);
    }

    pub fn lockstep_file_name(
        &mut self,
        basename: &str,
    ) -> Option<ApiSpecFileName> {
        match ApiSpecFileName::new_lockstep(&self.apis, basename) {
            Err(
                warning @ (BadLockstepFileName::NoSuchApi
                | BadLockstepFileName::MissingJsonSuffix),
            ) => {
                // These problems are not fatal.  They might just reflect an
                // extra file here (like an editor swap file or the like).
                let warning = anyhow!(warning)
                    .context(format!("skipping file {:?}", basename));
                self.load_warning(warning);
                None
            }
            Err(error @ BadLockstepFileName::NotLockstep) => {
                self.load_error(anyhow!(error));
                None
            }
            Ok(file_name) => Some(file_name),
        }
    }

    pub fn versioned_directory(&mut self, basename: &str) -> Option<ApiIdent> {
        let ident = ApiIdent::from(basename.to_owned());
        match self.apis.api(&ident) {
            Some(api) if api.is_versioned() => Some(ident),
            Some(_) => {
                self.load_error(anyhow!(
                    "found directory for lockstep API: {:?}",
                    basename,
                ));
                None
            }
            None => {
                self.load_warning(anyhow!(
                    "does not match a known API: {:?}",
                    basename,
                ));
                None
            }
        }
    }

    pub fn versioned_file_name(
        &mut self,
        ident: &ApiIdent,
        basename: &str,
    ) -> Option<ApiSpecFileName> {
        match ApiSpecFileName::new_versioned(&self.apis, ident, basename) {
            Ok(file_name) => Some(file_name),
            Err(error) => {
                // XXX-dap some of these should be warnings
                self.load_error(error);
                None
            }
        }
    }

    fn load_contents(&mut self, file_name: ApiSpecFileName, contents: Vec<u8>) {
        let maybe_file = serde_json::from_slice(&contents)
            .with_context(|| format!("parse {:?}", file_name.path()))
            .and_then(|parsed| {
                ApiSpecFile::for_contents(&file_name, parsed, contents)
            });
        match maybe_file {
            Ok(file) => {
                let ident = file.spec_file_name().ident();
                let api_version = file.version();
                self.spec_files
                    .entry(ident.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(api_version.clone())
                    .or_insert_with(Vec::new)
                    .push(file);
            }
            Err(error) => {
                self.errors.push(error);
            }
        }
    }

    pub fn into_parts<T: From<ApiSpecFile>>(
        self,
    ) -> (
        BTreeMap<ApiIdent, BTreeMap<semver::Version, Vec<T>>>,
        Vec<anyhow::Error>,
        Vec<anyhow::Error>,
    ) {
        let errors = self.errors;
        let warnings = self.warnings;
        // This mess is just mapping the items in the inner BTreeMap with the
        // caller's type T.
        let map = self
            .spec_files
            .into_iter()
            .map(|(api_ident, vmap)| {
                (
                    api_ident,
                    vmap.into_iter()
                        .map(|(v, files)| {
                            (
                                v,
                                files
                                    .into_iter()
                                    .map(T::from)
                                    .collect::<Vec<_>>(),
                            )
                        })
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        (map, errors, warnings)
    }
}
