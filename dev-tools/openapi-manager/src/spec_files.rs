// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files in the repository

use crate::apis::{ApiIdent, ManagedApi, ManagedApis};
use anyhow::{anyhow, bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use debug_ignore::DebugIgnore;
use openapiv3::OpenAPI;
use std::fmt::Display;
use std::{collections::BTreeMap, ops::Deref};

/// Container for OpenAPI spec files found in the local filesystem
///
/// Most validation is not done at this point.
/// XXX-dap be more specific about what has and has not been validated at this
/// point.
// XXX-dap move to a separate module?
#[derive(Debug)]
pub struct LocalFiles {
    api_files: BTreeMap<ApiIdent, Vec<ApiSpecFile>>,
}

impl LocalFiles {
    pub fn load_from_directory(
        dir: &Utf8Path,
        apis: &ManagedApis,
    ) -> anyhow::Result<(LocalFiles, Vec<anyhow::Error>)> {
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
                        api_files.load_lockstep(file_name, contents);
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
                )
            } else {
                // This is not something the tool cares about, but it's not
                // obviously a problem, either.
                api_files.load_warning(anyhow!(
                    "ignored (not a file or directory): {:?}",
                    path
                ));
            };
        }

        Ok((LocalFiles { api_files }, warnings))
    }

    fn load_versioned_directory(
        api_files: &mut ApiSpecFilesBuilder,
        path: &Utf8Path,
        basename: &str,
    ) -> anyhow::Result<Vec<ApiSpecFile>> {
        let ident = ApiIdent::from(basename.to_owned());
        // XXX-dap Whether this is a valid API identifier is checked in the
        // builder, where it's correctly flagged as a warning, not an error.  it
        // would make more sense to check it here so that we report it once and
        // don't bother looking in the directory, I think.
        //
        // Similarly, the builder checks if the API is really a versioned one.
        // That's good, and that should be an error, not a warning, but it also
        // makes sense to be checked here so that we can report it once and skip
        // the directory.
        let entry_iter = path
            .read_dir_utf8()
            .with_context(|| format!("readdir {:?}", path))?;
        for maybe_entry in entry_iter {
            let entry = maybe_entry
                .with_context(|| format!("readdir {:?} entry", path))?;
            let file_name = entry.file_name();
            // XXX-dap check if it's a file first?
            let contents = fs_err::read(&file_name);
            api_files.load_versioned(api_ident, file_name);
            let file_name = ApiSpecFileName::new_versioned(basename, file_name)
                .with_context(|| format!("path {:?}", entry.path()))?;
            rv.push(ApiSpecFile::load(file_name, entry.path())?);
        }

        Ok(rv)
    }

    pub fn into_map(self) -> BTreeMap<ApiIdent, Vec<ApiSpecFile>> {
        self.api_files
    }

    pub fn apis(&self) -> impl Iterator<Item = &ApiIdent> + '_ {
        self.api_files.keys()
    }

    pub fn api_spec_files(
        &self,
        ident: &ApiIdent,
    ) -> Option<impl Iterator<Item = &ApiSpecFile> + '_> {
        self.api_files.get(ident).map(|files| files.iter())
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
        ident: &str,
        basename: &str,
    ) -> anyhow::Result<ApiSpecFileName> {
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
            ident: ApiIdent::from(ident.to_string()),
            kind: ApiSpecFileNameKind::Versioned {
                version,
                label: label.to_string(),
            },
        })
    }

    /// Attempts to parse the given file basename as an ApiSpecFileName of kind
    /// `Lockstep`
    fn new_lockstep(basename: &str) -> anyhow::Result<ApiSpecFileName> {
        let ident = basename.strip_suffix(".json").ok_or_else(|| {
            anyhow!("lockstep API document filename did not end in .json")
        })?;
        Ok(ApiSpecFileName {
            ident: ApiIdent::from(ident.to_string()),
            kind: ApiSpecFileNameKind::Lockstep,
        })
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

    pub fn load_lockstep(&mut self, basename: &str, contents: Vec<u8>) {
        match self.parse_lockstep_filename(basename) {
            Ok((file_name, api)) => {
                if !api.is_lockstep() {
                    self.load_error(anyhow!(
                        "found lockstep file for non-lockstep API: {:?}",
                        basename
                    ));
                } else {
                    self.try_load_contents(file_name, contents);
                }
            }
            Err(warning) => {
                self.load_warning(warning);
            }
        };
    }

    pub fn load_versioned(
        &mut self,
        api_ident: &ApiIdent,
        basename: &str,
        contents: Vec<u8>,
    ) {
        // XXX-dap use a real path?
        let path = format!("{}/{}", api_ident, basename);
        match self.parse_versioned_filename(api_ident, &path, basename) {
            Ok((file_name, api)) => {
                if !api.is_versioned() {
                    self.load_error(anyhow!(
                        "found versioned file for lockstep API: {:?}",
                        path,
                    ));
                } else {
                    self.try_load_contents(file_name, contents);
                }
            }
            Err(warning) => {
                self.load_warning(warning);
            }
        };
    }

    fn parse_lockstep_filename(
        &self,
        basename: &str,
    ) -> anyhow::Result<(ApiSpecFileName, &ManagedApi)> {
        let file_name = ApiSpecFileName::new_lockstep(basename)
            .with_context(|| format!("path {:?}", basename))?;
        let ident = file_name.ident();
        let api = self
            .apis
            .api(ident)
            .ok_or_else(|| anyhow!("not a known API: {:?}", basename))?;
        Ok((file_name, api))
    }

    fn parse_versioned_filename(
        &self,
        api_ident: &ApiIdent,
        path: &str,
        basename: &str,
    ) -> anyhow::Result<(ApiSpecFileName, &ManagedApi)> {
        let api = self
            .apis
            .api(api_ident)
            .ok_or_else(|| anyhow!("not a known API: {}", path))?;
        // XXX-dap even within ApiSpecFileName::new_versioned() here, there's a
        // distinction between things that should be warnings vs. errors.
        let file_name = ApiSpecFileName::new_versioned(&api_ident, basename)?;
        Ok((file_name, api))
    }

    fn try_load_contents(&self, file_name: ApiSpecFileName, contents: Vec<u8>) {
        let maybe_file = serde_json::from_slice(&contents)
            .with_context(|| format!("parse {:?}", basename))
            .and_then(|parsed| {
                ApiSpecFile::for_contents(&file_name, parsed, contents)
            });
        match maybe_file {
            Ok(file) => {
                let file_name = file.spec_file_name();
                let api_ident = file.spec_file_name().ident();
                let api_version = file.version();
                self.spec_files
                    .entry(api_ident.clone())
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
}
