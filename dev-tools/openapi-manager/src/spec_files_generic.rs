// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files in the repository
//! XXX-dap TODO-doc needs update

use crate::apis::{ApiIdent, ManagedApi, ManagedApis};
use anyhow::{anyhow, bail, Context};
use camino::Utf8PathBuf;
use debug_ignore::DebugIgnore;
use openapiv3::OpenAPI;
use std::fmt::{Debug, Display};
use std::{collections::BTreeMap, ops::Deref};
use thiserror::Error;

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
    ) -> Result<ApiSpecFileName, BadVersionedFileName> {
        let ident = ApiIdent::from(ident.to_string());
        let Some(api) = apis.api(&ident) else {
            return Err(BadVersionedFileName::NoSuchApi);
        };

        if !api.is_versioned() {
            return Err(BadVersionedFileName::NotVersioned);
        }

        let expected_prefix = format!("{}-", &ident);
        let suffix =
            basename.strip_prefix(&expected_prefix).ok_or_else(|| {
                BadVersionedFileName::UnexpectedName {
                    ident: ident.clone(),
                    source: anyhow!("unexpected prefix"),
                }
            })?;

        let middle = suffix.strip_suffix(".json").ok_or_else(|| {
            BadVersionedFileName::UnexpectedName {
                ident: ident.clone(),
                source: anyhow!("bad suffix"),
            }
        })?;

        let (version_str, hash) = middle.rsplit_once("-").ok_or_else(|| {
            BadVersionedFileName::UnexpectedName {
                ident: ident.clone(),
                source: anyhow!("cannot extract version and hash"),
            }
        })?;

        let version: semver::Version =
            version_str.parse().map_err(|e: semver::Error| {
                BadVersionedFileName::UnexpectedName {
                    ident: ident.clone(),
                    source: anyhow!(e).context(format!(
                        "version string is not a semver: {:?}",
                        version_str
                    )),
                }
            })?;

        // Dropshot does not support pre-release strings and we don't either.
        // This could probably be made to work, but it's easier to constrain
        // things for now and relax it later.
        if !version.pre.is_empty() {
            return Err(BadVersionedFileName::UnexpectedName {
                ident,
                source: anyhow!(
                    "version string has a prerelease field \
                     (not supported): {:?}",
                    version_str
                ),
            });
        }

        if !version.build.is_empty() {
            return Err(BadVersionedFileName::UnexpectedName {
                ident,
                source: anyhow!(
                    "version string has a build field (not supported): {:?}",
                    version_str
                ),
            });
        }

        Ok(ApiSpecFileName {
            ident: ident,
            kind: ApiSpecFileNameKind::Versioned {
                version,
                hash: hash.to_string(),
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

    pub fn for_lockstep(api: &ManagedApi) -> ApiSpecFileName {
        ApiSpecFileName {
            ident: api.ident().clone(),
            kind: ApiSpecFileNameKind::Lockstep,
        }
    }

    pub fn for_versioned(
        api: &ManagedApi,
        version: semver::Version,
        contents: &[u8],
    ) -> ApiSpecFileName {
        let hash = hash_contents(contents);
        ApiSpecFileName {
            ident: api.ident().clone(),
            kind: ApiSpecFileNameKind::Versioned { version, hash },
        }
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

    pub fn basename(&self) -> String {
        match &self.kind {
            ApiSpecFileNameKind::Lockstep => format!("{}.json", self.ident),
            ApiSpecFileNameKind::Versioned { version, hash } => {
                // XXX-dap the version number must not contain dashes
                // XXX-dap actually I think that's fine now?
                format!("{}-{}-{}.json", self.ident, version, hash)
            }
        }
    }

    pub fn hash(&self) -> Option<&str> {
        match &self.kind {
            ApiSpecFileNameKind::Lockstep => None,
            ApiSpecFileNameKind::Versioned { hash, .. } => Some(hash),
        }
    }
}

/// Describes how this API's specification file is named
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
enum ApiSpecFileNameKind {
    Lockstep,
    Versioned { version: semver::Version, hash: String },
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

#[derive(Debug, Error)]
enum BadVersionedFileName {
    #[error("does not match a known API")]
    NoSuchApi,
    #[error("this API is not a versioned API")]
    NotVersioned,
    #[error(
        "expected a versioned API document filename for API {ident:?} to look \
         like \"{ident:?}-SEMVER-HASH.json\""
    )]
    UnexpectedName { ident: ApiIdent, source: anyhow::Error },
}

/// Describes an OpenAPI document found on disk
#[derive(Debug)]
pub struct ApiSpecFile {
    name: ApiSpecFileName,
    contents: DebugIgnore<OpenAPI>,
    contents_buf: DebugIgnore<Vec<u8>>,
    version: semver::Version,
}

impl ApiSpecFile {
    pub fn for_contents(
        spec_file_name: ApiSpecFileName,
        openapi: OpenAPI,
        contents_buf: Vec<u8>,
    ) -> anyhow::Result<ApiSpecFile> {
        let parsed_version: semver::Version =
            openapi.info.version.parse().with_context(|| {
                format!(
                    "file {:?}: parsing version from generated spec",
                    spec_file_name.path()
                )
            })?;

        if let ApiSpecFileNameKind::Versioned { version, hash } =
            &spec_file_name.kind
        {
            if *version != parsed_version {
                bail!(
                    "file {:?}: version in the file ({}) differs from \
                     the one in the filename",
                    spec_file_name.path(),
                    parsed_version
                );
            }

            let expected_hash = hash_contents(&contents_buf);
            if expected_hash != *hash {
                bail!(
                    "file {:?}: computed hash {:?}, but file name has \
                     different hash {:?}",
                    spec_file_name.path(),
                    expected_hash,
                    hash
                );
            }
        }

        Ok(ApiSpecFile {
            name: spec_file_name,
            contents: DebugIgnore(openapi),
            contents_buf: DebugIgnore(contents_buf),
            version: parsed_version,
        })
    }

    pub fn spec_file_name(&self) -> &ApiSpecFileName {
        &self.name
    }

    pub fn version(&self) -> &semver::Version {
        &self.version
    }

    pub fn openapi(&self) -> &OpenAPI {
        &self.contents
    }

    pub fn contents(&self) -> &[u8] {
        &self.contents_buf
    }
}

pub struct ApiSpecFilesBuilder<'a> {
    apis: &'a ManagedApis,
    spec_files: BTreeMap<ApiIdent, ApiFiles<Vec<ApiSpecFile>>>,
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
        misconfigurations_okay: bool,
    ) -> Option<ApiSpecFileName> {
        match ApiSpecFileName::new_lockstep(&self.apis, basename) {
            Err(
                warning @ (BadLockstepFileName::NoSuchApi
                | BadLockstepFileName::NotLockstep),
            ) if misconfigurations_okay => {
                // When we're looking at the blessed files, the caller provides
                // `misconfigurations_okay: true` and we treat these as
                // warnings because the configuration for an API may have
                // changed between the blessed files and the local changes.
                //
                // - NoSuchApi: somebody is deleting an API locally
                // - NotLockstep: somebody is converting a lockstep API to a
                //   versioned one
                let warning = anyhow!(warning)
                    .context(format!("skipping file {:?}", basename));
                self.load_warning(warning);
                None
            }
            Err(warning @ BadLockstepFileName::MissingJsonSuffix) => {
                // Even if the caller didn't provide `problems_okay: true`, it's
                // not a big deal to have an extra file here.  This could be an
                // editor swap file or something.
                let warning = anyhow!(warning)
                    .context(format!("skipping file {:?}", basename));
                self.load_warning(warning);
                None
            }
            Err(error) => {
                self.load_error(
                    anyhow!(error).context(format!("file {:?}", basename)),
                );
                None
            }
            Ok(file_name) => Some(file_name),
        }
    }

    pub fn versioned_directory(
        &mut self,
        basename: &str,
        misconfigurations_okay: bool,
    ) -> Option<ApiIdent> {
        let ident = ApiIdent::from(basename.to_owned());
        match self.apis.api(&ident) {
            Some(api) if api.is_versioned() => Some(ident),
            Some(_) => {
                // See lockstep_file_name().  This is not always a problem.
                let error = anyhow!(
                    "skipping directory for lockstep API: {:?}",
                    basename,
                );
                if misconfigurations_okay {
                    self.load_warning(error);
                } else {
                    self.load_error(error);
                }
                None
            }
            None => {
                let error = anyhow!(
                    "skipping directory for unknown API: {:?}",
                    basename,
                );
                if misconfigurations_okay {
                    self.load_warning(error);
                } else {
                    self.load_error(error);
                }
                None
            }
        }
    }

    pub fn versioned_file_name(
        &mut self,
        ident: &ApiIdent,
        basename: &str,
        misconfigurations_okay: bool,
    ) -> Option<ApiSpecFileName> {
        match ApiSpecFileName::new_versioned(&self.apis, ident, basename) {
            Ok(file_name) => Some(file_name),
            Err(
                warning @ (BadVersionedFileName::NoSuchApi
                | BadVersionedFileName::NotVersioned),
            ) if misconfigurations_okay => {
                // See lockstep_file_name().
                self.load_warning(
                    anyhow!(warning)
                        .context(format!("skipping file {}", basename)),
                );
                None
            }
            Err(warning @ BadVersionedFileName::UnexpectedName { .. }) => {
                // See lockstep_file_name().
                self.load_warning(
                    anyhow!(warning)
                        .context(format!("skipping file {}", basename)),
                );
                None
            }
            Err(error) => {
                self.load_error(
                    anyhow!(error).context(format!("file {}", basename)),
                );
                None
            }
        }
    }

    pub fn load_contents(
        &mut self,
        file_name: ApiSpecFileName,
        contents: Vec<u8>,
    ) {
        let maybe_file = serde_json::from_slice(&contents)
            .with_context(|| format!("parse {:?}", file_name.path()))
            .and_then(|parsed| {
                ApiSpecFile::for_contents(file_name, parsed, contents)
            });
        match maybe_file {
            Ok(file) => {
                let ident = file.spec_file_name().ident();
                let api_version = file.version();
                self.spec_files
                    .entry(ident.clone())
                    .or_insert_with(ApiFiles::new)
                    .spec_files
                    .entry(api_version.clone())
                    .or_insert_with(Vec::new)
                    .push(file);
            }
            Err(error) => {
                self.errors.push(error);
            }
        }
    }

    pub fn load_latest_link(
        &mut self,
        ident: &ApiIdent,
        links_to: ApiSpecFileName,
        misconfigurations_okay: bool,
    ) {
        let Some(api) = self.apis.api(ident) else {
            let error =
                anyhow!("link for unknown API {:?} ({})", ident, links_to);
            if misconfigurations_okay {
                self.load_warning(error);
            } else {
                self.load_error(error);
            }

            return;
        };

        if !api.is_versioned() {
            let error = anyhow!(
                "link for non-versioned API {:?} ({})",
                ident,
                links_to
            );
            if misconfigurations_okay {
                self.load_warning(error);
            } else {
                self.load_error(error);
            }
            return;
        }

        let api_files =
            self.spec_files.entry(ident.clone()).or_insert_with(ApiFiles::new);
        if let Some(previous) = api_files.latest_link.replace(links_to) {
            // unwrap(): we just put this here.
            let new_link = api_files.latest_link.as_ref().unwrap().to_string();
            self.load_error(anyhow!(
                "API {:?}: multiple \"latest\" links (at least {}, {})",
                ident,
                previous,
                new_link,
            ));
        }
    }

    pub fn into_parts<
        T: Debug + TryFrom<Vec<ApiSpecFile>, Error = anyhow::Error> + AsRawFiles,
    >(
        self,
    ) -> anyhow::Result<(
        BTreeMap<ApiIdent, ApiFiles<T>>,
        Vec<anyhow::Error>,
        Vec<anyhow::Error>,
    )> {
        let errors = self.errors;
        let warnings = self.warnings;
        // This mess is just mapping the items in the inner BTreeMap with the
        // caller's type T.
        let map = self
            .spec_files
            .into_iter()
            .map(|(api_ident, api_files)| Ok((api_ident, api_files.convert()?)))
            .collect::<Result<BTreeMap<_, _>, anyhow::Error>>()?;
        Ok((map, errors, warnings))
    }
}

#[derive(Debug)]
pub struct ApiFiles<T: AsRawFiles> {
    spec_files: BTreeMap<semver::Version, T>,
    latest_link: Option<ApiSpecFileName>,
}

impl<T: AsRawFiles> ApiFiles<T> {
    fn new() -> ApiFiles<T> {
        ApiFiles { spec_files: BTreeMap::new(), latest_link: None }
    }

    fn convert<U>(self) -> anyhow::Result<ApiFiles<U>>
    where
        U: Debug + TryFrom<T, Error = anyhow::Error> + AsRawFiles,
    {
        Ok(ApiFiles {
            spec_files: self
                .spec_files
                .into_iter()
                .map(|(v, files)| Ok((v, U::try_from(files)?)))
                .collect::<Result<_, _>>()?,
            latest_link: self.latest_link,
        })
    }

    pub fn versions(&self) -> &BTreeMap<semver::Version, T> {
        &self.spec_files
    }

    pub fn raw_files_map(
        &self,
    ) -> BTreeMap<semver::Version, Vec<&'_ ApiSpecFile>> {
        self.spec_files
            .iter()
            .map(|(k, v)| (k.clone(), v.as_raw_files().collect()))
            .collect()
    }

    pub fn latest_link(&self) -> Option<&ApiSpecFileName> {
        self.latest_link.as_ref()
    }
}

pub trait AsRawFiles: Debug {
    fn as_raw_files<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a ApiSpecFile> + 'a>;
}

impl AsRawFiles for Vec<ApiSpecFile> {
    fn as_raw_files<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a ApiSpecFile> + 'a> {
        Box::new(self.iter())
    }
}

fn hash_contents(contents: &[u8]) -> String {
    let hasher = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
    let computed_hash = hasher.checksum(contents);
    // XXX-dap why does this differ from what cksum reports?
    // eprintln!("dap: hashing: len = {}, first 4 = {} {} {} {}, result = {}", contents.len(), contents[0], contents[1], contents[2], contents[3], computed_hash);
    hex::encode(computed_hash.to_be_bytes())
}
