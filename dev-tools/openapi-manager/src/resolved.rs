// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resolve different sources of API information (blessed, local, upstream)

use crate::apis::ApiIdent;
use crate::apis::ManagedApi;
use crate::apis::ManagedApis;
use crate::compatibility::api_compatible;
use crate::compatibility::OpenApiCompatibilityError;
use crate::spec::Environment;
use crate::spec_files_blessed::BlessedApiSpecFile;
use crate::spec_files_blessed::BlessedFiles;
use crate::spec_files_generated::GeneratedApiSpecFile;
use crate::spec_files_generated::GeneratedFiles;
use crate::spec_files_generic::ApiSpecFileName;
use crate::spec_files_local::LocalApiSpecFile;
use crate::spec_files_local::LocalFiles;
use crate::validation::validate_generated_openapi_document;
use anyhow::bail;
use anyhow::Context;
use atomicwrites::AtomicFile;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::io::Write;
use thiserror::Error;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct DisplayableVec<T>(pub Vec<T>);
impl<T> Display for DisplayableVec<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // slice::join would require the use of unstable Rust.
        let mut iter = self.0.iter();
        if let Some(item) = iter.next() {
            write!(f, "{item}")?;
        }

        for item in iter {
            write!(f, ", {item}")?;
        }

        Ok(())
    }
}

/// A non-error note that's worth highlighting to the user
// These are not technically errors, but it is useful to treat them the same
// way in terms of having an associated message, etc.
#[derive(Debug, Error)]
pub enum Note {
    /// A previously-supported API version has been removed locally.
    ///
    /// This is not an error because we do expect to EOL old API specs.  There's
    /// not currently a way for this tool to know if the EOL'ing is correct or
    /// not, so we at least highlight it to the user.
    // XXX-dap consider this an error if they don't pass a --allow-removed?
    #[error(
        "API {api_ident} version {version}: formerly blessed version has been \
         removed.  This version will no longer be supported!  This will break \
         upgrade from software that still uses this version.  If this is \
         unexpected, check the list of supported versions in Rust for a \
         possible mismerge."
    )]
    BlessedVersionRemoved { api_ident: ApiIdent, version: semver::Version },
}

/// Describes the result of resolving the blessed spec(s), generated spec(s),
/// and local spec files for a particular API
pub struct Resolution<'a> {
    kind: ResolutionKind<'a>,
    problems: Vec<Problem>,
}

impl<'a> Resolution<'a> {
    pub fn new_lockstep(
        generated: &'a GeneratedApiSpecFile,
        problems: Vec<Problem>,
    ) -> Resolution<'a> {
        Resolution { kind: ResolutionKind::Lockstep(generated), problems }
    }

    pub fn new_blessed(problems: Vec<Problem>) -> Resolution<'a> {
        Resolution { kind: ResolutionKind::Blessed, problems }
    }

    pub fn new_new_locally(
        generated: &'a GeneratedApiSpecFile,
        problems: Vec<Problem>,
    ) -> Resolution<'a> {
        Resolution { kind: ResolutionKind::NewLocally(generated), problems }
    }

    pub fn problems(&self) -> impl Iterator<Item = &'_ Problem> + '_ {
        self.problems.iter()
    }

    // XXX-dap add "are all problems fixable"
}

#[derive(Debug)]
pub enum ResolutionKind<'a> {
    /// This is a lockstep API
    Lockstep(&'a GeneratedApiSpecFile),
    /// This is a versioned API and this version is blessed
    Blessed,
    /// This version is new to the current workspace (i.e., not present
    /// upstream)
    NewLocally(&'a GeneratedApiSpecFile),
}

/// Describes a problem resolving the blessed spec(s), generated spec(s), and
/// local spec files for a particular API
// XXX-dap this should be a struct with ProblemKind and resolution so that fix()
// and fixable() don't require an argument (and can't be mismatched)
#[derive(Debug, Error)]
pub enum Problem {
    // This kind of problem is not associated with any *supported* version of an
    // API.  (All the others are.)
    #[error(
        "One or more local spec files were found that do not correspond to a \
         supported version of this API: {spec_file_names}.  This is unusual, \
         but it could happen if you created this version of the API in this \
         branch, then later changed it (maybe because you merged with upstream \
         and had to adjust the version number for your changes).  In that \
         case, this tool can remove the unused file for you."
    )]
    LocalSpecFilesOrphaned { spec_file_names: DisplayableVec<ApiSpecFileName> },

    // All other problems are associated with specific supported versions of an
    // API.
    #[error(
        "This version is blessed, and it's a supported version, but it's \
         missing a local spec file.  This is unusual.  If you intended to \
         remove this version, you must also update the list of supported \
         versions in Rust.  If you didn't, restore the file from git: \
         {spec_file_name}"
    )]
    BlessedVersionMissingLocal { spec_file_name: ApiSpecFileName },

    // XXX-dap where is the error that means "the blessed file does not match
    // the corresponding local file"
    #[error(
        "Found extra local file for blessed version that does not match the \
         blessed (upstream) spec file: {spec_file_names}.  This can happen if \
         you created this version of the API in this branch, then merged with \
         an upstream commit that also added the same version number.  In that \
         case, you likely already bumped your local version number (when you \
         merged the list of supported versions in Rust) and this file is \
         vestigial. This tool can remove the unused file for you."
    )]
    BlessedVersionExtraLocalSpec {
        spec_file_names: DisplayableVec<ApiSpecFileName>,
    },

    #[error(
        "Spec generated from the current code is not compatible with the \
         blessed spec (from upstream)."
    )]
    BlessedVersionBroken {
        compatibility_issues: DisplayableVec<OpenApiCompatibilityError>,
    },

    #[error(
        "No local spec file was found for lockstep or non-blessed version.  \
         This is normal if you have just changed a lockstep API or added \
         this version to a versioned API.  This tool can generate the file \
         for you."
    )]
    LocalVersionMissingLocal,

    #[error(
        "Extra (incorrect) spec files were found for non-blessed version: \
         {spec_file_names}.  This tool can remove the files for you."
    )]
    LocalVersionExtra { spec_file_names: DisplayableVec<ApiSpecFileName> },

    #[error(
        "Spec generated from the current code does not match this lockstep \
         or locally-added spec: {spec_file_names}.  This tool can update the \
         local file(s) for you."
    )]
    // For versioned APIs, since the filename has its own hash in it, when the
    // local file is stale, it's not that the file contents will be wrong, but
    // rather that there will be one or more _incorrect_ files and the correct
    // one will be missing.  The fix will be to remove all the incorrect ones
    // and add the correct one.
    // XXX-dap this is going to be really annoying in that when iterating on
    // local changes, you will have to update the clients to point at the new
    // hashes all the time.  We could stop putting the checksum into the
    // filenames?  But then you could easily forget to update the client if you
    // bumped your local version.  Really, it'd be nice if the client filename
    // was somehow checked by this tool.  Maybe this library should define
    // constants like API_NAME_LATEST that are used in the client specs?
    LocalVersionStale { spec_file_names: DisplayableVec<ApiSpecFileName> },

    #[error(
        "Generated spec for API {api_ident:?} version {version} is not valid"
    )]
    GeneratedValidationError {
        api_ident: ApiIdent,
        version: semver::Version,
        #[source]
        source: anyhow::Error,
    },

    #[error("Extra file associated with API {api_ident:?} is stale")]
    ExtraFileStale {
        api_ident: ApiIdent,
        path: Utf8PathBuf,
        check_stale: CheckStale,
    },
}

impl Problem {
    pub fn is_fixable(&self) -> bool {
        match self {
            Problem::LocalSpecFilesOrphaned { .. } => true,
            Problem::BlessedVersionMissingLocal { .. } => false,
            Problem::BlessedVersionExtraLocalSpec { .. } => true,
            Problem::BlessedVersionBroken { .. } => false,
            Problem::LocalVersionMissingLocal => true,
            Problem::LocalVersionExtra { .. } => true,
            Problem::LocalVersionStale { .. } => true,
            Problem::GeneratedValidationError { .. } => false,
            Problem::ExtraFileStale { .. } => true,
        }
    }

    pub fn fix<'a>(
        &'a self,
        resolution: &Resolution<'a>,
        // XXX-dap can this return value not be a vec?
    ) -> Result<Option<Vec<Fix<'a>>>, anyhow::Error> {
        let kind = &resolution.kind;
        Ok(match self {
            Problem::LocalSpecFilesOrphaned { spec_file_names } => {
                Some(vec![Fix::DeleteFiles { files: spec_file_names }])
            }
            Problem::BlessedVersionMissingLocal { .. } => None,
            Problem::BlessedVersionExtraLocalSpec { spec_file_names } => {
                Some(vec![Fix::DeleteFiles { files: spec_file_names }])
            }
            Problem::BlessedVersionBroken { .. } => None,
            Problem::LocalVersionMissingLocal => {
                if let ResolutionKind::Lockstep(generated) = kind {
                    Some(vec![Fix::FixLockstepFile { generated }])
                } else if let ResolutionKind::NewLocally(generated) = kind {
                    Some(vec![Fix::FixVersionedFiles {
                        old: DisplayableVec(Vec::new()),
                        generated,
                    }])
                } else {
                    bail!(
                        "unexpected problem / resolution kind: {:?} / {:?}",
                        self,
                        kind
                    )
                }
            }
            Problem::LocalVersionExtra { spec_file_names } => {
                Some(vec![Fix::DeleteFiles { files: spec_file_names }])
            }
            Problem::LocalVersionStale { spec_file_names } => {
                if let ResolutionKind::Lockstep(generated) = kind {
                    Some(vec![Fix::FixLockstepFile { generated }])
                } else if let ResolutionKind::NewLocally(generated) = kind {
                    Some(vec![Fix::FixVersionedFiles {
                        old: spec_file_names.clone(),
                        generated,
                    }])
                } else {
                    bail!(
                        "unexpected problem / resolution kind: {:?} / {:?}",
                        self,
                        kind
                    );
                }
            }
            Problem::GeneratedValidationError { .. } => None,
            Problem::ExtraFileStale { path, check_stale, .. } => {
                Some(vec![Fix::FixExtraFile { path, check_stale }])
            }
        })
    }
}

pub enum Fix<'a> {
    DeleteFiles {
        files: &'a DisplayableVec<ApiSpecFileName>,
    },
    FixLockstepFile {
        generated: &'a GeneratedApiSpecFile,
    },
    FixVersionedFiles {
        old: DisplayableVec<ApiSpecFileName>,
        generated: &'a GeneratedApiSpecFile,
    },
    FixExtraFile {
        path: &'a Utf8Path,
        check_stale: &'a CheckStale,
    },
}

impl<'a> Display for Fix<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(match self {
            Fix::DeleteFiles { files } => {
                writeln!(f, "fix: delete files: {files}")?;
            }
            Fix::FixLockstepFile { generated } => {
                // XXX-dap add diff
                writeln!(
                    f,
                    "fix: rewrite lockstep file {} from generated",
                    generated.spec_file_name().path()
                )?;
            }
            Fix::FixVersionedFiles { old, generated } => {
                if !old.0.is_empty() {
                    writeln!(f, "fix: remove old files: {old}")?;
                }
                writeln!(
                    f,
                    "fix: write new file {} from generated",
                    generated.spec_file_name().path()
                )?;
            }
            Fix::FixExtraFile { path, check_stale } => {
                // XXX-dap add diff
                let label = match check_stale {
                    CheckStale::Modified { .. } => "rewrite",
                    CheckStale::New { .. } => "write new",
                };
                writeln!(f, "fix: {label} file {path} from generated")?;
            }
        })
    }
}

impl<'a> Fix<'a> {
    pub fn execute(&self, env: &Environment) -> anyhow::Result<()> {
        let root = env.openapi_dir();
        match self {
            Fix::DeleteFiles { files } => {
                for f in &files.0 {
                    let path = root.join(f.path());
                    fs_err::remove_file(&path)?;
                    // XXX-dap maybe return a representation instead
                    eprintln!("removed {}", path);
                }
            }
            Fix::FixLockstepFile { generated } => {
                let path = root.join(generated.spec_file_name().path());
                eprintln!(
                    "updated {}: {:?}",
                    &path,
                    overwrite_file(&path, generated.contents())?
                );
            }
            Fix::FixVersionedFiles { old, generated } => {
                for f in &old.0 {
                    let path = root.join(f.path());
                    fs_err::remove_file(&path)?;
                    eprintln!("removed {}", path);
                }

                let path = root.join(generated.spec_file_name().path());
                eprintln!(
                    "created {}: {:?}",
                    &path,
                    overwrite_file(&path, generated.contents())?
                );
                eprintln!(
                    "FIX NOTE: be sure to update the corresponding \
                     progenitor client to refer to this new OpenAPI \
                     document file!"
                );
            }
            Fix::FixExtraFile { path, check_stale } => {
                let expected_contents = match check_stale {
                    CheckStale::Modified { expected, .. } => expected,
                    CheckStale::New { expected } => expected,
                };
                eprintln!(
                    "write {}: {:?}",
                    &path,
                    overwrite_file(&path, expected_contents)?
                );
            }
        }

        Ok(())
    }
}

// XXX-dap enum of safeties?  and each thing can have a set of safeties that
// enable the fix?

/// Resolve differences between blessed spec(s), the generated spec, and any
/// local spec files for a given API
pub struct Resolved<'a> {
    notes: Vec<Note>,
    non_version_problems: Vec<Problem>,
    api_results: BTreeMap<ApiIdent, BTreeMap<semver::Version, Resolution<'a>>>,
}

impl<'a> Resolved<'a> {
    pub fn new(
        env: &'a Environment,
        apis: &'a ManagedApis,
        blessed: &'a BlessedFiles,
        generated: &'a GeneratedFiles,
        local: &'a LocalFiles,
    ) -> Resolved<'a> {
        // First, assemble a list of supported versions for each API, as defined
        // in the Rust list of supported versions.  We'll use this to identify
        // any blessed spec files or local spec files that don't belong at all.
        let supported_versions_by_api: BTreeMap<
            &ApiIdent,
            BTreeSet<&semver::Version>,
        > = apis
            .iter_apis()
            .map(|api| {
                (
                    api.ident(),
                    api.iter_versions_semver().collect::<BTreeSet<_>>(),
                )
            })
            .collect();

        // Get one easy case out of the way: if there are any blessed API
        // versions that aren't supported any more, note that.
        let notes = resolve_removed_blessed_versions(
            &supported_versions_by_api,
            blessed,
        )
        .map(|(ident, version)| Note::BlessedVersionRemoved {
            api_ident: ident.clone(),
            version: version.clone(),
        })
        .collect();

        // Get the other easy case out of the way: if there are any local spec
        // files for APIs or API versions that aren't supported any more, that's
        // a (fixable) problem.
        let non_version_problems =
            resolve_orphaned_local_specs(&supported_versions_by_api, local)
                .map(|spec_file_name| Problem::LocalSpecFilesOrphaned {
                    // XXX-dap is this just one or many?
                    spec_file_names: DisplayableVec(vec![
                        spec_file_name.clone()
                    ]),
                })
                .collect();

        // Now resolve each of the supported API versions.
        let api_results = apis
            .iter_apis()
            .map(|api| {
                let ident = api.ident().clone();
                let api_blessed = blessed.spec_files.get(&ident);
                // We should have generated an API for every supported version.
                let api_generated = generated.spec_files.get(&ident).unwrap();
                let api_local = local.spec_files.get(&ident);
                (
                    api.ident().clone(),
                    resolve_api(
                        env,
                        api,
                        api_blessed,
                        api_generated,
                        api_local,
                    ),
                )
            })
            .collect();

        Resolved { notes, non_version_problems, api_results }
    }

    pub fn notes(&self) -> impl Iterator<Item = &Note> + '_ {
        self.notes.iter()
    }

    pub fn general_problems(&self) -> impl Iterator<Item = &Problem> + '_ {
        self.non_version_problems.iter()
    }

    pub fn resolution_for_api_version(
        &self,
        ident: &ApiIdent,
        version: &semver::Version,
    ) -> Option<&Resolution> {
        self.api_results.get(ident).and_then(|v| v.get(version))
    }
}

fn resolve_removed_blessed_versions<'a>(
    supported_versions_by_api: &'a BTreeMap<
        &'a ApiIdent,
        BTreeSet<&'a semver::Version>,
    >,
    blessed: &'a BlessedFiles,
) -> impl Iterator<Item = (&'a ApiIdent, &'a semver::Version)> + 'a {
    blessed.spec_files.iter().flat_map(|(ident, version_map)| {
        let set = supported_versions_by_api.get(ident);
        version_map.keys().filter_map(move |version| match set {
            Some(set) if set.contains(version) => None,
            _ => Some((ident, version)),
        })
    })
}

fn resolve_orphaned_local_specs<'a>(
    supported_versions_by_api: &'a BTreeMap<
        &'a ApiIdent,
        BTreeSet<&'a semver::Version>,
    >,
    local: &'a LocalFiles,
) -> impl Iterator<Item = &'a ApiSpecFileName> + 'a {
    local.spec_files.iter().flat_map(|(ident, version_map)| {
        let set = supported_versions_by_api.get(ident);
        version_map
            .iter()
            .filter_map(move |(version, files)| match set {
                Some(set) if !set.contains(version) => {
                    Some(files.iter().map(|f| f.spec_file_name()))
                }
                _ => None,
            })
            .flatten()
    })
}

fn resolve_api<'a>(
    env: &'a Environment,
    api: &'a ManagedApi,
    api_blessed: Option<&'a BTreeMap<semver::Version, Vec<BlessedApiSpecFile>>>,
    api_generated: &'a BTreeMap<semver::Version, Vec<GeneratedApiSpecFile>>,
    api_local: Option<&'a BTreeMap<semver::Version, Vec<LocalApiSpecFile>>>,
) -> BTreeMap<semver::Version, Resolution<'a>> {
    if api.is_lockstep() {
        resolve_api_lockstep(env, api, api_generated, api_local)
    } else {
        api.iter_versions_semver()
            .map(|version| {
                let version = version.clone();
                let blessed =
                    api_blessed.and_then(|b| b.get(&version)).map(|list| {
                        // XXX-dap validate this and have the type reflect it; or
                        // else fail gracefully here
                        assert_eq!(list.len(), 1);
                        list.iter().next().unwrap()
                    });
                // XXX-dap validate this and have the type reflect it; or else
                // fail gracefully here
                let generated = api_generated
                    .get(&version)
                    .map(|list| {
                        assert_eq!(list.len(), 1);
                        list.iter().next().unwrap()
                    })
                    .unwrap();
                let local = api_local
                    .and_then(|b| b.get(&version))
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let resolution = resolve_api_version(
                    env, api, &version, blessed, generated, local,
                );
                (version, resolution)
            })
            .collect()
    }
}

#[derive(Debug, Error)]
enum OnlyError {
    #[error("list was unexpectedly empty")]
    Empty,

    #[error(
        "unexpectedly found at least two elements in one-element list:
         {0} {1}"
    )]
    // Store the debug representations directly here rather than the values
    // so that `OnlyError: 'static` (so that it can be used as the cause of
    // another error) even when `T` is not 'static.
    Extra(String, String),
}

fn iter_only<T: Debug>(
    mut iter: impl Iterator<Item = T>,
) -> Result<T, OnlyError> {
    let first = iter.next().ok_or(OnlyError::Empty)?;
    match iter.next() {
        None => Ok(first),
        Some(second) => Err(OnlyError::Extra(
            format!("{:?}", first),
            format!("{:?}", second),
        )),
    }
}

fn resolve_api_lockstep<'a>(
    env: &'a Environment,
    api: &'a ManagedApi,
    api_generated: &'a BTreeMap<semver::Version, Vec<GeneratedApiSpecFile>>,
    api_local: Option<&'a BTreeMap<semver::Version, Vec<LocalApiSpecFile>>>,
) -> BTreeMap<semver::Version, Resolution<'a>> {
    assert!(api.is_lockstep());

    // unwrap(): Lockstep APIs have exactly one version.
    let version = iter_only(api.iter_versions_semver())
        .with_context(|| {
            format!("list of versions for lockstep API {}", api.ident())
        })
        .unwrap();

    // unwrap(): We should always have generated an OpenAPI document for
    // each supported version.
    let generated_for_version = api_generated
        .get(version)
        .expect("at least one OpenAPI document for version of lockstep API");

    // unwrap(): a given supported version only ever has one generated
    // OpenAPI document.
    let generated = iter_only(generated_for_version.iter())
        .with_context(|| {
            format!(
                "list of generated OpenAPI documents for lockstep API {:?}",
                api.ident(),
            )
        })
        .unwrap();

    // We may or may not have found a local OpenAPI document for this API.
    let local = api_local
        .and_then(|by_version| by_version.get(version))
        .and_then(|list| match &list.as_slice() {
            &[first] => Some(first),
            &[] => None,
            items => {
                // Structurally, it's not possible to have more than one
                // local file for a lockstep API because the file is named
                // by the API itself.
                panic!(
                    "unexpectedly found more than one local OpenAPI \
                     document for lockstep API {}: {:?}",
                    api.ident(),
                    items
                );
            }
        });

    let mut problems = Vec::new();

    // Validate the generated API document.
    validate_generated(env, api, version, generated, &mut problems);

    match local {
        Some(local_file) if local_file.contents() == generated.contents() => (),
        Some(stale) => problems.push(Problem::LocalVersionStale {
            spec_file_names: DisplayableVec(vec![stale
                .spec_file_name()
                .clone()]),
        }),
        None => problems.push(Problem::LocalVersionMissingLocal),
    };

    BTreeMap::from([((
        version.clone(),
        Resolution::new_lockstep(generated, problems),
    ))])
}

fn resolve_api_version<'a>(
    env: &'_ Environment,
    api: &'_ ManagedApi,
    version: &'_ semver::Version,
    blessed: Option<&'a BlessedApiSpecFile>,
    generated: &'a GeneratedApiSpecFile,
    local: &'a [LocalApiSpecFile],
) -> Resolution<'a> {
    match blessed {
        Some(blessed) => resolve_api_version_blessed(
            env, api, version, blessed, generated, local,
        ),
        None => resolve_api_version_local(env, api, version, generated, local),
    }
}

fn resolve_api_version_blessed<'a>(
    env: &'_ Environment,
    api: &'_ ManagedApi,
    version: &'_ semver::Version,
    blessed: &'a BlessedApiSpecFile,
    generated: &'a GeneratedApiSpecFile,
    local: &'a [LocalApiSpecFile],
) -> Resolution<'a> {
    let mut problems = Vec::new();

    // Validate the generated API document.
    validate_generated(env, api, version, generated, &mut problems);

    // First off, the blessed spec must be a subset of the generated one.
    // If not, someone has made an incompatible change to the API
    // *implementation*, such that the implementation no longer faithfully
    // implements this older, supported version.
    match api_compatible(blessed.openapi(), generated.openapi()) {
        Ok(compatibility_issues) if !compatibility_issues.is_empty() => {
            problems.push(Problem::BlessedVersionBroken {
                compatibility_issues: DisplayableVec(compatibility_issues),
            });
        }
        Ok(_) => (),
        Err(error) => {
            // XXX-dap make this a real Problem?
            panic!("failed to check OpenAPI compatibility: {error:#}");
        }
    }

    // Now, there should be at least one local spec that exactly matches the
    // blessed one.
    // XXX-dap this could just check hashes, once we implement that and if we're
    // sure that it will be robust.
    let (matching, non_matching): (Vec<_>, Vec<_>) =
        local.iter().partition(|local| local.contents() == blessed.contents());
    if matching.is_empty() {
        // XXX-dap it would be weird if there were _more_ than one matching one.
        problems.push(Problem::BlessedVersionMissingLocal {
            spec_file_name: blessed.spec_file_name().clone(),
        })
    }
    // There shouldn't be any local specs that match the same version but don't
    // match the same contents.
    if !non_matching.is_empty() {
        let spec_file_names = DisplayableVec(
            non_matching
                .into_iter()
                .map(|s| s.spec_file_name().clone())
                .collect(),
        );
        problems.push(Problem::BlessedVersionExtraLocalSpec { spec_file_names })
    }

    Resolution::new_blessed(problems)
}

fn resolve_api_version_local<'a>(
    env: &'_ Environment,
    api: &'_ ManagedApi,
    version: &'_ semver::Version,
    generated: &'a GeneratedApiSpecFile,
    local: &'a [LocalApiSpecFile],
) -> Resolution<'a> {
    let mut problems = Vec::new();

    // Validate the generated API document.
    validate_generated(env, api, version, generated, &mut problems);

    let (matching, non_matching): (Vec<_>, Vec<_>) = local
        .iter()
        .partition(|local| local.contents() == generated.contents());
    let spec_file_names = DisplayableVec(
        non_matching.iter().map(|s| s.spec_file_name().clone()).collect(),
    );

    if matching.is_empty() {
        // There was no matching spec.
        if non_matching.is_empty() {
            // There were no non-matching specs, either.
            problems.push(Problem::LocalVersionMissingLocal);
        } else {
            // There were non-matching specs.  This is your basic "stale" case.
            problems.push(Problem::LocalVersionStale { spec_file_names });
        }
    } else if !non_matching.is_empty() {
        // There was a matching spec, but also some non-matching ones.
        // These are superfluous.  (It's not clear how this could happen.)
        problems.push(Problem::LocalVersionExtra { spec_file_names });
    }

    Resolution::new_new_locally(generated, problems)
}

fn validate_generated(
    env: &Environment,
    api: &ManagedApi,
    version: &semver::Version,
    generated: &GeneratedApiSpecFile,
    problems: &mut Vec<Problem>,
) {
    match validate(env, api, generated) {
        Err(source) => {
            problems.push(Problem::GeneratedValidationError {
                api_ident: api.ident().clone(),
                version: version.clone(),
                source,
            });
        }
        Ok(extra_files) => {
            // XXX-dap it would be nice if the data model accounted for the fact
            // that these extra files exist (so that we could report that we
            // checked them).
            for (path, status) in extra_files {
                match status {
                    CheckStatus::Fresh => (),
                    CheckStatus::Stale(check_stale) => {
                        problems.push(Problem::ExtraFileStale {
                            api_ident: api.ident().clone(),
                            path,
                            check_stale,
                        });
                    }
                }
            }
        }
    }
}

fn validate(
    env: &Environment,
    api: &ManagedApi,
    generated: &GeneratedApiSpecFile,
) -> anyhow::Result<Vec<(Utf8PathBuf, CheckStatus)>> {
    let openapi = generated.openapi();
    let validation_result = validate_generated_openapi_document(api, &openapi)?;
    let extra_files = validation_result
        .extra_files
        .into_iter()
        .map(|(path, contents)| {
            let full_path = env.workspace_root.join(&path);
            let status = check_file(full_path, contents)?;
            Ok((path, status))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(extra_files)
}

/// Check a file against expected contents.
fn check_file(
    full_path: Utf8PathBuf,
    contents: Vec<u8>,
) -> anyhow::Result<CheckStatus> {
    let existing_contents =
        read_opt(&full_path).context("failed to read contents on disk")?;

    match existing_contents {
        Some(existing_contents) if existing_contents == contents => {
            Ok(CheckStatus::Fresh)
        }
        Some(existing_contents) => {
            Ok(CheckStatus::Stale(CheckStale::Modified {
                full_path,
                actual: existing_contents,
                expected: contents,
            }))
        }
        None => Ok(CheckStatus::Stale(CheckStale::New { expected: contents })),
    }
}

fn read_opt(path: &Utf8Path) -> std::io::Result<Option<Vec<u8>>> {
    match fs_err::read(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => return Err(err),
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) enum CheckStatus {
    Fresh,
    Stale(CheckStale),
}

#[derive(Debug)]
#[must_use]
pub(crate) enum CheckStale {
    Modified { full_path: Utf8PathBuf, actual: Vec<u8>, expected: Vec<u8> },
    New { expected: Vec<u8> },
}

#[derive(Debug)]
#[must_use]
pub(crate) enum OverwriteStatus {
    Updated,
    Unchanged,
}

/// Overwrite a file with new contents, if the contents are different.
///
/// The file is left unchanged if the contents are the same. That's to avoid
/// mtime-based recompilations.
fn overwrite_file(
    path: &Utf8Path,
    contents: &[u8],
) -> anyhow::Result<OverwriteStatus> {
    // Only overwrite the file if the contents are actually different.
    let existing_contents =
        read_opt(path).context("failed to read contents on disk")?;

    // None means the file doesn't exist, in which case we always want to write
    // the new contents.
    if existing_contents.as_deref() == Some(contents) {
        return Ok(OverwriteStatus::Unchanged);
    }

    AtomicFile::new(path, atomicwrites::OverwriteBehavior::AllowOverwrite)
        .write(|f| f.write_all(contents))
        .with_context(|| format!("failed to write to `{}`", path))?;

    Ok(OverwriteStatus::Updated)
}
