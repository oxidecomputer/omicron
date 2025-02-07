// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resolve different sources of API information (blessed, local, upstream)

use crate::apis::ApiIdent;
use crate::apis::ManagedApi;
use crate::apis::ManagedApis;
use crate::compatibility::api_compatible;
use crate::compatibility::OpenApiCompatibilityError;
use crate::environment::Environment;
use crate::iter_only::iter_only;
use crate::output::plural;
use crate::spec_files_blessed::BlessedApiSpecFile;
use crate::spec_files_blessed::BlessedFiles;
use crate::spec_files_generated::GeneratedApiSpecFile;
use crate::spec_files_generated::GeneratedFiles;
use crate::spec_files_generic::ApiSpecFileName;
use crate::spec_files_local::LocalApiSpecFile;
use crate::spec_files_local::LocalFiles;
use crate::validation::validate_generated_openapi_document;
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
    #[allow(dead_code)]
    // This field is not currently used, but it's logically very significant.
    // The set of problems that can appear in `problems` depends on this `kind`.
    // It's clearer to have this here and may also be useful in the future.
    kind: ResolutionKind,
    problems: Vec<Problem<'a>>,
}

impl<'a> Resolution<'a> {
    pub fn new_lockstep(problems: Vec<Problem<'a>>) -> Resolution<'a> {
        Resolution { kind: ResolutionKind::Lockstep, problems }
    }

    pub fn new_blessed(problems: Vec<Problem<'a>>) -> Resolution<'a> {
        Resolution { kind: ResolutionKind::Blessed, problems }
    }

    pub fn new_new_locally(problems: Vec<Problem<'a>>) -> Resolution<'a> {
        Resolution { kind: ResolutionKind::NewLocally, problems }
    }

    pub fn has_problems(&self) -> bool {
        !self.problems.is_empty()
    }

    pub fn has_errors(&self) -> bool {
        self.problems().any(|p| !p.is_fixable())
    }

    pub fn problems(&self) -> impl Iterator<Item = &'_ Problem<'a>> + '_ {
        self.problems.iter()
    }
}

#[derive(Debug)]
pub enum ResolutionKind {
    /// This is a lockstep API
    Lockstep,
    /// This is a versioned API and this version is blessed
    Blessed,
    /// This version is new to the current workspace (i.e., not present
    /// upstream)
    NewLocally,
}

/// Describes a problem resolving the blessed spec(s), generated spec(s), and
/// local spec files for a particular API
#[derive(Debug, Error)]
pub enum Problem<'a> {
    // This kind of problem is not associated with any *supported* version of an
    // API.  (All the others are.)
    #[error(
        "A local spec file was found that does not correspond to a \
         supported version of this API: {spec_file_name}.  This is unusual, \
         but it could happen if you created this version of the API in this \
         branch, then later changed it (maybe because you merged with upstream \
         and had to adjust the version number for your changes).  In that \
         case, this tool can remove the unused file for you."
    )]
    LocalSpecFileOrphaned { spec_file_name: ApiSpecFileName },

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
        "No local spec file was found for lockstep API.  This is only \
         expected if you're adding a new lockstep API.  This tool can \
         generate the file for you."
    )]
    LockstepMissingLocal { generated: &'a GeneratedApiSpecFile },

    #[error(
        "Spec generated from the current code does not match this lockstep \
         spec: {:?}.  This tool can update the local file for \
         you.", generated.spec_file_name().path()
    )]
    LockstepStale {
        found: &'a LocalApiSpecFile,
        generated: &'a GeneratedApiSpecFile,
    },

    #[error(
        "No local spec file was found for locally-added API version.  \
         This is normal if you have added or changed this API version.  \
         This tool can generate the file for you."
    )]
    LocalVersionMissingLocal { generated: &'a GeneratedApiSpecFile },

    #[error(
        "Extra (incorrect) spec files were found for non-blessed version: \
         {spec_file_names}.  This tool can remove the files for you."
    )]
    LocalVersionExtra { spec_file_names: DisplayableVec<ApiSpecFileName> },

    #[error(
        "Spec generated from the current code does not match spec file(s) for \
         locally-new API: {}.  This tool can update the local file(s) for you.",
        DisplayableVec(
            spec_files.iter().map(|s| s.spec_file_name().to_string()).collect()
        )
    )]
    // For versioned APIs, since the filename has its own hash in it, when the
    // local file is stale, it's not that the file contents will be wrong, but
    // rather that there will be one or more _incorrect_ files and the correct
    // one will be missing.  The fix will be to remove all the incorrect ones
    // and add the correct one.
    LocalVersionStale {
        spec_files: Vec<&'a LocalApiSpecFile>,
        generated: &'a GeneratedApiSpecFile,
    },

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

impl<'a> Problem<'a> {
    pub fn is_fixable(&self) -> bool {
        self.fix().is_some()
    }

    pub fn fix(&'a self) -> Option<Fix<'a>> {
        match self {
            Problem::LocalSpecFileOrphaned { spec_file_name } => {
                Some(Fix::DeleteFiles {
                    files: DisplayableVec(vec![spec_file_name.clone()]),
                })
            }
            Problem::BlessedVersionMissingLocal { .. } => None,
            Problem::BlessedVersionExtraLocalSpec { spec_file_names } => {
                Some(Fix::DeleteFiles { files: spec_file_names.clone() })
            }
            Problem::BlessedVersionBroken { .. } => None,
            Problem::LockstepMissingLocal { generated }
            | Problem::LockstepStale { generated, .. } => {
                Some(Fix::FixLockstepFile { generated })
            }
            Problem::LocalVersionMissingLocal { generated } => {
                Some(Fix::FixVersionedFiles {
                    old: DisplayableVec(Vec::new()),
                    generated,
                })
            }
            Problem::LocalVersionExtra { spec_file_names } => {
                Some(Fix::DeleteFiles { files: spec_file_names.clone() })
            }
            Problem::LocalVersionStale { spec_files, generated } => {
                Some(Fix::FixVersionedFiles {
                    old: DisplayableVec(
                        spec_files.iter().map(|s| s.spec_file_name()).collect(),
                    ),
                    generated,
                })
            }
            Problem::GeneratedValidationError { .. } => None,
            Problem::ExtraFileStale { path, check_stale, .. } => {
                Some(Fix::FixExtraFile { path, check_stale })
            }
        }
    }
}

pub enum Fix<'a> {
    DeleteFiles {
        files: DisplayableVec<ApiSpecFileName>,
    },
    FixLockstepFile {
        generated: &'a GeneratedApiSpecFile,
    },
    FixVersionedFiles {
        old: DisplayableVec<&'a ApiSpecFileName>,
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
                writeln!(
                    f,
                    "delete {}: {files}",
                    plural::files(files.0.len())
                )?;
            }
            Fix::FixLockstepFile { generated } => {
                writeln!(
                    f,
                    "rewrite lockstep file {} from generated",
                    generated.spec_file_name().path()
                )?;
            }
            Fix::FixVersionedFiles { old, generated } => {
                if !old.0.is_empty() {
                    writeln!(
                        f,
                        "remove old {}: {old}",
                        plural::files(old.0.len())
                    )?;
                }
                writeln!(
                    f,
                    "write new file {} from generated",
                    generated.spec_file_name().path()
                )?;
            }
            Fix::FixExtraFile { path, check_stale } => {
                let label = match check_stale {
                    CheckStale::Modified { .. } => "rewrite",
                    CheckStale::New { .. } => "write new",
                };
                writeln!(f, "{label} file {path} from generated")?;
            }
        })
    }
}

impl<'a> Fix<'a> {
    pub fn execute(&self, env: &Environment) -> anyhow::Result<Vec<String>> {
        let root = env.openapi_dir();
        match self {
            Fix::DeleteFiles { files } => {
                let mut rv = Vec::new();
                for f in &files.0 {
                    let path = root.join(f.path());
                    fs_err::remove_file(&path)?;
                    rv.push(format!("removed {}", path));
                }
                Ok(rv)
            }
            Fix::FixLockstepFile { generated } => {
                let path = root.join(generated.spec_file_name().path());
                Ok(vec![format!(
                    "updated {}: {:?}",
                    &path,
                    overwrite_file(&path, generated.contents())?
                )])
            }
            Fix::FixVersionedFiles { old, generated } => {
                let mut rv = Vec::new();
                for f in &old.0 {
                    let path = root.join(f.path());
                    fs_err::remove_file(&path)?;
                    rv.push(format!("removed {}", path));
                }

                let path = root.join(generated.spec_file_name().path());
                rv.push(format!(
                    "created {}: {:?}",
                    &path,
                    overwrite_file(&path, generated.contents())?
                ));
                rv.push(format!(
                    "FIX NOTE: be sure to update the corresponding \
                     progenitor client to refer to this new OpenAPI \
                     document file!"
                ));
                Ok(rv)
            }
            Fix::FixExtraFile { path, check_stale } => {
                let expected_contents = match check_stale {
                    CheckStale::Modified { expected, .. } => expected,
                    CheckStale::New { expected } => expected,
                };
                Ok(vec![format!(
                    "write {}: {:?}",
                    &path,
                    overwrite_file(&path, expected_contents)?
                )])
            }
        }
    }
}

/// Resolve differences between blessed spec(s), the generated spec, and any
/// local spec files for a given API
pub struct Resolved<'a> {
    notes: Vec<Note>,
    non_version_problems: Vec<Problem<'a>>,
    api_results: BTreeMap<ApiIdent, BTreeMap<semver::Version, Resolution<'a>>>,
    nexpected_documents: usize,
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

        let nexpected_documents = supported_versions_by_api
            .values()
            .map(|v| v.len())
            .fold(0, |sum_so_far, count| sum_so_far + count);

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
                .map(|spec_file_name| Problem::LocalSpecFileOrphaned {
                    spec_file_name: spec_file_name.clone(),
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

        Resolved {
            notes,
            non_version_problems,
            api_results,
            nexpected_documents,
        }
    }

    pub fn nexpected_documents(&self) -> usize {
        self.nexpected_documents
    }

    pub fn notes(&self) -> impl Iterator<Item = &Note> + '_ {
        self.notes.iter()
    }

    // XXX-dap we should have one of these for unrecognized local files
    // XXX-dap is this where we should put the errors or warnings from loading?
    pub fn general_problems(&self) -> impl Iterator<Item = &Problem<'a>> + '_ {
        self.non_version_problems.iter()
    }

    pub fn resolution_for_api_version(
        &self,
        ident: &ApiIdent,
        version: &semver::Version,
    ) -> Option<&Resolution> {
        self.api_results.get(ident).and_then(|v| v.get(version))
    }

    pub fn has_unfixable_problems(&self) -> bool {
        self.general_problems().any(|p| !p.is_fixable())
            || self
                .api_results
                .values()
                .any(|version_map| version_map.values().any(|r| r.has_errors()))
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
        Some(found) => {
            problems.push(Problem::LockstepStale { found, generated })
        }
        None => problems.push(Problem::LockstepMissingLocal { generated }),
    };

    BTreeMap::from([((version.clone(), Resolution::new_lockstep(problems)))])
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
    let issues = api_compatible(blessed.openapi(), generated.openapi());
    if !issues.is_empty() {
        problems.push(Problem::BlessedVersionBroken {
            compatibility_issues: DisplayableVec(issues),
        });
    }

    // Now, there should be at least one local spec that exactly matches the
    // blessed one.
    let (matching, non_matching): (Vec<_>, Vec<_>) =
        local.iter().partition(|local| {
            // It should be enough to compare the hashes, since we should have
            // already validated that the hashes are correct for the contents.
            // But while it's cheap enough to do, we may as well compare the
            // contents, too, and make sure we haven't messed something up.
            let contents_match = local.contents() == blessed.contents();
            let local_hash = local.spec_file_name().hash().expect(
                "this should be a versioned file so it should have a hash",
            );
            let blessed_hash = blessed.spec_file_name().hash().expect(
                "this should be a versioned file so it should have a hash",
            );
            let hashes_match = local_hash == blessed_hash;
            // If the hashes are equal, the contents should be equal, and vice
            // versa.
            assert_eq!(hashes_match, contents_match);
            hashes_match
        });
    if matching.is_empty() {
        problems.push(Problem::BlessedVersionMissingLocal {
            spec_file_name: blessed.spec_file_name().clone(),
        })
    } else {
        // The specs are identified by, among other things, their hash.  Thus,
        // to have two matching specs (i.e., having the same contents), we'd
        // have to have a hash collision.  This is conceivable but unlikely
        // enough that this is more likely a logic bug.
        assert_eq!(matching.len(), 1);
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

    if matching.is_empty() {
        // There was no matching spec.
        if non_matching.is_empty() {
            // There were no non-matching specs, either.
            problems.push(Problem::LocalVersionMissingLocal { generated });
        } else {
            // There were non-matching specs.  This is your basic "stale" case.
            problems.push(Problem::LocalVersionStale {
                spec_files: non_matching,
                generated,
            });
        }
    } else if !non_matching.is_empty() {
        // There was a matching spec, but also some non-matching ones.
        // These are superfluous.  (It's not clear how this could happen.)
        let spec_file_names = DisplayableVec(
            non_matching.iter().map(|s| s.spec_file_name().clone()).collect(),
        );
        problems.push(Problem::LocalVersionExtra { spec_file_names });
    }

    Resolution::new_new_locally(problems)
}

fn validate_generated(
    env: &Environment,
    api: &ManagedApi,
    version: &semver::Version,
    generated: &GeneratedApiSpecFile,
    problems: &mut Vec<Problem<'_>>,
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
