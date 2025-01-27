// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resolve different sources of API information (blessed, local, upstream)

use crate::apis::ApiIdent;
use crate::apis::ManagedApi;
use crate::apis::ManagedApis;
use crate::combined::DisplayableVec;
use crate::spec_files_blessed::BlessedApiSpecFile;
use crate::spec_files_blessed::BlessedFiles;
use crate::spec_files_generated::GeneratedApiSpecFile;
use crate::spec_files_generated::GeneratedFiles;
use crate::spec_files_generic::ApiSpecFileName;
use crate::spec_files_local::LocalApiSpecFile;
use crate::spec_files_local::LocalFiles;
use openapi_manager_types::SupportedVersion;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use thiserror::Error;

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
pub enum Resolution {
    NoProblems,
    Problems(Vec<Problem>),
}

/// Describes a problem resolving the blessed spec(s), generated spec(s), and
/// local spec files for a particular API
#[derive(Debug, Error)]
pub enum Problem {
    // This kind of problem is not associated with any *supported* version of an
    // API.  (All the others are.)
    #[error(
        "One or more local spec files were found that do not correspond to a \
         supported version of this API: {spec_file_names:?}.  This is unusual, \
         but it could happen if you created this version of the API in this \
         branch, then later changed it (maybe because you merged with upstream \
         and had to adjust the version number for your changes).  In that \
         case, this tool can remove the unused file for you."
    )]
    LocalSpecFilesOrphaned { spec_file_names: Vec<ApiSpecFileName> },

    // All other problems are associated with specific supported versions of an
    // API.
    #[error(
        "This version is blessed, and it's a supported version, but it's \
         missing a local spec file.  This is unusual.  If you intended to \
         remove this version, you must also update the list of supported \
         versions in Rust.  If you didn't, restore the file from git: \
         {spec_file_name:?}"
    )]
    BlessedVersionMissingLocal { spec_file_name: ApiSpecFileName },

    #[error(
        "Found extra local file for blessed version that does not match the \
         blessed (upstream) spec file: {spec_file_name:?}.  This can happen if \
         you created this version of the API in this branch, then merged with \
         an upstream commit that also added the same version number.  In that \
         case, you likely already bumped your local version number (when you \
         merged the list of supported versions in Rust) and this file is \
         vestigial. This tool can remove the unused file for you."
    )]
    // XXX-dap should this have a Vec<ApiSpecFileName> to be parallel with
    // LocalVersionStale?
    BlessedVersionExtraLocalSpec { spec_file_name: ApiSpecFileName },

    #[error(
        "Spec generated from the current code is not compatible with the \
         blessed spec (from upstream)"
    )]
    BlessedVersionBroken {
        #[source]
        compatibility_issues: OpenApiIncompatibilityProblems,
    },

    #[error(
        "No local spec file was found and this is not a blessed version, \
         either.  This is normal if you have just added this version number. \
         This tool can generate the file for you."
    )]
    LocalVersionMissingLocal,

    #[error(
        "Spec generated from the current code is not compatible with this \
         locally-added spec: {spec_file_names}.  This tool can update the \
         local file(s) for you."
    )]
    // Since the filename has its own hash in it, when the local file is stale,
    // it's not that the file contents will be wrong, but rather that there will
    // be one or more _incorrect_ files and the correct one will be missing.
    // The fix will be to remove all the incorrect ones and add the correct one.
    // XXX-dap this is going to be really annoying in that when iterating on
    // local changes, you will have to update the clients to point at the new
    // hashes all the time.  We could stop putting the checksum into the
    // filenames?  But then you could easily forget to update the client if you
    // bumped your local version.  Really, it'd be nice if the client filename
    // was somehow checked by this tool.  Maybe this library should define
    // constants like API_NAME_LATEST that are used in the client specs?
    LocalVersionStale { spec_file_names: DisplayableVec<ApiSpecFileName> },
}

// enum of safeties?  and each thing can have a set of safeties that enable the
// fix?

// XXX-dap
#[derive(Debug, Error)]
#[error("XXX-dap")] // XXX-dap
struct OpenApiIncompatibilityProblems {}

/// Resolve differences between blessed spec(s), the generated spec, and any
/// local spec files for a given API
pub struct Resolved {
    notes: Vec<Note>,
    non_version_problems: Vec<Problem>,
    api_results: BTreeMap<ApiIdent, BTreeMap<semver::Version, Resolution>>,
}

impl Resolved {
    pub fn new(
        apis: &ManagedApis,
        blessed: &BlessedFiles,
        generated: &GeneratedFiles,
        local: &LocalFiles,
    ) -> Resolved {
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
                    spec_file_names: vec![spec_file_name.clone()],
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
                    resolve_api(api, api_blessed, api_generated, api_local),
                )
            })
            .collect();

        Resolved { notes, non_version_problems, api_results }
    }
}

fn resolve_removed_blessed_versions<'a>(
    supported_versions_by_api: &'a BTreeMap<
        &'a ApiIdent,
        BTreeSet<&'a semver::Version>,
    >,
    blessed: &'a BlessedFiles,
) -> impl Iterator<Item = (&'a ApiIdent, &'a semver::Version)> + 'a {
    let mut notes = Vec::new();
    for (ident, version_map) in &blessed.spec_files {
        let supported = supported_versions_by_api.get(ident);
        for version in version_map.keys() {
            match supported {
                Some(set) if set.contains(version) => (),
                _ => notes.push((ident, version)),
            };
        }
    }

    // XXX-dap TODO-cleanup rewrite like orphaned_local_specs()
    notes.into_iter()
}

fn resolve_orphaned_local_specs<'a>(
    supported_versions_by_api: &'a BTreeMap<
        &'a ApiIdent,
        BTreeSet<&'a semver::Version>,
    >,
    local: &'a LocalFiles,
) -> impl Iterator<Item = (&'a ApiSpecFileName)> + 'a {
    local.spec_files.iter().flat_map(|(ident, version_map)| {
        let set = supported_versions_by_api.get(ident);
        version_map
            .iter()
            .filter_map(move |(version, files)| match set {
                Some(set) if set.contains(version) => {
                    Some(files.iter().map(|f| f.spec_file_name()))
                }
                _ => None,
            })
            .flatten()
    })
}

fn resolve_api(
    api: &ManagedApi,
    api_blessed: Option<&BTreeMap<semver::Version, Vec<BlessedApiSpecFile>>>,
    api_generated: &BTreeMap<semver::Version, Vec<GeneratedApiSpecFile>>,
    api_local: Option<&BTreeMap<semver::Version, Vec<LocalApiSpecFile>>>,
) -> BTreeMap<semver::Version, Resolution> {
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
            let resolution =
                resolve_api_version(api, &version, blessed, generated, local);
            (version, resolution)
        })
        .collect()
}

fn resolve_api_version(
    api: &ManagedApi,
    version: &semver::Version,
    blessed: Option<&BlessedApiSpecFile>,
    generated: &GeneratedApiSpecFile,
    local: &[LocalApiSpecFile],
) -> Resolution {
    match blessed {
        Some(blessed) => {
            resolve_api_version_blessed(api, version, blessed, generated, local)
        }
        None => resolve_api_version_local(api, version, generated, local),
    }
}

fn resolve_api_version_blessed(
    api: &ManagedApi,
    version: &semver::Version,
    blessed: &BlessedApiSpecFile,
    generated: &GeneratedApiSpecFile,
    local: &[LocalApiSpecFile],
) -> Resolution {
    todo!(); // XXX-dap working here
}

fn resolve_api_version_local(
    api: &ManagedApi,
    version: &semver::Version,
    generated: &GeneratedApiSpecFile,
    local: &[LocalApiSpecFile],
) -> Resolution {
    todo!(); // XXX-dap working here
}
