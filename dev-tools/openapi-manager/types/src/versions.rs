// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used by trait-based API definitions to define the versions that they
//! support.

#[derive(Debug)]
pub struct SupportedVersion {
    semver: &'static semver::Version,
    label: &'static str,
}

impl SupportedVersion {
    pub const fn new(
        semver: &'static semver::Version,
        label: &'static str,
    ) -> SupportedVersion {
        SupportedVersion { semver, label }
    }

    pub fn semver(&self) -> &semver::Version {
        &self.semver
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}

#[derive(Debug)]
pub struct SupportedVersions {
    versions: Vec<SupportedVersion>,
}

impl SupportedVersions {
    pub fn new(versions: Vec<SupportedVersion>) -> SupportedVersions {
        assert!(
            !versions.is_empty(),
            "at least one version of an API must be supported"
        );
        // XXX-dap need to add other constraints:
        // - no semver is duplicated
        // - no id is duplicated
        // - look at schema_versions to see what else
        // XXX-dap should this be in a test instead?  downside is you could get
        // pretty far (running `cargo xtask openapi generate`) without noticing
        assert!(
            versions.iter().map(|v| v.semver()).is_sorted(),
            "list of supported versions for an API must be sorted to ensure \
             that upstream merges have been done correctly"
        );
        SupportedVersions { versions }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ SupportedVersion> + '_ {
        self.versions.iter()
    }
}

/// Helper macro used to define API versions
///
/// ```
/// api_versions!([
///     (2, 0, 0, ADD_FOOBAR_OPERATION, "add-foobar-operation"),
///     (1, 0, 0, INITIAL, "initial-version"),
/// ])
/// ```
///
/// This example says that there are two API versions: `1.0.0` (the initial
/// version) and `2.0.0` (which adds an operation called "foobar").  This macro
/// invocation defines symbolic constants of type `semver::Version` for each of
/// these, equivalent to:
///
/// ```
///     const VERSION_ADD_FOOBAR_OPERATION: semver::Version =
///         semver::Version::new(2, 0, 0);
///     const VERSION_INITIAL: semver::Version =
///         semver::Version::new(1, 0, 0);
/// ```
///
/// It also defines a function called `pub fn supported_versions() ->
/// SupportedVersions` that, as the name suggests, returns a
/// [`SupportedVersions`] that describes these two supported API versions.
// Design constraints:
// - For each new API version, we need a developer-chosen semver and a
//   developer-chosen string label to be used as an identifier.
// - We want to produce:
//   - a symbolic constant for each version that won't change if the developer
//     needs to change the semver value for this API version
//   - a list of supported API versions
// - Critically, we want to ensure that if two developers both add new API
//   versions in separate branches, whether or not they choose the same value,
//   there must be a git conflict that requires manual resolution.
//   - To achieve this, we put the list of versions in a list.
// - We want to make it hard to do this merge wrong without noticing.
//   - We want to require that the list be sorted (so that someone hasn't put
//     something in the wrong order).
//   - The list should have no duplicates.
// - We want to minimize boilerplate.
//
// That's how we've landed on defining API versions using this macro where:
// - each API definition is simple and fits on a single line
// - there will necessarily be a conflict if two people try to add a line in the
//   same spot of the file, even if they overlap, assuming they choose different
//   labels for their API version
// - the consumer of this value will be able to do those checks that help make
//   sure there wasn't a mismerge.

#[macro_export]
macro_rules! api_versions {
    ( [ $( (
        $major:literal,
        $minor:literal,
        $patch:literal,
        $name:ident,
        $desc:expr
    ) ),* $(,)? ] ) => {
        openapi_manager_types::paste! {
            $(
                static [<VERSION_ $name>]: semver::Version =
                    semver::Version::new($major, $minor, $patch);
            )*

            pub fn supported_versions() -> SupportedVersions {
                SupportedVersions::new(vec![
                    $( SupportedVersion::new(&[<VERSION_ $name>], $desc) ),*
                ])
            }
        }
    };
}
