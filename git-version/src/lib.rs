// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A very tiny crate for obtaining the current Git SHA of the `omicron`
//! repository.
//!
//! This crate defines the [`GitVersion`] struct, which represents a Git version
//! for the `omicron` repository, along with functions for [obtaining the
//! version that the program was built from](GitVersion::current), serializing
//! and deserializing it.
//!
//! This lives in its own crate, rather than someplace like `omicron-common`,
//! because the `vergen` stuff in the build script will invoke
//! `cargo::rerun_if_changed` and so on any time the Git SHA changes. For
//! incremental builds, like during development, we don't want to invalidate the
//! build cache for *every* crate in the workspace every time the developer
//! makes a commit; we'd rather just invalidate the ones that actually need the
//! Git version.
use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

/// A Git version for the `omicron` repository, including the SHA of the HEAD
/// commit and a flag indicating whether the repository is _dirty_ (has
/// uncommitted changes).
///
/// Two Git versions are considered equal if their SHA is the same *and* neither
/// has uncommitted changes. Two dirty versions with the same HEAD SHA will
/// never be considered equal, because it is impossible to determine whether or
/// not they have the same set of uncommitted changes.
///
/// Use [`GitVersion::current()`] to obtain the version of the repository when
/// this code was compiled.
///
/// A `GitVersion` may also be deserialized from a string using
/// [`GitVersion::from_str`], or its [`serde::Deserialize`] implementation. If
/// the string ends with the suffix `-dirty`, the version is considered dirty;
/// any other characters in the string are treated as the SHA.
// TODO(eliza): we could probably validate that the SHA consists only of the
// expected hex digits and is of the expected length, but that gets a bit
// complex and is not necessary here.
///
/// ## Known Limitations
///
/// Currently, the determination of whether or not the repository is "dirty"
/// (contains uncommitted changes) in [`GitVersion::current`] relies on a build
/// script for this crate, which may not be re-run under some circumstances. In
/// particular, the build script currently emits a `cargo::rerun-if-changed`
/// directive that ensures it is re-run any time the `.git/HEAD` file changes.
/// This ensures that the git version is re-generated whenever a commit is made
/// or a branch is checked out. In order to detect transitions between clean (no
/// uncommitted changes) and dirty (uncommitted changes) states reliably, the
/// build script must issue `cargo::rerun-if-changed` directives for...every
/// file in the workspace. If it does this unconditionally, this results in this
/// crate, and any other crates that depend on it, being rebuilt every time any
/// file in the workspace changes. This could substantially increase the amount
/// of code which must be compiled for incremental builds while a developer is
/// working in the repository, since any changes would invalidate the build
/// cache for all crates that depend on this one.
///
/// In the future, we could potentially enhance the build script to emit
/// rerun-if-changed directives only when it is in a clean state, so that any
/// change causes the git version to become dirty, but to cease doing so once
/// the repository has become dirty --- it is still dirty if additional files
/// change. While the repository is dirty, the build script would only emit
/// `cargo::rerun-if-changed` directives for `.git/HEAD`. This avoids
/// invalidating the build cache on any change in the repository, while
/// improving the accuracy of detecting dirty repos. However, it still has
/// potential deficiencies, such as missing a transition back to the "clean"
/// state due to _deleting_ uncommitted added files, rather than making a new
/// commit.
///
/// For now, we accept that the dirty flag may not always be set accurately,
/// since the primary purpose of including these versions is to compare the Git
/// SHAs of TUF repos produced by CI. In that case, we are comparing between
/// commits, and the repo should never be dirty. So...whatever. In the future,
/// we may want to improve this.
///
/// See [here][1] for discussion of this limitation.
///
/// [1]: https://github.com/oxidecomputer/omicron/pull/10578#discussion_r3384362440
#[derive(Debug, serde_with::DeserializeFromStr)]
pub struct GitVersion {
    // We use a `Cow` here so that we need not allocate when constructing a
    // `GitVersion` to represent the current state of the repository, as it can
    // always be backed by a single build-time string constant. This may not
    // *actually* matter that much, but it made me feel happier, especially
    // since (at time of writing) Nexus only uses the `current()` path, and will
    // never try to parse a git version from a string (the path that may
    // allocate).
    //
    // So even if we aren't doing this in a particularly hot path, I felt
    // pleased with being able to golf away the allocation in the path that
    // Nexus uses in production.
    sha: Cow<'static, str>,
    dirty: bool,
}

impl GitVersion {
    const DIRTY: &str = "-dirty";

    /// Returns a `GitVersion` representing the state of the repository when
    /// this code was compiled.
    pub fn current() -> Self {
        let dirty = env!("VERGEN_GIT_DIRTY") == "true";
        const SHA: &str = env!("VERGEN_GIT_SHA");
        Self { sha: Cow::Borrowed(SHA), dirty }
    }

    /// Returns `true` if this Git version has uncommitted changes.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}

impl FromStr for GitVersion {
    type Err = core::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (sha, dirty) = match s.strip_suffix(Self::DIRTY) {
            Some(sha) => (sha, true),
            None => (s, false),
        };
        Ok(Self { sha: Cow::Owned(sha.to_owned()), dirty })
    }
}

impl fmt::Display for GitVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dirty = if self.is_dirty() { Self::DIRTY } else { "" };
        write!(f, "{}{dirty}", self.sha)
    }
}

impl PartialEq for GitVersion {
    fn eq(&self, other: &Self) -> bool {
        // If either version is dirty, then we cannot determine that they
        // represent the same codebase, even if the SHA is the same.
        if self.is_dirty() || other.is_dirty() {
            return false;
        }

        self.sha == other.sha
    }
}

impl serde::Serialize for GitVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.dirty {
            // Using `collect_str` *may* avoid allocating a `String` if the
            // serializer is smart enough
            serializer.collect_str(self)
        } else {
            // If we don't need to add the dirty suffix, we can just use
            // `serialize_str` and never make an intermediate `String`
            // allocation.
            serializer.serialize_str(&self.sha)
        }
    }
}
