// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A very tiny crate for obtaining the current Git SHA of the `omicron`
//! repository.
//!
//! **Warning:** Only depend on this crate in leaf nodes! Depending on it in a
//! core library will cause it and all its dependents to be recompiled every
//! time the underlying Git commit hash changes. See
//! [#10692](https://github.com/oxidecomputer/omicron/issues/10692).
//!
//! For the core type definitions, see [`omicron_git_version_types`].
//!
//! This lives in its own crate, rather than someplace like `omicron-common`,
//! because the `vergen` stuff in the build script will invoke
//! `cargo::rerun_if_changed` and so on any time the Git SHA changes. For
//! incremental builds, like during development, we don't want to invalidate the
//! build cache for *every* crate in the workspace every time the developer
//! makes a commit; we'd rather just invalidate the ones that actually need the
//! Git version.

use omicron_git_version_types::GitVersion;

/// Returns a `GitVersion` representing the state of the repository when
/// this code was compiled.
///
/// ## Known Limitations
///
/// Currently, the determination of whether or not the repository is "dirty"
/// (contains uncommitted changes) in [`current`] relies on a build
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
pub fn current() -> GitVersion {
    let dirty = env!("VERGEN_GIT_DIRTY") == "true";
    GitVersion::new_static(env!("VERGEN_GIT_SHA"), dirty)
}
