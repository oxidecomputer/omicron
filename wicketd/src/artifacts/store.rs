// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use semver::Version;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tufaceous::ArtifactHandle;
use tufaceous::Repository;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactId;

/// The artifact store for wicketd.
///
/// This can be cheaply cloned, and is intended to be shared across the parts of artifactd that
/// upload artifacts and the parts that fetch them.
#[derive(Clone, Debug)]
pub struct WicketdArtifactStore {
    log: Logger,
    // NOTE: this is a `std::sync::Mutex` rather than a `tokio::sync::Mutex`
    // because the critical sections are extremely small.
    inner: Arc<Mutex<Option<Inner>>>,
}

#[derive(Debug)]
struct Inner {
    repo: Arc<Repository>,
    by_hash: BTreeMap<ArtifactHash, ArtifactHandle>,
}

impl WicketdArtifactStore {
    pub(crate) fn new(log: &Logger) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact store"));
        Self { log, inner: Default::default() }
    }

    pub(crate) fn set_repository(&self, repo: Arc<Repository>) {
        slog::debug!(self.log, "setting repository");
        let by_hash = repo
            .handles()
            .map(|handle| (handle.artifact().hash, handle))
            .collect();
        self.replace(Inner { by_hash, repo });
    }

    pub(crate) fn system_version_and_artifact_ids(
        &self,
    ) -> Option<(Version, Vec<ArtifactId>)> {
        let guard = self.inner.lock().unwrap();
        let inner = guard.as_ref()?;
        let system_version = inner.repo.system_version().clone();
        let artifact_ids = inner
            .repo
            .artifacts()
            .iter()
            .map(|artifact| artifact.id())
            .collect();
        Some((system_version, artifact_ids))
    }

    /// Obtain the current repository.
    ///
    /// Exposed as `pub` for testing.
    pub fn current_repository(&self) -> Option<Arc<Repository>> {
        Some(Arc::clone(&self.inner.lock().unwrap().as_ref()?.repo))
    }

    // ---
    // Helper methods
    // ---

    pub(super) fn get_installinator_artifact(
        &self,
        hash: ArtifactHash,
    ) -> Option<ArtifactHandle> {
        self.inner.lock().unwrap().as_ref()?.by_hash.get(&hash).cloned()
    }

    // `pub` to allow use in integration tests.
    pub fn contains_installinator_artifact(&self, hash: ArtifactHash) -> bool {
        self.get_installinator_artifact(hash).is_some()
    }

    /// Replaces the inner value.
    fn replace(&self, inner: Inner) {
        let mut guard = self.inner.lock().unwrap();
        *guard = Some(inner);
    }
}
