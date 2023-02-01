// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::fmt;

use async_trait::async_trait;
use hyper::Body;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use slog::Logger;

/// Represents a way to fetch artifacts.
#[async_trait]
pub trait ArtifactGetter: fmt::Debug + Send + Sync + 'static {
    /// Gets an artifact, returning it as a [`Body`].
    async fn get(&self, id: &UpdateArtifactId) -> Option<Body>;
}

/// The artifact store -- a simple wrapper around a dynamic [`ArtifactGetter`] that does some basic
/// logging.
#[derive(Debug)]
pub(crate) struct ArtifactStore {
    log: Logger,
    getter: Box<dyn ArtifactGetter>,
    // TODO: implement this
}

impl ArtifactStore {
    pub(crate) fn new<Getter: ArtifactGetter>(
        getter: Getter,
        log: &Logger,
    ) -> Self {
        let log = log.new(slog::o!("component" => "artifact store"));
        Self { log, getter: Box::new(getter) }
    }

    pub(crate) async fn get_artifact(
        &self,
        id: &UpdateArtifactId,
    ) -> Option<Body> {
        slog::debug!(self.log, "Artifact requested: {:?}", id);
        self.getter.get(id).await
    }
}
