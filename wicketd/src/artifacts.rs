// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use hyper::body::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use slog::Logger;

/// Path parameters for Silo requests
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
pub(crate) struct ArtifactId {
    /// The artifact's name.
    pub(crate) name: String,

    /// The version of the artifact.
    pub(crate) version: String,
}

/// The artifact store, used to cache artifacts.
pub struct ArtifactStore {
    log: Logger,
    // TODO: implement this
}

impl ArtifactStore {
    pub(crate) fn new(log: &Logger) -> Self {
        let log = log.new(slog::o!("component" => "wicketd artifact store"));
        Self { log }
    }

    pub(crate) fn get_artifact(&self, id: &ArtifactId) -> Option<Bytes> {
        slog::debug!(self.log, "Artifact requested (this is a stub implementation which always 404s): {:?}", id);
        // TODO: implement this
        None
    }
}
