// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! User provided dropshot server context

use crate::store::ArtifactStore;

pub struct ServerContext {
    pub(crate) artifact_store: ArtifactStore,
}
