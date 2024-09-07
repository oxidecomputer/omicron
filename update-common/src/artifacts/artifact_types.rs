// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! General types for artifacts that don't quite fit into the other modules.

use std::borrow::Borrow;

use omicron_common::update::ArtifactId;

use super::ExtractedArtifactDataHandle;

/// A pair containing both the ID of an artifact and a handle to its data.
///
/// Note that cloning an `ArtifactIdData` will clone the handle, which has
/// implications on temporary directory cleanup. See
/// [`ExtractedArtifactDataHandle`] for details.
#[derive(Debug, Clone)]
pub struct ArtifactIdData {
    pub id: ArtifactId,
    pub data: ExtractedArtifactDataHandle,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Board(pub String);

impl Borrow<String> for Board {
    fn borrow(&self) -> &String {
        &self.0
    }
}
