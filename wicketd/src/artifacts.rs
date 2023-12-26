// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::update::ArtifactId;
use std::borrow::Borrow;

mod artifacts_with_plan;
mod error;
mod extracted_artifacts;
mod server;
mod store;
mod update_plan;

pub(crate) use self::extracted_artifacts::ExtractedArtifactDataHandle;
pub(crate) use self::server::WicketdArtifactServer;
pub(crate) use self::store::WicketdArtifactStore;
pub use self::update_plan::UpdatePlan;

/// A pair containing both the ID of an artifact and a handle to its data.
///
/// Note that cloning an `ArtifactIdData` will clone the handle, which has
/// implications on temporary directory cleanup. See
/// [`ExtractedArtifactDataHandle`] for details.
#[derive(Debug, Clone)]
pub(crate) struct ArtifactIdData {
    pub(crate) id: ArtifactId,
    pub(crate) data: ExtractedArtifactDataHandle,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Board(pub(crate) String);

impl Borrow<String> for Board {
    fn borrow(&self) -> &String {
        &self.0
    }
}
