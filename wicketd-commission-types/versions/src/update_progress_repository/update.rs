// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::IdOrdMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::update::RepositoryDescription;
use crate::v4;
use crate::v4::update::SpUpdateProgress;

/// The response to a request for update progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GetUpdateProgressResponse {
    /// Information about the uploaded repository.
    pub repository: RepositoryDescription,

    /// The service processors that have update state, and their progress.
    ///
    /// A service processor appears here only once an update has been started
    /// for it, and disappears when any of the following occur:
    ///
    /// * The update state is cleared.
    ///
    /// * A TUF repository is uploaded.
    pub sps: IdOrdMap<SpUpdateProgress>,
}

impl From<GetUpdateProgressResponse> for v4::update::GetUpdateProgressResponse {
    fn from(new: GetUpdateProgressResponse) -> Self {
        let GetUpdateProgressResponse { repository: _, sps } = new;
        Self { sps }
    }
}
