// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Describes the states of network-attached storage.

use uuid::Uuid;

/// Action to be taken on behalf of state transition.
#[derive(Clone, Debug)]
pub enum Action {
    Attach(Uuid),
    Detach(Uuid),
    Destroy,
}
