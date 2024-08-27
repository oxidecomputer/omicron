// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::Clickward;
use slog::Logger;

pub struct ServerContext {
    clickward: Clickward,
    _log: Logger,
}

impl ServerContext {
    pub fn new(clickward: Clickward, _log: Logger) -> Self {
        Self { clickward, _log }
    }

    pub fn clickward(&self) -> &Clickward {
        &self.clickward
    }
}
