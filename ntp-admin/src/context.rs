// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use slog::Logger;

pub struct ServerContext {
    log: Logger,
}

impl ServerContext {
    pub fn new(log: Logger) -> Self {
        Self { log }
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }
}
