// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{ClickhouseCli, Clickward};
use slog::Logger;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ServerContext {
    clickward: Clickward,
    clickhouse_cli: ClickhouseCli,
    _log: Logger,
}

impl ServerContext {
    pub fn new(
        clickward: Clickward,
        clickhouse_cli: ClickhouseCli,
        _log: Logger,
    ) -> Self {
        Self { clickward, clickhouse_cli, _log }
    }

    pub fn clickward(&self) -> &Clickward {
        &self.clickward
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }
}

pub struct SingleServerContext {
    clickhouse_cli: ClickhouseCli,
    db_initialized: Arc<Mutex<bool>>,
}

impl SingleServerContext {
    pub fn new(clickhouse_cli: ClickhouseCli) -> Self {
        Self { clickhouse_cli, db_initialized: Arc::new(Mutex::new(false)) }
    }

    pub fn clickhouse_cli(&self) -> &ClickhouseCli {
        &self.clickhouse_cli
    }

    pub fn db_initialized(&self) -> Arc<Mutex<bool>> {
        self.db_initialized.clone()
    }
}
