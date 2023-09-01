// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::executor::HostExecutor;
use crate::libc::RealLibc;
use crate::swapctl::RealSwapctl;
use helios_fusion::interfaces::{libc::Libc, swapctl::Swapctl};
use helios_fusion::{Executor, Host, HostSystem};
use slog::Logger;
use std::sync::Arc;

struct RealHost {
    executor: Arc<HostExecutor>,

    libc: RealLibc,
    swapctl: RealSwapctl,
}

impl RealHost {
    pub fn new(log: Logger) -> Arc<Self> {
        Arc::new(Self {
            executor: HostExecutor::new(log),
            libc: Default::default(),
            swapctl: Default::default(),
        })
    }

    pub fn as_host(self: Arc<Self>) -> HostSystem {
        self
    }
}

impl Host for RealHost {
    fn executor(&self) -> &dyn Executor {
        &*self.executor
    }

    fn libc(&self) -> &dyn Libc {
        &self.libc
    }

    fn swapctl(&self) -> &dyn Swapctl {
        &self.swapctl
    }
}
