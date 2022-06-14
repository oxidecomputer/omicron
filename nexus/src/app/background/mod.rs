// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background tasks managed by Nexus.

mod services;

use crate::app::Nexus;
use std::sync::Arc;
use tokio::task::{spawn, JoinHandle};

/// Management structure which encapsulates periodically-executing background
/// tasks.
pub struct TaskRunner {
    _handle: JoinHandle<()>,
}

impl TaskRunner {
    pub fn new(nexus: Arc<Nexus>) -> Self {
        let handle = spawn(async move {
            let log = nexus.log.new(o!("component" => "BackgroundTaskRunner"));
            let service_balancer = services::ServiceBalancer::new(log.clone(), nexus.clone());

            loop {
                // TODO: We may want triggers to exist here, to invoke this task
                // more frequently (e.g., on Sled failure).
                let opctx = nexus.opctx_for_background();
                if let Err(e) = service_balancer.balance_services(&opctx).await {
                    warn!(log, "Failed to balance services: {:?}", e);
                }

                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });
        Self {
            _handle: handle,
        }
    }
}
