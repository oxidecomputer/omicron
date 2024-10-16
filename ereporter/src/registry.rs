// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::buffer;
use crate::server;
use crate::Reporter;
use omicron_common::FileKv;
use slog::debug;
use slog::error;
use slog::Drain;
use slog::Logger;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::sync::watch;
use uuid::Uuid;

// Our public interface depends directly or indirectly on these types; we
// export them so that consumers need not depend on dropshot themselves and
// to simplify how we stage incompatible upgrades.
pub use dropshot::ConfigLogging;
pub use dropshot::ConfigLoggingIfExists;
pub use dropshot::ConfigLoggingLevel;

#[derive(Clone, Debug)]
pub struct ReporterRegistry(pub(crate) Arc<RegistryInner>);

/// Either configuration for building a logger, or an actual logger already
/// instantiated.
///
/// This can be used to start a [`Server`] with a new logger or a child of a
/// parent logger if desired.
#[derive(Debug, Clone)]
pub enum LogConfig {
    /// Configuration for building a new logger.
    Config(ConfigLogging),
    /// An explicit logger to use.
    Logger(slog::Logger),
}

#[derive(Debug)]
pub(crate) struct RegistryInner {
    reporters: RwLock<HashMap<Uuid, buffer::Handle>>,
    buffer_capacity: usize,
    // Used for registering reporters as they are inserted.
    pub(crate) log: slog::Logger,
    pub(crate) server_tx: watch::Sender<Option<server::State>>,
    server_state: watch::Receiver<Option<server::State>>,
}

impl ReporterRegistry {
    pub fn new(log: LogConfig, buffer_capacity: usize) -> anyhow::Result<Self> {
        let log = {
            let base_logger = match log {
                LogConfig::Config(conf) => conf.to_logger("ereporter")?,
                LogConfig::Logger(log) => log.clone(),
            };
            let (drain, registration) = slog_dtrace::with_drain(base_logger);
            let log = Logger::root(drain.fuse(), slog::o!(FileKv));
            if let slog_dtrace::ProbeRegistration::Failed(e) = registration {
                error!(log, "failed to register DTrace probes: {e}",);
            } else {
                debug!(log, "registered DTrace probes");
            }
            log
        };

        anyhow::ensure!(
            buffer_capacity > 0,
            "a 0-capacity ereport buffer is nonsensical"
        );

        let (server_tx, server_state) = tokio::sync::watch::channel(None);
        Ok(Self(Arc::new(RegistryInner {
            reporters: RwLock::new(HashMap::new()),
            buffer_capacity,
            log,
            server_tx,
            server_state,
        })))
    }

    pub fn register_reporter(&self, id: Uuid) -> Reporter {
        let mut reporters = self.0.reporters.write().unwrap();
        let ereports = reporters
            .entry(id)
            .or_insert_with(|| {
                buffer::BufferWorker::spawn(
                    id,
                    &self.0.log,
                    self.0.buffer_capacity,
                    self.0.server_state.clone(),
                )
            })
            .ereports
            .clone();
        Reporter(ereports)
    }

    pub fn register_proxy(&self, _id: Uuid, _proxy: ()) {
        unimplemented!(
            "TODO(eliza): this will eventually take a trait representing how \
             to proxy requests to a non-local reporter, for use by e.g. MGS",
        );
    }

    pub(crate) fn get_worker(
        &self,
        id: &Uuid,
    ) -> Option<mpsc::Sender<buffer::ServerReq>> {
        self.0
            .reporters
            .read()
            .unwrap()
            .get(id)
            .map(|handle| &handle.requests)
            .cloned()
    }
}

impl Drop for RegistryInner {
    // Abbort all spawned buffer tasks when the reporter registry is dropped.
    fn drop(&mut self) {
        let reporters = self
            .reporters
            .get_mut()
            // if the lock is poisoned, don't panic, because we are in a `Drop`
            // impl and that would cause a double-panic.
            .unwrap_or_else(|e| e.into_inner());
        for (_, reporter) in reporters.drain() {
            reporter.task.abort();
        }
    }
}
