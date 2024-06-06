// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces available to saga actions and undo actions

use crate::app::background::BackgroundTasks;
use crate::app::crucible::Crucible;
use crate::app::disk::Disk;
use crate::app::instance::Instance;
use crate::app::instance_network::InstanceNetwork;
use crate::app::ip_pool::IpPool;
use crate::app::sled::Sled;
use crate::app::vpc::Vpc;
use nexus_db_queries::{authz, db};
use slog::Logger;
use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;

/// Callers that want to execute a saga can clone a context from here,  and
/// modify the logger if desired.
pub static SAGA_CONTEXT: OnceLock<SagaContext> = OnceLock::new();

/// A type accessible to all saga methods
#[derive(Clone)]
pub struct SagaContext {
    log: Logger,
    datastore: Arc<db::DataStore>,
    authz: Arc<authz::Authz>,
    internal_resolver: internal_dns::resolver::Resolver,
    vpc: Vpc,
    disk: Disk,
    crucible: Crucible,
    sled: Sled,
    instance: Instance,
    instance_network: InstanceNetwork,
    ip_pool: IpPool,
    background_tasks: Arc<BackgroundTasks>,
}

impl fmt::Debug for SagaContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaContext { (nexus) ... }")
    }
}

impl SagaContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        log: Logger,
        datastore: Arc<db::DataStore>,
        authz: Arc<authz::Authz>,
        internal_resolver: internal_dns::resolver::Resolver,
        vpc: Vpc,
        disk: Disk,
        crucible: Crucible,
        sled: Sled,
        instance: Instance,
        instance_network: InstanceNetwork,
        ip_pool: IpPool,
        background_tasks: Arc<BackgroundTasks>,
    ) -> SagaContext {
        SagaContext {
            log,
            datastore,
            authz,
            internal_resolver,
            vpc,
            disk,
            crucible,
            sled,
            instance,
            instance_network,
            ip_pool,
            background_tasks,
        }
    }

    /// While all other fields in `SagaContext`  are shallow cloned, we allow
    /// setting a distinct logger per saga so that we get more information.
    pub(crate) fn set_logger(&mut self, log: Logger) {
        self.log = log;
    }

    pub(crate) fn log(&self) -> &Logger {
        &self.log
    }

    pub(crate) fn authz(&self) -> &Arc<authz::Authz> {
        &self.authz
    }

    pub(crate) fn datastore(&self) -> &Arc<db::DataStore> {
        &self.datastore
    }

    pub(crate) fn vpc(&self) -> &Vpc {
        &self.vpc
    }

    pub(crate) fn disk(&self) -> &Disk {
        &self.disk
    }

    pub(crate) fn crucible(&self) -> &Crucible {
        &self.crucible
    }

    pub(crate) fn sled(&self) -> &Sled {
        &self.sled
    }

    pub(crate) fn instance(&self) -> &Instance {
        &self.instance
    }

    pub(crate) fn instance_network(&self) -> &InstanceNetwork {
        &self.instance_network
    }

    pub(crate) fn ip_pool(&self) -> &IpPool {
        &self.ip_pool
    }

    pub(crate) fn background_tasks(&self) -> &Arc<BackgroundTasks> {
        &self.background_tasks
    }

    pub(crate) fn internal_dns_resolver(
        &self,
    ) -> &internal_dns::resolver::Resolver {
        &self.internal_resolver
    }
}
