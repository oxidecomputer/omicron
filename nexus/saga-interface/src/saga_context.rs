use std::{fmt, sync::Arc};

use nexus_auth::{
    authn,
    authz::{self, Authz},
    context::{OpContext, OpKind},
};
use nexus_background_task_interface::BackgroundTasks;
use omicron_common::api::external::Error;
use slog::{Logger, o};

use crate::DataStoreContext;

pub struct SagaContext {
    log: Logger,
    nexus: NexusContext,
}

impl fmt::Debug for SagaContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaContext { (nexus) ... }")
    }
}

impl SagaContext {
    pub fn new(log: Logger, nexus: NexusContext) -> Self {
        Self { log, nexus }
    }

    #[inline]
    pub fn log(&self) -> &Logger {
        &self.log
    }

    #[inline]
    pub fn authz(&self) -> &Arc<Authz> {
        self.nexus.authz()
    }

    #[inline]
    pub fn nexus(&self) -> &NexusContext {
        &self.nexus
    }

    #[inline]
    pub fn datastore(&self) -> &DataStoreContext {
        self.nexus.datastore()
    }
}

pub struct NexusContext {
    nexus: Arc<dyn NexusInterface>,
}

impl NexusContext {
    pub fn new(nexus: Arc<dyn NexusInterface>) -> Self {
        Self { nexus }
    }

    #[inline]
    pub fn authz(&self) -> &Arc<Authz> {
        self.nexus.authz()
    }

    #[inline]
    pub fn datastore(&self) -> &DataStoreContext {
        self.nexus.datastore()
    }

    #[inline]
    pub fn background_tasks(&self) -> &BackgroundTasks {
        self.nexus.background_tasks()
    }

    /// Attempt to delete all of the Dendrite NAT configuration for the
    /// instance identified by `authz_instance`.
    pub async fn instance_delete_dpd_config(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> Result<(), Error> {
        self.nexus.instance_delete_dpd_config(opctx, authz_instance).await
    }
}

#[async_trait::async_trait]
pub trait NexusInterface: Send + Sync + 'static {
    fn authz(&self) -> &Arc<Authz>;
    fn datastore(&self) -> &DataStoreContext;
    fn background_tasks(&self) -> &BackgroundTasks;

    async fn instance_delete_dpd_config(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> Result<(), Error>;
}

pub fn op_context_for_saga_action<T>(
    sagactx: &steno::ActionContext<T>,
    serialized_authn: &authn::saga::Serialized,
) -> OpContext
where
    T: steno::SagaType<ExecContextType = Arc<SagaContext>>,
{
    let osagactx = sagactx.user_data();
    let nexus = osagactx.nexus();
    let datastore = Arc::clone(nexus.datastore().as_storage());

    // TODO-debugging This would be a good place to put the saga name, but
    // we don't have it available here.  This log maybe should come from
    // steno, prepopulated with useful metadata similar to the way
    let log = osagactx.log().new(o!(
        "saga_node" => sagactx.node_label()
    ));

    OpContext::new(
        &log,
        || {
            let authn = Arc::new(serialized_authn.to_authn());
            let authz = authz::Context::new(
                Arc::clone(&authn),
                Arc::clone(&osagactx.authz()),
                datastore,
            );
            Ok::<_, std::convert::Infallible>((authn, authz))
        },
        |metadata| {
            metadata.insert(String::from("saga_node"), sagactx.node_label());
        },
        OpKind::Saga,
    )
    .expect("infallible")
}
