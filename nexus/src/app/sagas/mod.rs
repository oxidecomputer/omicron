// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga actions, undo actions, and saga constructors used in Nexus.

// NOTE: We want to be careful about what interfaces we expose to saga actions.
// In the future, we expect to mock these out for comprehensive testing of
// correctness, idempotence, etc.  The more constrained this interface is, the
// easier it will be to test, version, and update in deployed systems.

use crate::authn;
use crate::saga_interface::SagaContext;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::SagaType;
use thiserror::Error;
use uuid::Uuid;

pub mod disk_create;
// pub mod disk_delete; // XXX-dap
// pub mod instance_create;
// pub mod instance_migrate;

#[derive(Debug)]
pub struct NexusSagaType;
impl steno::SagaType for NexusSagaType {
    type ExecContextType = Arc<SagaContext>;
}

pub type ActionRegistry = steno::ActionRegistry<NexusSagaType>;
pub type NexusAction = Arc<dyn steno::Action<NexusSagaType>>;
pub type NexusActionContext = steno::ActionContext<NexusSagaType>;

// XXX this should be the internal version of the trait
pub trait NexusSaga {
    const NAME: &'static str;

    type Params: serde::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug;

    fn register_actions(registry: &mut ActionRegistry);

    fn make_saga_dag(
        params: &Self::Params,
        builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError>;
}

#[derive(Debug, Error)]
pub enum SagaInitError {
    #[error("internal error building saga graph: {0:#}")]
    DagBuildError(steno::DagBuilderError),
    #[error("failed to serialize saga parameters: {0:#}")]
    SerializeParamsError(serde_json::Error),
}

impl From<steno::DagBuilderError> for SagaInitError {
    fn from(error: steno::DagBuilderError) -> Self {
        SagaInitError::DagBuildError(error)
    }
}

impl From<serde_json::Error> for SagaInitError {
    fn from(error: serde_json::Error) -> Self {
        SagaInitError::SerializeParamsError(error)
    }
}

impl From<SagaInitError> for omicron_common::api::external::Error {
    fn from(error: SagaInitError) -> Self {
        // All of these errors reflect things that shouldn't be possible.
        // They're basically bugs.
        omicron_common::api::external::Error::internal_error(&format!(
            "creating saga: {:#}",
            error
        ))
    }
}

lazy_static! {
    pub (super) static ref ACTION_GENERATE_ID: NexusAction =
        new_action_noop_undo("common.uuid_generate", saga_generate_uuid);

    pub static ref ACTION_REGISTRY: Arc<ActionRegistry> =
        Arc::new(make_action_registry());

    // XXX-dap replace with all NexusSaga impls
    // pub static ref ALL_TEMPLATES: BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>> = todo!();
}

pub fn new_saga<N: NexusSaga>(
    nexus_saga: &N,
    params: N::Params,
) -> Result<steno::SagaDag, SagaInitError> {
    let builder = steno::DagBuilder::new(steno::SagaName::new(N::NAME));
    let dag = N::make_saga_dag(&params, builder)?;
    let params = serde_json::to_value(&params)?;
    Ok(steno::SagaDag::new(dag, params))
}

fn make_action_registry() -> ActionRegistry {
    let mut registry = steno::ActionRegistry::new();
    registry.register(Arc::clone(&*ACTION_GENERATE_ID));

    // XXX-dap register each of the NexusSaga impls

    registry
}

// fn all_templates(
// ) -> BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>> {
//     vec![
//         (
//             instance_create::SAGA_NAME,
//             Arc::clone(&instance_create::SAGA_TEMPLATE)
//                 as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
//         ),
//         (
//             instance_migrate::SAGA_NAME,
//             Arc::clone(&instance_migrate::SAGA_TEMPLATE)
//                 as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
//         ),
//         (
//             disk_create::SAGA_NAME,
//             Arc::clone(&disk_create::SAGA_TEMPLATE)
//                 as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
//         ),
//         (
//             disk_delete::SAGA_NAME,
//             Arc::clone(&disk_delete::SAGA_TEMPLATE)
//                 as Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>,
//         ),
//     ]
//     .into_iter()
//     .collect()
// }

pub(super) async fn saga_generate_uuid<UserType: SagaType>(
    _: ActionContext<UserType>,
) -> Result<Uuid, ActionError> {
    Ok(Uuid::new_v4())
}

/// A trait for sagas with serialized authentication information.
///
/// This allows sharing code in different sagas which rely on some
/// authentication information, for example when doing database lookups.
pub(super) trait AuthenticatedSagaParams {
    fn serialized_authn(&self) -> &authn::saga::Serialized;
}

/// A helper macro which implements the `AuthenticatedSagaParams` trait for saga
/// parameter types which have a field called `serialized_authn`.
macro_rules! impl_authenticated_saga_params {
    ($typ:ty) => {
        impl crate::app::sagas::AuthenticatedSagaParams
            for <$typ as SagaType>::SagaParamsType
        {
            fn serialized_authn(&self) -> &authn::saga::Serialized {
                &self.serialized_authn
            }
        }
    };
}

pub(super) use impl_authenticated_saga_params;
