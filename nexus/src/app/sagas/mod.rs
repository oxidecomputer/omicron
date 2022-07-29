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
use uuid::Uuid;

pub mod disk_create;
// pub mod disk_delete; // XXX-dap
// pub mod instance_create;
// pub mod instance_migrate;

pub type ActionRegistry = steno::ActionRegistry<NexusSagaType>;

#[derive(Debug)]
pub struct NexusSagaType;
impl steno::SagaType for NexusSagaType {
    type ExecContextType = Arc<SagaContext>;
}

pub(super) trait NexusSaga {
    const NAME: &'static str;

    type Params: serde::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug;

    fn register_actions(
        registry: &mut ActionRegistry,
    ) -> Result<(), anyhow::Error>;

    fn make_saga(
        params: &Self::Params,
    ) -> Result<steno::SagaDag, anyhow::Error>;
}

lazy_static! {
    pub (super) static ref ACTION_GENERATE_ID:
        Arc<dyn steno::Action<NexusSagaType>> =
        new_action_noop_undo("generate-uuid", saga_generate_uuid);

    pub static ref ACTION_REGISTRY: ActionRegistry = make_action_registry();
    // XXX-dap replace with all NexusSaga impls
    // pub static ref ALL_TEMPLATES: BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<Arc<SagaContext>>>> = todo!();
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
