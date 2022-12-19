// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga actions, undo actions, and saga constructors used in Nexus.

// NOTE: We want to be careful about what interfaces we expose to saga actions.
// In the future, we expect to mock these out for comprehensive testing of
// correctness, idempotence, etc.  The more constrained this interface is, the
// easier it will be to test, version, and update in deployed systems.

use crate::saga_interface::SagaContext;
use lazy_static::lazy_static;
use std::sync::Arc;
use steno::new_action_noop_undo;
use steno::ActionContext;
use steno::ActionError;
use steno::SagaType;
use thiserror::Error;
use uuid::Uuid;

pub mod disk_create;
pub mod disk_delete;
pub mod instance_create;
pub mod instance_delete;
pub mod instance_migrate;
pub mod project_create;
pub mod snapshot_create;
pub mod volume_delete;
pub mod volume_remove_rop;
pub mod vpc_create;

pub mod common_storage;

#[derive(Debug)]
pub struct NexusSagaType;
impl steno::SagaType for NexusSagaType {
    type ExecContextType = Arc<SagaContext>;
}

pub type ActionRegistry = steno::ActionRegistry<NexusSagaType>;
pub type NexusAction = Arc<dyn steno::Action<NexusSagaType>>;
pub type NexusActionContext = steno::ActionContext<NexusSagaType>;

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
    #[error("failed to serialize {0:?}: {1:#}")]
    SerializeError(String, serde_json::Error),
}

impl From<steno::DagBuilderError> for SagaInitError {
    fn from(error: steno::DagBuilderError) -> Self {
        SagaInitError::DagBuildError(error)
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
    pub(super) static ref ACTION_GENERATE_ID: NexusAction =
        new_action_noop_undo("common.uuid_generate", saga_generate_uuid);
    pub static ref ACTION_REGISTRY: Arc<ActionRegistry> =
        Arc::new(make_action_registry());
}

fn make_action_registry() -> ActionRegistry {
    let mut registry = steno::ActionRegistry::new();
    registry.register(Arc::clone(&*ACTION_GENERATE_ID));

    <disk_create::SagaDiskCreate as NexusSaga>::register_actions(&mut registry);
    <disk_delete::SagaDiskDelete as NexusSaga>::register_actions(&mut registry);
    <instance_create::SagaInstanceCreate as NexusSaga>::register_actions(
        &mut registry,
    );
    <instance_delete::SagaInstanceDelete as NexusSaga>::register_actions(
        &mut registry,
    );
    <instance_migrate::SagaInstanceMigrate as NexusSaga>::register_actions(
        &mut registry,
    );
    <project_create::SagaProjectCreate as NexusSaga>::register_actions(
        &mut registry,
    );
    <snapshot_create::SagaSnapshotCreate as NexusSaga>::register_actions(
        &mut registry,
    );
    <volume_delete::SagaVolumeDelete as NexusSaga>::register_actions(
        &mut registry,
    );
    <volume_remove_rop::SagaVolumeRemoveROP as NexusSaga>::register_actions(
        &mut registry,
    );
    <vpc_create::SagaVpcCreate as NexusSaga>::register_actions(&mut registry);

    registry
}

pub(super) async fn saga_generate_uuid<UserType: SagaType>(
    _: ActionContext<UserType>,
) -> Result<Uuid, ActionError> {
    Ok(Uuid::new_v4())
}

macro_rules! __stringify_ident {
    ($i:ident) => {
        stringify!($i)
    };
}

macro_rules! __emit_action {
    ($node:ident, $output:literal) => {
        paste::paste! {
            #[allow(dead_code)]
            fn [<$node:lower _action>]() -> ::steno::Node {
                ::steno::Node::action(
                    $output,
                    crate::app::sagas::__stringify_ident!([<$node:camel>]),
                    $node.as_ref(),
                )
            }
        }
    };
}

macro_rules! __action_name {
    ($saga:ident, $node:ident) => {
        paste::paste! {
            concat!(
                stringify!($saga),
                ".",
                crate::app::sagas::__stringify_ident!([<$node:lower>]),
            )
        }
    };
}

/// A macro intended to reduce boilerplate when writing saga actions.
///
/// For this input:
///
/// ```ignore
/// declare_saga_actions! {
///     my_saga;
///     SAGA_NODE1 -> "output1" {
///         + do1
///         - undo1
///     }
///     SAGA_NODE2 -> "output2" {
///         + do2
///     }
/// }
/// ```
///
/// We generate the following:
/// - For `SAGA_NODE1`:
///     - A `NexusAction` labeled "my_saga.saga_node1" (containing "do1" and "undo1").
///     - `fn saga_node1_action() -> steno::Node` referencing this node, with an
///     output named "output1".
/// - For `SAGA_NODE2`:
///     - A `NexusAction` labeled "my_saga.saga_node2" (containing "do2").
///     - `fn saga_node2_action() -> steno::Node` referencing this node, with an
///     output named "output2".
/// - For `my_saga`:
///     - `fn my_saga_register_actions(...)`, which can be called to implement
///     `NexusSaga::register_actions`.
macro_rules! declare_saga_actions {
    // The entrypoint to the macro.
    // We expect the input to be of the form:
    //
    //  saga-name;
    ($saga:ident; $($tail:tt)*) => {
        declare_saga_actions!(S = $saga <> $($tail)*);
    };
    // Subsequent lines of the saga action declaration.
    // These take the form:
    //
    //  ACTION_NAME -> "output" {
    //      + action
    //      - undo_action
    //  }
    //
    // However, we also want to propagate the Saga structure and collection of
    // all node names, so this is *actually* parsed with a hidden prefix:
    //
    //  S = SagaName <old nodes> <> ...
    //
    // Basically, everything to the left of "<>" is just us propagating state
    // through the macro, and everything to the right of it is user input.
    (S = $saga:ident $($nodes:ident),* <> $node:ident -> $out:literal { + $a:ident - $u:ident } $($tail:tt)*) => {
        lazy_static::lazy_static! {
            static ref $node: crate::app::sagas::NexusAction = ::steno::ActionFunc::new_action(
                crate::app::sagas::__action_name!($saga, $node), $a, $u,
            );
        }
        crate::app::sagas::__emit_action!($node, $out);
        declare_saga_actions!(S = $saga $($nodes,)* $node <> $($tail)*);
    };
    // Same as the prior match, but without the undo action.
    (S = $saga:ident $($nodes:ident),* <> $node:ident -> $out:literal { + $a:ident } $($tail:tt)*) => {
        lazy_static::lazy_static! {
            static ref $node: crate::app::sagas::NexusAction = ::steno::new_action_noop_undo(
                crate::app::sagas::__action_name!($saga, $node), $a,
            );
        }
        crate::app::sagas::__emit_action!($node, $out);
        declare_saga_actions!(S = $saga $($nodes,)* $node <> $($tail)*);
    };
    // The end of the macro, which registers all previous generated saga nodes.
    //
    // We generate a new function, rather than implementing
    // "NexusSaga::register_actions", because traits cannot be partially
    // implemented, and "make_saga_dag" is not being generated through this
    // macro.
    (S = $saga:ident $($nodes:ident),* <>) => {
        paste::paste! {
            fn [<$saga _register_actions>](registry: &mut crate::app::sagas::ActionRegistry) {
                $(
                    registry.register(::std::sync::Arc::clone(&* $nodes ));
                )*
            }
        }
    };
}

pub(crate) use __action_name;
pub(crate) use __emit_action;
pub(crate) use __stringify_ident;
pub(crate) use declare_saga_actions;
