// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga actions, undo actions, and saga constructors used in Nexus.

// NOTE: We want to be careful about what interfaces we expose to saga actions.
// In the future, we expect to mock these out for comprehensive testing of
// correctness, idempotence, etc.  The more constrained this interface is, the
// easier it will be to test, version, and update in deployed systems.

use crate::saga_interface::SagaContext;
use std::sync::Arc;
use std::sync::LazyLock;
use steno::ActionContext;
use steno::ActionError;
use steno::DagBuilder;
use steno::SagaDag;
use steno::SagaName;
use steno::SagaType;
use steno::new_action_noop_undo;
use thiserror::Error;
use uuid::Uuid;

pub mod demo;
pub mod disk_create;
pub mod disk_delete;
pub mod finalize_disk;
pub mod image_create;
pub mod image_delete;
pub(crate) mod instance_common;
pub mod instance_create;
pub mod instance_delete;
pub mod instance_ip_attach;
pub mod instance_ip_detach;
pub mod instance_migrate;
pub mod instance_start;
pub mod instance_update;
pub mod multicast_group_dpd_ensure;
pub mod project_create;
pub mod region_replacement_drive;
pub mod region_replacement_finish;
pub mod region_replacement_start;
pub mod region_snapshot_replacement_finish;
pub mod region_snapshot_replacement_garbage_collect;
pub mod region_snapshot_replacement_start;
pub mod region_snapshot_replacement_step;
pub mod region_snapshot_replacement_step_garbage_collect;
pub mod snapshot_create;
pub mod snapshot_delete;
pub mod test_saga;
pub mod volume_delete;
pub mod volume_remove_rop;
pub mod vpc_create;
pub mod vpc_subnet_create;
pub mod vpc_subnet_delete;
pub mod vpc_subnet_update;

pub mod common_storage;

#[cfg(test)]
pub(crate) mod test_helpers;

#[derive(Debug)]
pub struct NexusSagaType;
impl steno::SagaType for NexusSagaType {
    type ExecContextType = Arc<SagaContext>;
}

pub(crate) type ActionRegistry = steno::ActionRegistry<NexusSagaType>;
pub(crate) type NexusAction = Arc<dyn steno::Action<NexusSagaType>>;
pub(crate) type NexusActionContext = steno::ActionContext<NexusSagaType>;

pub(crate) trait NexusSaga {
    const NAME: &'static str;

    type Params: serde::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug;

    fn register_actions(registry: &mut ActionRegistry);

    fn make_saga_dag(
        params: &Self::Params,
        builder: steno::DagBuilder,
    ) -> Result<steno::Dag, SagaInitError>;

    fn prepare(
        params: &Self::Params,
    ) -> Result<SagaDag, omicron_common::api::external::Error> {
        let builder = DagBuilder::new(SagaName::new(Self::NAME));
        let dag = Self::make_saga_dag(&params, builder)?;
        let params = serde_json::to_value(&params).map_err(|e| {
            SagaInitError::SerializeError(format!("saga params: {params:?}"), e)
        })?;
        Ok(SagaDag::new(dag, params))
    }
}

#[derive(Debug, Error)]
pub enum SagaInitError {
    #[error("internal error building saga graph: {0:#}")]
    DagBuildError(steno::DagBuilderError),

    #[error("failed to serialize {0:?}: {1:#}")]
    SerializeError(String, serde_json::Error),

    #[error("invalid parameter: {0}")]
    InvalidParameter(String),
}

impl From<steno::DagBuilderError> for SagaInitError {
    fn from(error: steno::DagBuilderError) -> Self {
        SagaInitError::DagBuildError(error)
    }
}

impl From<SagaInitError> for omicron_common::api::external::Error {
    fn from(error: SagaInitError) -> Self {
        match error {
            SagaInitError::DagBuildError(_)
            | SagaInitError::SerializeError(_, _) => {
                // All of these errors reflect things that shouldn't be possible.
                // They're basically bugs.
                omicron_common::api::external::Error::internal_error(&format!(
                    "creating saga: {:#}",
                    error
                ))
            }

            SagaInitError::InvalidParameter(s) => {
                omicron_common::api::external::Error::invalid_request(&s)
            }
        }
    }
}

pub(super) static ACTION_GENERATE_ID: LazyLock<NexusAction> =
    LazyLock::new(|| {
        new_action_noop_undo("common.uuid_generate", saga_generate_uuid)
    });
pub(crate) static ACTION_REGISTRY: LazyLock<Arc<ActionRegistry>> =
    LazyLock::new(|| Arc::new(make_action_registry()));

macro_rules! register_actions {
    ( $registry:ident, $( $saga: ty ),* ) => {
        $(
            <$saga as NexusSaga>::register_actions(&mut $registry);
        )*
    };
}

fn make_action_registry() -> ActionRegistry {
    let mut registry = steno::ActionRegistry::new();
    registry.register(Arc::clone(&*ACTION_GENERATE_ID));

    register_actions! [
        registry,
        demo::SagaDemo,
        disk_create::SagaDiskCreate,
        disk_delete::SagaDiskDelete,
        finalize_disk::SagaFinalizeDisk,
        instance_create::SagaInstanceCreate,
        instance_delete::SagaInstanceDelete,
        instance_ip_attach::SagaInstanceIpAttach,
        instance_ip_detach::SagaInstanceIpDetach,
        instance_migrate::SagaInstanceMigrate,
        instance_start::SagaInstanceStart,
        instance_update::SagaInstanceUpdate,
        project_create::SagaProjectCreate,
        snapshot_create::SagaSnapshotCreate,
        snapshot_delete::SagaSnapshotDelete,
        volume_delete::SagaVolumeDelete,
        volume_remove_rop::SagaVolumeRemoveROP,
        vpc_create::SagaVpcCreate,
        vpc_subnet_create::SagaVpcSubnetCreate,
        vpc_subnet_delete::SagaVpcSubnetDelete,
        vpc_subnet_update::SagaVpcSubnetUpdate,
        image_delete::SagaImageDelete,
        region_replacement_start::SagaRegionReplacementStart,
        region_replacement_drive::SagaRegionReplacementDrive,
        region_replacement_finish::SagaRegionReplacementFinish,
        region_snapshot_replacement_start::SagaRegionSnapshotReplacementStart,
        region_snapshot_replacement_garbage_collect::SagaRegionSnapshotReplacementGarbageCollect,
        region_snapshot_replacement_step::SagaRegionSnapshotReplacementStep,
        region_snapshot_replacement_step_garbage_collect::SagaRegionSnapshotReplacementStepGarbageCollect,
        region_snapshot_replacement_finish::SagaRegionSnapshotReplacementFinish,
        image_create::SagaImageCreate,
        multicast_group_dpd_ensure::SagaMulticastGroupDpdEnsure
    ];

    #[cfg(test)]
    <test_saga::SagaTest as NexusSaga>::register_actions(&mut registry);

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
/// This macro aims to reduce this boilerplate, by requiring only the following:
/// - The name of the saga
/// - The name of each action
/// - The output of each action
/// - The "forward" action function
/// - (Optional) The "undo" action function
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
        static $node: ::std::sync::LazyLock<crate::app::sagas::NexusAction> =
            ::std::sync::LazyLock::new(|| {
                ::steno::ActionFunc::new_action(
                    crate::app::sagas::__action_name!($saga, $node), $a, $u,
                )
            });
        crate::app::sagas::__emit_action!($node, $out);
        declare_saga_actions!(S = $saga $($nodes,)* $node <> $($tail)*);
    };
    // Same as the prior match, but without the undo action.
    (S = $saga:ident $($nodes:ident),* <> $node:ident -> $out:literal { + $a:ident } $($tail:tt)*) => {
        static $node: ::std::sync::LazyLock<crate::app::sagas::NexusAction> =
            ::std::sync::LazyLock::new(|| {
                ::steno::new_action_noop_undo(
                    crate::app::sagas::__action_name!($saga, $node), $a,
                )
            });
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
