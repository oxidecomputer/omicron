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
pub mod instance_migrate;
pub mod snapshot_create;
pub mod volume_delete;

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
    <instance_migrate::SagaInstanceMigrate as NexusSaga>::register_actions(
        &mut registry,
    );
    <snapshot_create::SagaSnapshotCreate as NexusSaga>::register_actions(
        &mut registry,
    );
    <volume_delete::SagaVolumeDelete as NexusSaga>::register_actions(
        &mut registry,
    );

    registry
}

pub(super) async fn saga_generate_uuid<UserType: SagaType>(
    _: ActionContext<UserType>,
) -> Result<Uuid, ActionError> {
    Ok(Uuid::new_v4())
}

// Arbitrary limit on concurrency, for operations issued on multiple regions
// within a disk at the same time.
const MAX_CONCURRENT_REGION_REQUESTS: usize = 3;
