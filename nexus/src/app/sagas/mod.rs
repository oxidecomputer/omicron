// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga actions, undo actions, and saga constructors used in Nexus.

// NOTE: We want to be careful about what interfaces we expose to saga actions.
// In the future, we expect to mock these out for comprehensive testing of
// correctness, idempotence, etc.  The more constrained this interface is, the
// easier it will be to test, version, and update in deployed systems.

use crate::db;
use crate::db::identity::Asset;
use crate::saga_interface::SagaContext;
use anyhow::anyhow;
use crucible_agent_client::{
    types::{CreateRegion, RegionId, State as RegionState},
    Client as CrucibleAgentClient,
};
use futures::StreamExt;
use lazy_static::lazy_static;
use omicron_common::api::external::Error;
use omicron_common::backoff::{self, BackoffError};
use slog::Logger;
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

/// Call out to Crucible agent and perform region creation.
pub async fn ensure_region_in_dataset(
    log: &Logger,
    dataset: &db::model::Dataset,
    region: &db::model::Region,
) -> Result<crucible_agent_client::types::Region, Error> {
    let url = format!("http://{}", dataset.address());
    let client = CrucibleAgentClient::new(&url);

    let region_request = CreateRegion {
        block_size: region.block_size().to_bytes(),
        extent_count: region.extent_count().try_into().unwrap(),
        extent_size: region.blocks_per_extent().try_into().unwrap(),
        // TODO: Can we avoid casting from UUID to string?
        // NOTE: This'll require updating the crucible agent client.
        id: RegionId(region.id().to_string()),
        encrypted: region.encrypted(),
        cert_pem: None,
        key_pem: None,
        root_pem: None,
    };

    let create_region = || async {
        let region = client
            .region_create(&region_request)
            .await
            .map_err(|e| BackoffError::Permanent(e.into()))?;
        match region.state {
            RegionState::Requested => Err(BackoffError::transient(anyhow!(
                "Region creation in progress"
            ))),
            RegionState::Created => Ok(region),
            _ => Err(BackoffError::Permanent(anyhow!(
                "Failed to create region, unexpected state: {:?}",
                region.state
            ))),
        }
    };

    let log_create_failure = |_, delay| {
        warn!(
            log,
            "Region requested, not yet created. Retrying in {:?}", delay
        );
    };

    let region = backoff::retry_notify(
        backoff::internal_service_policy(),
        create_region,
        log_create_failure,
    )
    .await
    .map_err(|e| Error::internal_error(&e.to_string()))?;

    Ok(region.into_inner())
}

pub async fn ensure_all_datasets_and_regions(
    log: &Logger,
    datasets_and_regions: Vec<(db::model::Dataset, db::model::Region)>,
) -> Result<
    Vec<(db::model::Dataset, crucible_agent_client::types::Region)>,
    ActionError,
> {
    let request_count = datasets_and_regions.len();

    // Allocate regions, and additionally return the dataset that the region was
    // allocated in.
    let datasets_and_regions: Vec<(
        db::model::Dataset,
        crucible_agent_client::types::Region,
    )> = futures::stream::iter(datasets_and_regions)
        .map(|(dataset, region)| async move {
            match ensure_region_in_dataset(log, &dataset, &region).await {
                Ok(result) => Ok((dataset, result)),
                Err(e) => Err(e),
            }
        })
        // Execute the allocation requests concurrently.
        .buffer_unordered(std::cmp::min(
            request_count,
            MAX_CONCURRENT_REGION_REQUESTS,
        ))
        .collect::<Vec<
            Result<
                (db::model::Dataset, crucible_agent_client::types::Region),
                Error,
            >,
        >>()
        .await
        .into_iter()
        .collect::<Result<
            Vec<(db::model::Dataset, crucible_agent_client::types::Region)>,
            Error,
        >>()
        .map_err(ActionError::action_failed)?;

    // Assert each region has the same block size, otherwise Volume creation
    // will fail.
    let all_region_have_same_block_size = datasets_and_regions
        .windows(2)
        .all(|w| w[0].1.block_size == w[1].1.block_size);

    if !all_region_have_same_block_size {
        return Err(ActionError::action_failed(Error::internal_error(
            "volume creation will fail due to block size mismatch",
        )));
    }

    Ok(datasets_and_regions)
}
