use std::sync::Arc;

use omicron_common::api::external::Error;
use steno::{DagBuilder, SagaDag, SagaName};
use thiserror::Error;

use crate::saga_context::SagaContext;

#[derive(Debug)]
pub struct NexusSagaType;

impl steno::SagaType for NexusSagaType {
    type ExecContextType = Arc<SagaContext>;
}

pub type ActionRegistry = steno::ActionRegistry<NexusSagaType>;
pub type NexusAction = Arc<dyn steno::Action<NexusSagaType>>;
pub type NexusActionContext = steno::ActionContext<NexusSagaType>;

/// Given a particular kind of Nexus saga (the type parameter `N`) and
/// parameters for that saga, construct a [`SagaDag`] for it.
pub fn create_saga_dag<N: NexusSaga2>(
    params: N::Params,
) -> Result<SagaDag, Error> {
    N::prepare(&params)
}

pub trait NexusSaga2 {
    const NAME: &'static str;

    type Params: serde::Serialize
        + serde::de::DeserializeOwned
        + std::fmt::Debug;

    fn actions() -> Vec<NexusAction>;

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
