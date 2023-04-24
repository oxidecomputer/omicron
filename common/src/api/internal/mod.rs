// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internally facing APIs.

pub mod nexus;

use futures::future::ready;
use futures::stream::StreamExt;
use schemars::JsonSchema;
use serde::Serialize;
use uuid::Uuid;

use super::external::ObjectStream;

pub async fn to_list<T, U>(object_stream: ObjectStream<T>) -> Vec<U>
where
    T: Into<U>,
{
    object_stream
        .filter(|maybe_object| ready(maybe_object.is_ok()))
        .map(|maybe_object| maybe_object.unwrap().into())
        .collect::<Vec<U>>()
        .await
}

// Sagas
//
// These are currently only intended for observability by developers.  We will
// eventually want to flesh this out into something more observable for end
// users.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Saga {
    pub id: Uuid,
    pub state: SagaState,
}

impl From<steno::SagaView> for Saga {
    fn from(s: steno::SagaView) -> Self {
        Saga { id: Uuid::from(s.id), state: SagaState::from(s.state) }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SagaState {
    Running,
    Succeeded,
    Failed { error_node_name: steno::NodeName, error_info: SagaErrorInfo },
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum SagaErrorInfo {
    ActionFailed { source_error: serde_json::Value },
    DeserializeFailed { message: String },
    InjectedError,
    SerializeFailed { message: String },
    SubsagaCreateFailed { message: String },
}

impl From<steno::SagaStateView> for SagaState {
    fn from(st: steno::SagaStateView) -> Self {
        match st {
            steno::SagaStateView::Ready { .. } => SagaState::Running,
            steno::SagaStateView::Running { .. } => SagaState::Running,
            steno::SagaStateView::Done {
                result: steno::SagaResult { kind: Ok(_), .. },
                ..
            } => SagaState::Succeeded,
            steno::SagaStateView::Done {
                result: steno::SagaResult { kind: Err(e), .. },
                ..
            } => SagaState::Failed {
                error_node_name: e.error_node_name,
                error_info: match e.error_source {
                    steno::ActionError::ActionFailed { source_error } => {
                        SagaErrorInfo::ActionFailed { source_error }
                    }
                    steno::ActionError::DeserializeFailed { message } => {
                        SagaErrorInfo::DeserializeFailed { message }
                    }
                    steno::ActionError::InjectedError => {
                        SagaErrorInfo::InjectedError
                    }
                    steno::ActionError::SerializeFailed { message } => {
                        SagaErrorInfo::SerializeFailed { message }
                    }
                    steno::ActionError::SubsagaCreateFailed { message } => {
                        SagaErrorInfo::SubsagaCreateFailed { message }
                    }
                },
            },
        }
    }
}
