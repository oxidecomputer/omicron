// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Small helpers and utilities for serde and JSONSchema.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(JsonSchema, Serialize, Deserialize)]
#[serde(rename = "Result{T}Or{E}")]
#[serde(rename_all = "snake_case")]
pub enum SnakeCaseResult<T, E> {
    Ok(T),
    Err(E),
}

/// Serialize a result as a `SnakeCaseResult`.
pub fn serialize_snake_case_result<S, T, E>(
    value: &Result<T, E>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Serialize,
    E: Serialize,
{
    match value {
        Ok(val) => SnakeCaseResult::Ok(val),
        Err(err) => SnakeCaseResult::Err(err),
    }
    .serialize(serializer)
}

/// Deserialize a `SnakeCaseResult` into a Result.
pub fn deserialize_snake_case_result<'de, D, T, E>(
    deserializer: D,
) -> Result<Result<T, E>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
    E: Deserialize<'de>,
{
    SnakeCaseResult::<T, E>::deserialize(deserializer).map(|snek| match snek {
        SnakeCaseResult::Ok(x) => Ok(x),
        SnakeCaseResult::Err(x) => Err(x),
    })
}
