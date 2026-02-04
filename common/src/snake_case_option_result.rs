// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A serializable `Option<Result>` that plays nicely with OpenAPI lints.

use crate::snake_case_result::SnakeCaseResult;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(rename = "OptionResult{T}Or{E}")]
#[serde(untagged)]
pub enum SnakeCaseOptionResult<T, E> {
    Some(SnakeCaseResult<T, E>),
    None,
}

impl<T, E> JsonSchema for SnakeCaseOptionResult<T, E>
where
    T: JsonSchema,
    E: JsonSchema,
{
    fn schema_name() -> String {
        format!("OptionResult{}Or{}", T::schema_name(), E::schema_name())
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        let mut ok_schema = schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Object.into()),
            ..Default::default()
        };
        let obj = ok_schema.object();
        obj.required.insert("ok".to_owned());
        obj.properties.insert("ok".to_owned(), generator.subschema_for::<T>());

        let mut err_schema = schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Object.into()),
            ..Default::default()
        };
        let obj = err_schema.object();
        obj.required.insert("err".to_owned());
        obj.properties.insert("err".to_owned(), generator.subschema_for::<E>());

        let mut schema = schemars::schema::SchemaObject::default();
        schema.subschemas().one_of =
            Some(vec![ok_schema.into(), err_schema.into()]);

        schema
            .extensions
            .insert(String::from("nullable"), serde_json::json!(true));
        schema.extensions.insert(
            String::from("x-rust-type"),
            serde_json::json!({
                "crate": "std",
                "version": "*",
                "path": "::std::result::Result",
                "parameters": [
                    generator.subschema_for::<T>(),
                    generator.subschema_for::<E>(),
                ],
            }),
        );
        schema.into()
    }
}

/// Serialize an Option<Result<T, E>> as a `SnakeCaseOptionResult`.
pub fn serialize<S, T, E>(
    value: &Option<Result<T, E>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Serialize,
    E: Serialize,
{
    match value {
        None => serializer.serialize_none(),
        Some(Ok(val)) => {
            SnakeCaseOptionResult::<&T, &E>::Some(SnakeCaseResult::Ok(val))
                .serialize(serializer)
        }
        Some(Err(err)) => {
            SnakeCaseOptionResult::<&T, &E>::Some(SnakeCaseResult::Err(err))
                .serialize(serializer)
        }
    }
}

/// Deserialize a `SnakeCaseOptionResult` into an `Option<Result>`.
pub fn deserialize<'de, D, T, E>(
    deserializer: D,
) -> Result<Option<Result<T, E>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
    E: Deserialize<'de>,
{
    SnakeCaseOptionResult::<T, E>::deserialize(deserializer).map(|snek| {
        match snek {
            SnakeCaseOptionResult::Some(SnakeCaseResult::Ok(val)) => {
                Some(Ok(val))
            }
            SnakeCaseOptionResult::Some(SnakeCaseResult::Err(err)) => {
                Some(Err(err))
            }
            SnakeCaseOptionResult::None => None,
        }
    })
}
