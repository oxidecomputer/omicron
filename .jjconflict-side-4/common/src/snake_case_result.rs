// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A serializable Result that plays nicely with OpenAPI lints.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
#[serde(rename = "Result{T}Or{E}")]
#[serde(rename_all = "snake_case")]
pub enum SnakeCaseResult<T, E> {
    Ok(T),
    Err(E),
}

impl<T, E> JsonSchema for SnakeCaseResult<T, E>
where
    T: JsonSchema,
    E: JsonSchema,
{
    fn schema_name() -> String {
        format!("Result{}Or{}", T::schema_name(), E::schema_name())
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

/// Serialize a result as a `SnakeCaseResult`.
pub fn serialize<S, T, E>(
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
pub fn deserialize<'de, D, T, E>(
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
