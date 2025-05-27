// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(any(test, feature = "test-alerts"))]
pub mod test;

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "alert_class", content = "data")]
pub enum Alert {
    #[cfg(any(test, feature = "test-alerts"))]
    #[serde(rename = "test.foo")]
    Foo(TestFoo),
    #[cfg(any(test, feature = "test-alerts"))]
    #[serde(rename = "test.foo.bar")]
    FooBar(TestFooBar),
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "version")]
#[cfg(any(test, feature = "test-alerts"))]
#[repr(usize)]
pub enum TestFoo {
    #[serde(rename = "1")]
    V1 { hello_world: bool },

    #[serde(rename = "2")]
    V2 { hello_world: bool, new_thing: usize },
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "version")]
#[cfg(any(test, feature = "test-alerts"))]
#[repr(usize)]
pub enum TestFooBar {
    #[serde(rename = "1")]
    V1 { hello: String } = 1,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema = schemars::schema_for!(Alert);
        eprintln!("{}", serde_json::to_string_pretty(&schema).unwrap());
    }
}
