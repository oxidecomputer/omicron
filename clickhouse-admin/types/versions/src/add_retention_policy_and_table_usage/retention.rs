// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU8;

/// Policy for retaining telemetry data.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct RetentionPolicy {
    /// The retention period, in days.
    pub days: Days,
}

/// A number of days used for a retention period.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(try_from = "u8")]
pub struct Days(NonZeroU8);

// Use a custom schema to ensure we never deserialize out-of-range values.
impl JsonSchema for Days {
    fn schema_name() -> String {
        String::from("Days")
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        let mut schema = u8::json_schema(generator).into_object();
        let number = schema.number.as_mut().expect("this is a number");
        number.minimum = Some(1.0);
        number.maximum = Some(MAX_DAYS.into());
        schema.into()
    }
}

impl From<Days> for u8 {
    fn from(val: Days) -> u8 {
        val.0.get()
    }
}

impl TryFrom<u8> for Days {
    type Error = String;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        Self::new(val)
            .ok_or_else(|| format!("days must be in the range [1, {MAX_DAYS}]"))
    }
}

const MAX_DAYS: u8 = 30;

impl Days {
    /// Return a new number of days.
    ///
    /// The argument must be within [1, 30].
    pub const fn new(val: u8) -> Option<Self> {
        if val > MAX_DAYS {
            None
        } else {
            match NonZeroU8::new(val) {
                Some(v) => Some(Self(v)),
                None => None,
            }
        }
    }

    /// Return a human-friendly string displaying the policy.
    pub fn as_human_str(&self) -> String {
        let n_days = self.0.get();
        let suffix = if n_days >= 1 { "days" } else { "day" };
        format!("{n_days} {suffix}")
    }
}
