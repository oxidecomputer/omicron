// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::TimeDelta;
use serde::Deserialize;
use serde::Serialize;

/// It's just a type with the same representation as a `TimeDelta` that
/// implements `Serialize` and `Deserialize`, because `chrono`'s `Deserialize`
/// implementation for this type is not actually for `TimeDelta`, but for the
/// `rkyv::Archived` wrapper type (see [here]). While `chrono` *does* provide a
/// `Serialize` implementation that we could use with this type, it's preferable
/// to provide our own `Serialize` as well as `Deserialize`, since a future
/// semver-compatible change in `chrono` could change the struct's internal
/// representation, quietly breaking our ability to round-trip it. So, let's
/// just derive both traits for this thing, which we control.
///
/// If you feel like this is unfortunate...yeah, I do too.
///
/// [here]: https://docs.rs/chrono/latest/chrono/struct.TimeDelta.html#impl-Deserialize%3CTimeDelta,+__D%3E-for-%3CTimeDelta+as+Archive%3E::Archived
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SerdeTimeDelta {
    secs: i64,
    nanos: i32,
}

impl From<TimeDelta> for SerdeTimeDelta {
    fn from(delta: TimeDelta) -> Self {
        Self { secs: delta.num_seconds(), nanos: delta.subsec_nanos() }
    }
}

impl TryFrom<SerdeTimeDelta> for TimeDelta {
    type Error = &'static str;
    fn try_from(
        SerdeTimeDelta { secs, nanos }: SerdeTimeDelta,
    ) -> Result<Self, Self::Error> {
        // This is a bit weird: `chrono::TimeDelta`'s getter for
        // nanoseconds (`TimeDelta::subsec_nanos`) returns them as an i32,
        // with the sign coming from the seconds part, but when constructing
        // a `TimeDelta`, it takes them as a `u32` and panics if they're too
        // big. So, we take the absolute value here, because what the serialize
        // impl saw may have had its sign bit set, but the constructor will get
        // mad if we give it something with that bit set. Hopefully that made
        // sense?
        let nanos = nanos.unsigned_abs();
        TimeDelta::new(secs, nanos).ok_or("time delta out of range")
    }
}

pub(crate) mod optional_time_delta {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<TimeDelta>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = Option::<SerdeTimeDelta>::deserialize(deserializer)?;
        match val {
            None => return Ok(None),
            Some(delta) => delta
                .try_into()
                .map_err(|e| {
                    <D::Error as serde::de::Error>::custom(format!(
                        "{e}: {val:?}"
                    ))
                })
                .map(Some),
        }
    }

    pub(crate) fn serialize<S>(
        td: &Option<TimeDelta>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        td.as_ref()
            .map(|&delta| SerdeTimeDelta::from(delta))
            .serialize(serializer)
    }
}
