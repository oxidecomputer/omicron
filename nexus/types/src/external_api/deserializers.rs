// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Deserializer utilities for API parameter types

use std::fmt;

use serde::{
    Deserializer,
    de::{self, Visitor},
};

use crate::external_api::params::SwitchPortUplink;

/// Deserializes an optional `Vec<String>` into `Vec<SwitchPortUplink>` with deduplication.
///
/// This deserializer handles both string and object formats:
/// - String format: "switch0.qsfp0" (from real API calls)
/// - Object format: {"switch_location": "switch0", "port_name": "qsfp0"} (from test serialization)
///
/// Duplicates are automatically removed based on the string representation.
pub fn parse_and_dedup_switch_port_uplinks<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<SwitchPortUplink>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct SwitchPortUplinksVisitor;

    impl<'de> Visitor<'de> for SwitchPortUplinksVisitor {
        type Value = Option<Vec<SwitchPortUplink>>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an optional array of switch port uplinks")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let vec =
                deserializer.deserialize_seq(SwitchPortUplinksSeqVisitor)?;
            Ok(Some(vec))
        }
    }

    struct SwitchPortUplinksSeqVisitor;

    impl<'de> Visitor<'de> for SwitchPortUplinksSeqVisitor {
        type Value = Vec<SwitchPortUplink>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an array of switch port uplinks")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();

            while let Some(item) = seq.next_element::<serde_json::Value>()? {
                let uplink = match item {
                    // Handle string format: "switch0.qsfp0"
                    serde_json::Value::String(s) => {
                        if !seen.insert(s.clone()) {
                            continue; // Skip duplicate
                        }
                        s.parse::<SwitchPortUplink>()
                            .map_err(|e| de::Error::custom(e))?
                    }
                    // Handle object format: {"switch_location": "switch0", "port_name": "qsfp0"}
                    serde_json::Value::Object(_) => {
                        let uplink: SwitchPortUplink =
                            serde_json::from_value(item)
                                .map_err(|e| de::Error::custom(e))?;
                        let uplink_str = uplink.to_string();
                        if !seen.insert(uplink_str) {
                            continue; // Skip duplicate
                        }
                        uplink
                    }
                    _ => {
                        return Err(de::Error::custom(
                            "expected string or object",
                        ));
                    }
                };
                result.push(uplink);
            }
            Ok(result)
        }
    }

    deserializer.deserialize_option(SwitchPortUplinksVisitor)
}
