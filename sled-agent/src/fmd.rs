// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collects fault information from the illumos Fault Management Daemon (FMD).

use sled_agent_types::inventory::FmdInventoryResult;

#[cfg(target_os = "illumos")]
mod illumos {
    use fmd_adm::{FmdAdm, NvList, NvValue};
    use sled_agent_types::inventory::{
        FmdCase, FmdInventory, FmdInventoryResult, FmdResource,
    };

    pub(super) fn nvvalue_to_json(value: &NvValue) -> serde_json::Value {
        match value {
            NvValue::Boolean => serde_json::Value::Bool(true),
            NvValue::BooleanValue(b) => serde_json::Value::Bool(*b),
            NvValue::Byte(n) => serde_json::json!(*n),
            NvValue::Int8(n) => serde_json::json!(*n),
            NvValue::UInt8(n) => serde_json::json!(*n),
            NvValue::Int16(n) => serde_json::json!(*n),
            NvValue::UInt16(n) => serde_json::json!(*n),
            NvValue::Int32(n) => serde_json::json!(*n),
            NvValue::UInt32(n) => serde_json::json!(*n),
            NvValue::Int64(n) => serde_json::json!(*n),
            NvValue::UInt64(n) => serde_json::json!(*n),
            NvValue::Double(f) => serde_json::json!(*f),
            NvValue::String(s) => serde_json::Value::String(s.clone()),
            NvValue::Hrtime(n) => serde_json::json!(*n),
            NvValue::NvList(nvl) => nvlist_to_json(nvl),
            NvValue::BooleanArray(arr) => serde_json::json!(arr),
            NvValue::ByteArray(arr) => serde_json::json!(arr),
            NvValue::Int8Array(arr) => serde_json::json!(arr),
            NvValue::UInt8Array(arr) => serde_json::json!(arr),
            NvValue::Int16Array(arr) => serde_json::json!(arr),
            NvValue::UInt16Array(arr) => serde_json::json!(arr),
            NvValue::Int32Array(arr) => serde_json::json!(arr),
            NvValue::UInt32Array(arr) => serde_json::json!(arr),
            NvValue::Int64Array(arr) => serde_json::json!(arr),
            NvValue::UInt64Array(arr) => serde_json::json!(arr),
            NvValue::StringArray(arr) => serde_json::json!(arr),
            NvValue::NvListArray(arr) => {
                let items: Vec<serde_json::Value> =
                    arr.iter().map(nvlist_to_json).collect();
                serde_json::Value::Array(items)
            }
            NvValue::Unknown { type_code } => {
                serde_json::json!({
                    "_unknown_type": format!("{type_code:?}")
                })
            }
        }
    }

    pub(super) fn nvlist_to_json(nvl: &NvList) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for (name, value) in nvl {
            map.insert(name.to_string(), nvvalue_to_json(value));
        }
        serde_json::Value::Object(map)
    }

    pub(super) fn collect() -> FmdInventoryResult {
        let adm = match FmdAdm::open() {
            Ok(adm) => adm,
            Err(e) => {
                return FmdInventoryResult::Error {
                    error: format!("failed to open fmd: {e}"),
                };
            }
        };

        let cases = match adm.cases(None) {
            Ok(cases) => cases
                .into_iter()
                .map(|c| FmdCase {
                    uuid: c.uuid,
                    code: c.code,
                    url: c.url,
                    event: c.event.as_ref().map(nvlist_to_json),
                })
                .collect(),
            Err(e) => {
                return FmdInventoryResult::Error {
                    error: format!("failed to list fmd cases: {e}"),
                };
            }
        };

        let resources = match adm.resources(true) {
            Ok(resources) => resources
                .into_iter()
                .map(|r| FmdResource {
                    fmri: r.fmri,
                    uuid: r.uuid,
                    case_id: r.case,
                    faulty: r.faulty,
                    unusable: r.unusable,
                    invisible: r.invisible,
                })
                .collect(),
            Err(e) => {
                return FmdInventoryResult::Error {
                    error: format!("failed to list fmd resources: {e}"),
                };
            }
        };

        FmdInventoryResult::Available(FmdInventory { cases, resources })
    }
}

pub(crate) async fn collect_fmd_inventory() -> FmdInventoryResult {
    #[cfg(target_os = "illumos")]
    {
        // FMD queries go through door calls to fmd(1M) and can block, so run
        // them on a blocking-friendly thread rather than stalling the runtime.
        match tokio::task::spawn_blocking(illumos::collect).await {
            Ok(inv) => inv,
            Err(e) => FmdInventoryResult::Error {
                error: format!("fmd collection task failed: {e}"),
            },
        }
    }
    #[cfg(not(target_os = "illumos"))]
    {
        FmdInventoryResult::Error {
            error: "fmd not supported on this platform".to_string(),
        }
    }
}

#[cfg(test)]
#[cfg(target_os = "illumos")]
mod tests {
    use super::illumos::nvvalue_to_json;
    use fmd_adm::NvValue;

    #[test]
    fn boolean_presence() {
        assert_eq!(nvvalue_to_json(&NvValue::Boolean), serde_json::json!(true));
    }

    #[test]
    fn boolean_value() {
        assert_eq!(
            nvvalue_to_json(&NvValue::BooleanValue(false)),
            serde_json::json!(false),
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::BooleanValue(true)),
            serde_json::json!(true),
        );
    }

    #[test]
    fn integers() {
        assert_eq!(nvvalue_to_json(&NvValue::Byte(42)), serde_json::json!(42));
        assert_eq!(nvvalue_to_json(&NvValue::Int8(-1)), serde_json::json!(-1));
        assert_eq!(
            nvvalue_to_json(&NvValue::UInt8(255)),
            serde_json::json!(255)
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::Int16(-32000)),
            serde_json::json!(-32000),
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::UInt16(65535)),
            serde_json::json!(65535),
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::Int32(-100_000)),
            serde_json::json!(-100_000),
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::UInt32(4_000_000_000)),
            serde_json::json!(4_000_000_000u64),
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::Int64(i64::MIN)),
            serde_json::json!(i64::MIN),
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::UInt64(u64::MAX)),
            serde_json::json!(u64::MAX),
        );
    }

    #[test]
    fn double() {
        assert_eq!(
            nvvalue_to_json(&NvValue::Double(4.2069)),
            serde_json::json!(4.2069),
        );
    }

    #[test]
    fn string() {
        assert_eq!(
            nvvalue_to_json(&NvValue::String("hello".to_string())),
            serde_json::json!("hello"),
        );
    }

    #[test]
    fn hrtime() {
        assert_eq!(
            nvvalue_to_json(&NvValue::Hrtime(1_000_000_000)),
            serde_json::json!(1_000_000_000i64),
        );
    }

    #[test]
    fn integer_arrays() {
        assert_eq!(
            nvvalue_to_json(&NvValue::Int32Array(vec![1, 2, 3])),
            serde_json::json!([1, 2, 3]),
        );
        assert_eq!(
            nvvalue_to_json(&NvValue::UInt8Array(vec![0, 128, 255])),
            serde_json::json!([0, 128, 255]),
        );
    }

    #[test]
    fn boolean_array() {
        assert_eq!(
            nvvalue_to_json(&NvValue::BooleanArray(vec![true, false, true])),
            serde_json::json!([true, false, true]),
        );
    }

    #[test]
    fn string_array() {
        assert_eq!(
            nvvalue_to_json(&NvValue::StringArray(vec![
                "a".to_string(),
                "b".to_string(),
            ])),
            serde_json::json!(["a", "b"]),
        );
    }

    #[test]
    fn unknown_type() {
        // The type_code is a data_type_t from the illumos nvpair FFI.
        // We just format it via Debug.
        let val = NvValue::Unknown { type_code: 0 };
        let json = nvvalue_to_json(&val);
        // Should be an object with a single "_unknown_type" key.
        assert!(json.is_object());
        assert!(json.get("_unknown_type").unwrap().is_string());
    }
}
