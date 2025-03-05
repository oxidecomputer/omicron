use std::collections::{BTreeMap, BTreeSet, HashMap};

use camino::Utf8PathBuf;
use schemars::{
    JsonSchema,
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Zone = String;
pub type Service = BTreeMap<String, Vec<Utf8PathBuf>>;

#[derive(Serialize, Deserialize, Default, JsonSchema)]
pub struct SledDiagnosticsLogs {
    #[schemars(schema_with = "path_schema")]
    logs: BTreeMap<Zone, Service>,
}

impl SledDiagnosticsLogs {
    pub fn logs(&self) -> &BTreeMap<Zone, Service> {
        &self.logs
    }
}

#[derive(Debug, Error)]
pub enum SledDiagnosticsLogError {
    #[error("Failed to find log paths: {0}")]
    ZonePaths(anyhow::Error),
}

// Used for schemars to be able to be used with camino:
// See https://github.com/camino-rs/camino/issues/91#issuecomment-2027908513
fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some("Utf8PathBuf".to_owned());
    schema.into()
}

/// Provides all of the valid directores logs can be found in for this sled.
pub fn valid_log_dirs() -> Result<BTreeSet<Utf8PathBuf>, SledDiagnosticsLogError>
{
    let zones =
        oxlog::Zones::load().map_err(SledDiagnosticsLogError::ZonePaths)?;
    Ok(zones
        .zones
        .into_values()
        .flat_map(|entry| {
            let mut paths = Vec::new();
            paths.push(entry.primary);
            paths.extend(entry.debug);
            paths.extend(entry.extra.into_iter().map(|e| e.1));
            paths
        })
        .collect())
}

/// Find the relevant logs that should be returned as apart of a Support Bundle.
pub fn find_logs() -> Result<SledDiagnosticsLogs, SledDiagnosticsLogError> {
    let zones =
        oxlog::Zones::load().map_err(SledDiagnosticsLogError::ZonePaths)?;

    let mut result = SledDiagnosticsLogs { logs: BTreeMap::new() };
    for zone in zones.zones.keys() {
        let filter = oxlog::Filter {
            current: true,
            archived: true,
            extra: true,
            // avoid having to call `lstat` on each log file
            show_empty: true,
        };

        let mut logs = Service::new();
        for (svc, mut svclogs) in zones.zone_logs(&zone, filter) {
            // Current
            if let Some(current) = svclogs.current {
                logs.insert(svc.clone(), vec![current.path]);
            }

            // Archived
            logs.entry(svc.clone()).or_default().extend(
                svclogs.archived.into_iter().rev().take(5).map(|s| s.path),
            );

            // Extra
            let extra = std::mem::take(&mut svclogs.extra);
            for (_, cockroach) in process_extra_logs(extra) {
                logs.entry(svc.clone()).or_default().extend(cockroach.current);
                logs.entry(svc.clone())
                    .or_default()
                    .extend(cockroach.rotated.into_iter().rev().take(5));
            }
        }
        result.logs.insert(zone.clone(), logs);
    }

    Ok(result)
}

#[derive(Default, Debug, PartialEq)]
struct CockroachExtraLog {
    current: Vec<Utf8PathBuf>,
    rotated: Vec<Utf8PathBuf>,
}

/// Walk over the extra logs for cockroachdb preserving order and sort them into
/// the appropriate buckets.
fn process_extra_logs(
    extra: Vec<oxlog::LogFile>,
) -> HashMap<String, CockroachExtraLog> {
    // Known logging paths for cockroachdb:
    // https://www.cockroachlabs.com/docs/stable/logging-overview#logging-destinations
    let cockroach_logs = [
        "cockroach",
        "cockroach-health",
        "cockroach-kv-distribution",
        "cockroach-security",
        "cockroach-sql-audit",
        "cockroach-sql-auth",
        "cockroach-sql-exec",
        "cockroach-sql-slow",
        "cockroach-sql-schema",
        "cockroach-pebble",
        "cockroach-telemetry",
        // Not documented but found on our sleds
        "cockroach-stderr",
    ];

    // Find all cockroachdb extra logs and split them up by file prefix
    let mut sorted: HashMap<String, CockroachExtraLog> = HashMap::new();
    for file in extra {
        if let Some(file_name) = file.path.file_name() {
            if let Some(file_parts) = file_name.split_once(".") {
                if cockroach_logs.contains(&file_parts.0) {
                    match file_parts.1 {
                        // Current
                        "log" => {
                            sorted
                                .entry(file_parts.0.to_string())
                                .or_default()
                                .current
                                .push(file.path);
                        }
                        // Rotated
                        _ => {
                            sorted
                                .entry(file_parts.0.to_string())
                                .or_default()
                                .rotated
                                .push(file.path);
                        }
                    };
                }
            }
        };
    }

    sorted
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use camino::Utf8PathBuf;

    use super::*;

    #[test]
    fn test_process_extra_logs() {
        let logs: Vec<_> = [
            "cockroach-health.log",
            "cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T21_43_26Z.011435.log",
            "cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_53Z.011486.log",
            "cockroach-stderr.log",
            "cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-30T18_56_19Z.011950.log",
            "cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-31T02_59_24Z.010479.log",
            "cockroach.log",
            "cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T17_11_45Z.011435.log",
            "cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_51Z.011486.log",
        ].into_iter().map(|l| {
                oxlog::LogFile { path: Utf8PathBuf::from(l), size: None, modified: None }
            }).collect();

        let mut expected: HashMap<String, CockroachExtraLog> = HashMap::new();

        // cockroach
        expected
            .entry("cockroach".to_string())
            .or_default()
            .current
            .push(Utf8PathBuf::from("cockroach.log"));
        expected
            .entry("cockroach".to_string())
            .or_default()
            .rotated
            .push(Utf8PathBuf::from("cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T17_11_45Z.011435.log"));
        expected
            .entry("cockroach".to_string())
            .or_default()
            .rotated
            .push(Utf8PathBuf::from("cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_51Z.011486.log"));

        // cockroach-health
        expected
            .entry("cockroach-health".to_string())
            .or_default()
            .current
            .push(Utf8PathBuf::from("cockroach-health.log"));
        expected
            .entry("cockroach-health".to_string())
            .or_default()
            .rotated
            .push(Utf8PathBuf::from("cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T21_43_26Z.011435.log"));
        expected
            .entry("cockroach-health".to_string())
            .or_default()
            .rotated
            .push(Utf8PathBuf::from("cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_53Z.011486.log"));

        // cockroach-stderr
        expected
            .entry("cockroach-stderr".to_string())
            .or_default()
            .current
            .push(Utf8PathBuf::from("cockroach-stderr.log"));
        expected
            .entry("cockroach-stderr".to_string())
            .or_default()
            .rotated
            .push(Utf8PathBuf::from("cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-30T18_56_19Z.011950.log"));
        expected
            .entry("cockroach-stderr".to_string())
            .or_default()
            .rotated
            .push(Utf8PathBuf::from("cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-31T02_59_24Z.010479.log"));

        let extra = process_extra_logs(logs);
        assert_eq!(
            extra, expected,
            "cockroachdb extra logs are properly sorted"
        );
    }
}
