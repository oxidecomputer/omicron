// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulation of TUF repos, zone images, and thereabouts.

use std::collections::BTreeSet;

use anyhow::bail;
use camino::Utf8Path;
use itertools::Itertools;
use omicron_common::{
    api::external::TufRepoDescription, update::OmicronInstallManifestSource,
};
use sled_agent_types::inventory::{
    ManifestBootInventory, ZoneArtifactInventory, ZoneKind,
};
use swrite::{SWrite, swrite};
use tufaceous_artifact::KnownArtifactKind;

use crate::errors::UnknownZoneNamesError;

/// The reconfigurator simulator's notion of a TUF repository.
#[derive(Clone, Debug)]
pub struct SimTufRepoDescription {
    /// The description and manifest source, or a simulated error.
    pub source: Result<SimTufRepoSource, String>,

    /// A simulated error for just the measurement manifest
    pub measurement_error: Option<String>,

    /// A message describing the operation.
    pub message: String,
}

impl SimTufRepoDescription {
    /// Creates a new `SimTufRepoDescription`.
    pub fn new(source: SimTufRepoSource) -> Self {
        let message = source.full_message();
        Self { source: Ok(source), measurement_error: None, message }
    }

    /// Creates a new `SimTufRepoDescription` with a simulated error with
    /// the measurement manifest but a valid zone manifest
    pub fn new_measurement_error(
        source: SimTufRepoSource,
        measurement_error: String,
    ) -> Self {
        let message = source.full_message();
        Self {
            source: Ok(source),
            measurement_error: Some(measurement_error),
            message,
        }
    }

    /// Creates a new description with a simulated error reading the zone
    /// manifest.
    pub fn new_error(message: String) -> Self {
        Self { source: Err(message.clone()), measurement_error: None, message }
    }

    /// Generates a simulated [`ManifestBootInventory`] for zones or an error.
    pub fn to_zone_boot_inventory(
        &self,
    ) -> Result<ManifestBootInventory, String> {
        match &self.source {
            Ok(source) => Ok(source.to_zone_boot_inventory()),
            Err(error) => {
                Err(format!("reconfigurator-sim simulated error: {error}"))
            }
        }
    }

    /// Generates a simulated [`ManifestBootInventory`] for measurements or an error.
    pub fn to_measurement_boot_inventory(
        &self,
    ) -> Result<ManifestBootInventory, String> {
        match &self.source {
            Ok(source) => match &self.measurement_error {
                None => Ok(source.to_measurement_boot_inventory()),
                Some(error) => Err(format!(
                    "reconfigurator-sim simulated measurement error: {error}"
                )),
            },
            Err(error) => {
                Err(format!("reconfigurator-sim simulated error: {error}"))
            }
        }
    }
}

/// The reconfigurator simulator's notion of a TUF repository where there wasn't
/// an error reading the zone manifest.
#[derive(Clone, Debug)]
pub struct SimTufRepoSource {
    description: TufRepoDescription,
    zone_manifest_source: OmicronInstallManifestSource,
    measurement_manifest_source: OmicronInstallManifestSource,
    message: String,
    known_artifact_id_names: BTreeSet<String>,
    error_artifact_id_names: BTreeSet<String>,
}

impl SimTufRepoSource {
    /// Creates a new `SimTufRepoSource`.
    ///
    /// The message should be of the form "from repo at ..." or "to target release".
    pub fn new(
        description: TufRepoDescription,
        zone_manifest_source: OmicronInstallManifestSource,
        measurement_manifest_source: OmicronInstallManifestSource,
        message: String,
    ) -> anyhow::Result<Self> {
        let mut unknown = BTreeSet::new();
        let known = description
            .artifacts
            .iter()
            .filter_map(|artifact| {
                if artifact.id.kind.to_known() != Some(KnownArtifactKind::Zone)
                {
                    return None;
                }

                // Check that the zone name is known to ZoneKind.
                if ZoneKind::artifact_id_name_to_install_dataset_file(
                    &artifact.id.name,
                )
                .is_some()
                {
                    Some(artifact.id.name.clone())
                } else {
                    unknown.insert(artifact.id.name.clone());
                    None
                }
            })
            .collect();
        if !unknown.is_empty() {
            bail!(
                "unknown zone artifact ID names in provided description \
                 ({message}): {}",
                unknown.iter().join(", "),
            );
        }
        Ok(Self {
            description,
            zone_manifest_source,
            measurement_manifest_source,
            message,
            known_artifact_id_names: known,
            error_artifact_id_names: BTreeSet::new(),
        })
    }

    /// Simulates errors validating zones by the given artifact ID name.
    ///
    /// Returns an error if any of the provided zone names weren't found in the
    /// description.
    pub fn simulate_zone_errors<I, S>(
        &mut self,
        artifact_id_names: I,
    ) -> Result<(), UnknownZoneNamesError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let (known, unknown): (Vec<_>, Vec<_>) = artifact_id_names
            .into_iter()
            .map(|zone_name| zone_name.as_ref().to_owned())
            .partition(|zone_name| {
                self.known_artifact_id_names.contains(zone_name)
            });
        if !unknown.is_empty() {
            return Err(UnknownZoneNamesError::new(
                unknown,
                self.known_artifact_id_names.clone(),
            ));
        }
        self.error_artifact_id_names.extend(known);
        Ok(())
    }

    /// Generates a simulated [`ManifestBootInventory`] from the measurement manifest.
    pub fn to_measurement_boot_inventory(&self) -> ManifestBootInventory {
        let artifacts = self
            .description
            .artifacts
            .iter()
            .filter_map(|artifact| {
                if artifact.id.kind.to_known()
                    != Some(KnownArtifactKind::MeasurementCorpus)
                {
                    return None;
                }

                let file_name = artifact.id.name.to_string();
                let path = Utf8Path::new("/fake/path/install").join(&file_name);
                let status =
                    if self.error_artifact_id_names.contains(&artifact.id.name)
                    {
                        Err("reconfigurator-sim: simulated error \
                             validating zone image"
                            .to_owned())
                    } else {
                        Ok(())
                    };
                Some(ZoneArtifactInventory {
                    file_name,
                    path,
                    expected_size: artifact.size,
                    expected_hash: artifact.hash,
                    status,
                })
            })
            .collect();
        ManifestBootInventory {
            source: self.measurement_manifest_source,
            artifacts,
        }
    }

    /// Generates a simulated [`ManifestBootInventory`] from the zone manifest.
    pub fn to_zone_boot_inventory(&self) -> ManifestBootInventory {
        let artifacts = self
            .description
            .artifacts
            .iter()
            .filter_map(|artifact| {
                if artifact.id.kind.to_known() != Some(KnownArtifactKind::Zone)
                {
                    return None;
                }

                let file_name =
                    ZoneKind::artifact_id_name_to_install_dataset_file(
                        &artifact.id.name,
                    )
                    .expect("we checked this was Some at construction time")
                    .to_owned();
                let path = Utf8Path::new("/fake/path/install").join(&file_name);
                let status =
                    if self.error_artifact_id_names.contains(&artifact.id.name)
                    {
                        Err("reconfigurator-sim: simulated error \
                             validating zone image"
                            .to_owned())
                    } else {
                        Ok(())
                    };
                Some(ZoneArtifactInventory {
                    file_name,
                    path,
                    expected_size: artifact.size,
                    expected_hash: artifact.hash,
                    status,
                })
            })
            .collect();
        ManifestBootInventory { source: self.zone_manifest_source, artifacts }
    }

    /// Returns a message including the system version and the number of zone
    /// errors.
    pub fn full_message(&self) -> String {
        let mut message = self.message.clone();
        swrite!(
            message,
            " (system version {}",
            self.description.repo.system_version
        );
        if !self.error_artifact_id_names.is_empty() {
            swrite!(
                message,
                ", {} zone errors",
                self.error_artifact_id_names.len()
            );
        }
        message.push(')');

        message
    }
}
