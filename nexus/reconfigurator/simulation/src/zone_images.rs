// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulation of TUF repos, zone images, and thereabouts.

use std::collections::BTreeSet;

use camino::Utf8Path;
use nexus_sled_agent_shared::inventory::{
    ZoneArtifactInventory, ZoneManifestBootInventory,
};
use omicron_common::{
    api::external::TufRepoDescription, update::OmicronZoneManifestSource,
};
use swrite::{SWrite, swrite};
use tufaceous_artifact::KnownArtifactKind;

use crate::errors::UnknownZoneNamesError;

/// The reconfigurator simulator's notion of a TUF repository.
#[derive(Clone, Debug)]
pub struct SimTufRepoDescription {
    /// The description and manifest source, or a simulated error.
    pub source: Result<SimTufRepoSource, String>,

    /// A message describing the operation.
    pub message: String,
}

impl SimTufRepoDescription {
    /// Creates a new `SimTufRepoDescription`.
    pub fn new(source: SimTufRepoSource) -> Self {
        let message = source.full_message();
        Self { source: Ok(source), message }
    }

    /// Creates a new description with a simulated error reading the zone
    /// manifest.
    pub fn new_error(message: String) -> Self {
        Self { source: Err(message.clone()), message }
    }

    /// Generates a simulated [`ZoneManifestBootInventory`] or an error.
    pub fn to_boot_inventory(
        &self,
    ) -> Result<ZoneManifestBootInventory, String> {
        match &self.source {
            Ok(source) => Ok(source.to_boot_inventory()),
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
    manifest_source: OmicronZoneManifestSource,
    message: String,
    known_zone_names: BTreeSet<String>,
    error_zone_names: BTreeSet<String>,
}

impl SimTufRepoSource {
    /// Creates a new `SimTufRepoSource`.
    ///
    /// The message should be of the form "from repo at ..." or "to target release".
    pub fn new(
        description: TufRepoDescription,
        manifest_source: OmicronZoneManifestSource,
        message: String,
    ) -> Self {
        let known_zone_names = description
            .artifacts
            .iter()
            .filter_map(|x| {
                (x.id.kind.to_known() == Some(KnownArtifactKind::Zone))
                    .then(|| x.id.name.clone())
            })
            .collect();
        Self {
            description,
            manifest_source,
            message,
            known_zone_names,
            error_zone_names: BTreeSet::new(),
        }
    }

    /// Simulates errors validating zones by the given name.
    ///
    /// Returns an error if any of the provided zone names weren't found in the
    /// description.
    pub fn simulate_zone_errors<I, S>(
        &mut self,
        zone_names: I,
    ) -> Result<(), UnknownZoneNamesError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let (known, unknown): (Vec<_>, Vec<_>) = zone_names
            .into_iter()
            .map(|zone_name| zone_name.as_ref().to_owned())
            .partition(|zone_name| self.known_zone_names.contains(zone_name));
        if !unknown.is_empty() {
            return Err(UnknownZoneNamesError::new(
                unknown,
                self.known_zone_names.clone(),
            ));
        }
        self.error_zone_names.extend(known);
        Ok(())
    }

    /// Generates a simulated [`ZoneManifestBootInventory`].
    pub fn to_boot_inventory(&self) -> ZoneManifestBootInventory {
        let artifacts = self
            .description
            .artifacts
            .iter()
            .filter_map(|artifact| {
                if artifact.id.kind.to_known() != Some(KnownArtifactKind::Zone)
                {
                    return None;
                }

                let file_name = format!("{}.tar.gz", artifact.id.name);
                let path = Utf8Path::new("/fake/path/install").join(&file_name);
                let status =
                    if self.error_zone_names.contains(&artifact.id.name) {
                        Err("reconfigurator-sim: simulated error validating zone image".to_owned())
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
        ZoneManifestBootInventory { source: self.manifest_source, artifacts }
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
        if !self.error_zone_names.is_empty() {
            swrite!(message, ", {} zone errors", self.error_zone_names.len());
        }
        message.push(')');

        message
    }
}
