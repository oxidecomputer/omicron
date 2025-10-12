// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{
    collections::{BTreeSet, HashMap},
    fmt,
};

use anyhow::anyhow;
use iddqd::{IdOrdItem, IdOrdMap, id_ord_map::RefMut, id_upcast};
use nexus_sled_agent_shared::inventory::{
    BootPartitionContents, BootPartitionDetails,
    MeasurementManifestBootInventory, ZoneKind, ZoneManifestBootInventory,
};
use nexus_types::{
    deployment::{
        BlueprintArtifactVersion, BlueprintHostPhase2DesiredContents,
        BlueprintHostPhase2DesiredSlots,
        BlueprintMeasurementSetDesiredContents,
        BlueprintMeasurementsDesiredContents, BlueprintSingleMeasurement,
        BlueprintZoneConfig, BlueprintZoneDisposition,
        BlueprintZoneImageSource, PlanningInput, SledFilter,
        TargetReleaseDescription,
    },
    inventory::Collection,
};
use omicron_common::api::external::TufArtifactMeta;
use omicron_uuid_kinds::{MupdateOverrideUuid, OmicronZoneUuid, SledUuid};
use slog::{debug, info, o, warn};
use tufaceous_artifact::ArtifactHash;

use crate::blueprint_builder::{BlueprintBuilder, Error};

/// Information about zones eligible for noop conversion from `InstallDataset`
/// to `Artifact`.
#[derive(Clone, Debug)]
pub(crate) enum NoopConvertInfo {
    /// There's a global reason due to which no-op conversions cannot occur.
    GlobalIneligible(NoopConvertGlobalIneligibleReason),

    /// Global checks have passed.
    GlobalEligible { sleds: IdOrdMap<NoopConvertSledInfo> },
}

impl NoopConvertInfo {
    pub(crate) fn new(
        input: &PlanningInput,
        inventory: &Collection,
        blueprint: &BlueprintBuilder<'_>,
    ) -> Result<Self, Error> {
        let TargetReleaseDescription::TufRepo(current_artifacts) =
            input.tuf_repo().description()
        else {
            return Ok(Self::GlobalIneligible(
                NoopConvertGlobalIneligibleReason::NoTargetRelease,
            ));
        };

        let mut sleds = IdOrdMap::new();

        let artifacts_by_hash: HashMap<_, _> = current_artifacts
            .artifacts
            .iter()
            .map(|artifact| (artifact.hash, artifact))
            .collect();

        for sled_id in input.all_sled_ids(SledFilter::InService) {
            let Some(inv_sled) = inventory.sled_agents.get(&sled_id) else {
                sleds
                    .insert_unique(NoopConvertSledInfo {
                        sled_id,
                        status: NoopConvertSledStatus::Ineligible(
                            NoopConvertSledIneligibleReason::NotInInventory,
                        ),
                    })
                    .expect("sled IDs are unique");
                continue;
            };

            let zone_manifest = match &inv_sled
                .zone_image_resolver
                .zone_manifest
                .boot_inventory
            {
                Ok(zm) => zm,
                Err(message) => {
                    sleds
                        .insert_unique(NoopConvertSledInfo {
                            sled_id,
                            status: NoopConvertSledStatus::Ineligible(
                                NoopConvertSledIneligibleReason::ManifestError {
                                    message: message.to_owned(),
                                },
                            ),
                        })
                        .expect("sled IDs are unique");
                    continue;
                }
            };

            let measurement_manifest = match &inv_sled
                .measurement_resolver
                .measurement_manifest
                .boot_inventory
            {
                Ok(zm) => zm,
                Err(message) => {
                    sleds
                        .insert_unique(NoopConvertSledInfo {
                            sled_id,
                            status: NoopConvertSledStatus::Ineligible(
                                NoopConvertSledIneligibleReason::ManifestError {
                                    message: message.to_owned(),
                                },
                            ),
                        })
                        .expect("sled IDs are unique");
                    continue;
                }
            };

            let measurements = blueprint.current_sled_measurements(sled_id)?;

            let measurements = NoopConvertMeasurements::new(
                measurements,
                measurement_manifest,
                &artifacts_by_hash,
            );

            // Out of these, which zones' hashes (as reported in the zone
            // manifest) match the corresponding ones in the TUF repo?
            let zones = blueprint
                .current_sled_zones(
                    sled_id,
                    BlueprintZoneDisposition::is_in_service,
                )
                .map(|zone| {
                    NoopConvertZoneInfo::new(
                        zone,
                        zone_manifest,
                        &artifacts_by_hash,
                    )
                })
                .collect();

            let host_phase_2 = NoopConvertHostPhase2Slots::new(
                blueprint.current_sled_host_phase_2(sled_id)?,
                inv_sled
                    .last_reconciliation
                    .as_ref()
                    .map(|s| &s.boot_partitions),
                &artifacts_by_hash,
            );

            let status = if let Some(mupdate_override_id) =
                blueprint.sled_get_remove_mupdate_override(sled_id)?
            {
                NoopConvertSledStatus::Ineligible(
                    NoopConvertSledIneligibleReason::MupdateOverride {
                        mupdate_override_id,
                        zones,
                        host_phase_2: Box::new(host_phase_2),
                        measurements,
                    },
                )
            } else {
                NoopConvertSledStatus::Eligible(NoopConvertSledEligible {
                    zones,
                    host_phase_2,
                    measurements,
                })
            };

            sleds
                .insert_unique(NoopConvertSledInfo { sled_id, status })
                .expect("sled IDs are unique");
        }

        Ok(Self::GlobalEligible { sleds })
    }

    pub(crate) fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::GlobalIneligible(reason) => {
                info!(
                    log,
                    "skipping noop image source check for all sleds";
                    "reason" => %reason,
                );
            }
            Self::GlobalEligible { sleds } => {
                for sled in sleds {
                    let log =
                        log.new(o!("sled_id" => sled.sled_id.to_string()));
                    sled.status.log_to(&log);
                }
            }
        }
    }

    /// Return a mutable reference to [`NoopConvertSledInfo`] for the given
    /// sled.
    ///
    /// Returns `Err(Error::Planner)` if the sled ID wasn't found.
    pub(crate) fn sled_info_mut(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<NoopConvertSledInfoMut<'_>, Error> {
        match self {
            Self::GlobalIneligible(_) => {
                Ok(NoopConvertSledInfoMut::GlobalIneligible(
                    NoopConvertGlobalIneligibleReason::NoTargetRelease,
                ))
            }
            Self::GlobalEligible { sleds } => {
                let Some(sled_info) = sleds.get_mut(&sled_id) else {
                    return Err(Error::Planner(anyhow!(
                        "tried to get noop convert zone info \
                         for unknown sled {sled_id}"
                    )));
                };
                Ok(NoopConvertSledInfoMut::Ok(sled_info))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum NoopConvertSledInfoMut<'a> {
    Ok(RefMut<'a, NoopConvertSledInfo>),
    GlobalIneligible(NoopConvertGlobalIneligibleReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertGlobalIneligibleReason {
    /// No target release was set.
    NoTargetRelease,
}

impl fmt::Display for NoopConvertGlobalIneligibleReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoTargetRelease => {
                write!(f, "no target release is currently set")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NoopConvertSledInfo {
    pub(crate) sled_id: SledUuid,
    pub(crate) status: NoopConvertSledStatus,
}

impl IdOrdItem for NoopConvertSledInfo {
    type Key<'a> = SledUuid;

    fn key(&self) -> Self::Key<'_> {
        self.sled_id
    }

    id_upcast!();
}

#[derive(Clone, Debug)]
pub(crate) enum NoopConvertSledStatus {
    /// The sled is ineligible for conversion.
    Ineligible(NoopConvertSledIneligibleReason),

    /// The sled is eligible for conversion.
    Eligible(NoopConvertSledEligible),
}

impl NoopConvertSledStatus {
    fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::Ineligible(reason) => {
                // The slog macros require that the log level is determined at
                // compile time, but we want the different enum variants here to
                // be logged at different levels. Hence this mess.
                match reason {
                    NoopConvertSledIneligibleReason::NotInInventory
                    | NoopConvertSledIneligibleReason::MupdateOverride {
                        ..
                    } => {
                        info!(
                            log,
                            "skipped noop image source check on sled";
                            "reason" => %reason,
                        )
                    }
                    NoopConvertSledIneligibleReason::ManifestError {
                        ..
                    }
                    | NoopConvertSledIneligibleReason::MupdateOverrideError {
                        ..
                    } => {
                        warn!(
                            log,
                            "skipped noop image source check on sled";
                            "reason" => %reason,
                        )
                    }
                }
            }
            Self::Eligible(sled) => {
                sled.log_to(log);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NoopConvertSledEligible {
    pub(crate) zones: IdOrdMap<NoopConvertZoneInfo>,
    pub(crate) host_phase_2: NoopConvertHostPhase2Slots,
    pub(crate) measurements: NoopConvertMeasurements,
}

impl NoopConvertSledEligible {
    pub(crate) fn zone_counts(&self) -> NoopConvertZoneCounts {
        NoopConvertZoneCounts::new(&self.zones)
    }

    fn log_to(&self, log: &slog::Logger) {
        let zone_counts = self.zone_counts();

        info!(
            log,
            "performed noop zone image source checks on sled";
            "num_total" => zone_counts.num_total,
            "num_already_artifact" => zone_counts.num_already_artifact,
            // Since mupdate_override_id is None, maybe-eligible zones are
            // truly eligible.
            "num_eligible" => zone_counts.num_eligible,
            "num_ineligible" => zone_counts.num_ineligible,
        );

        for zone in &self.zones {
            zone.log_to(log);
        }

        self.host_phase_2.log_to(log);
        self.measurements.log_to(log);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NoopConvertZoneCounts {
    pub(crate) num_total: usize,
    pub(crate) num_already_artifact: usize,
    pub(crate) num_eligible: usize,
    pub(crate) num_ineligible: usize,
}

impl NoopConvertZoneCounts {
    pub(crate) fn new(zones: &IdOrdMap<NoopConvertZoneInfo>) -> Self {
        let mut num_already_artifact = 0;
        let mut num_eligible = 0;
        let mut num_ineligible = 0;

        for zone in zones {
            match &zone.status {
                NoopConvertZoneStatus::AlreadyArtifact { .. } => {
                    num_already_artifact += 1;
                }
                NoopConvertZoneStatus::Eligible(_) => {
                    num_eligible += 1;
                }
                NoopConvertZoneStatus::Ineligible(_) => {
                    num_ineligible += 1;
                }
            }
        }

        Self {
            num_total: zones.len(),
            num_already_artifact,
            num_eligible,
            num_ineligible,
        }
    }

    pub(crate) fn num_install_dataset(&self) -> usize {
        self.num_eligible + self.num_ineligible
    }
}

/// The reason a sled is ineligible for noop image source conversions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertSledIneligibleReason {
    /// This sled is missing from inventory.
    NotInInventory,

    /// An error occurred retrieving the sled's install dataset zone manifest.
    ManifestError { message: String },

    /// The `remove_mupdate_override` field is set for this sled in the
    /// blueprint.
    MupdateOverride {
        /// The override ID.
        mupdate_override_id: MupdateOverrideUuid,

        /// Information about zones.
        ///
        /// If the mupdate override is changed, a sled can transition from
        /// ineligible to eligible, or vice versa. We build and retain the zone
        /// map for easy state transitions.
        zones: IdOrdMap<NoopConvertZoneInfo>,

        /// Information about host phase 2.
        ///
        /// Stored for reasons similar to `zones` above.
        host_phase_2: Box<NoopConvertHostPhase2Slots>,

        /// Information about measurements
        measurements: NoopConvertMeasurements,
    },

    /// An error was obtained while retrieving mupdate override information.
    ///
    /// In this case, the system is in an indeterminate state: we do not alter
    /// zone image sources in any way.
    MupdateOverrideError {
        /// The error message.
        message: String,
    },
}

impl fmt::Display for NoopConvertSledIneligibleReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInInventory => write!(f, "sled not found in inventory"),
            Self::ManifestError { message } => {
                write!(f, "error retrieving zone manifest: {}", message)
            }
            Self::MupdateOverride { mupdate_override_id, .. } => {
                write!(
                    f,
                    "remove_mupdate_override is set in the blueprint \
                     ({mupdate_override_id})",
                )
            }
            Self::MupdateOverrideError { message } => {
                write!(
                    f,
                    "error retrieving mupdate override information: {}",
                    message
                )
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NoopConvertZoneInfo {
    pub(crate) zone_id: OmicronZoneUuid,
    pub(crate) kind: ZoneKind,
    pub(crate) status: NoopConvertZoneStatus,
}

impl NoopConvertZoneInfo {
    fn new(
        zone: &BlueprintZoneConfig,
        zone_manifest: &ZoneManifestBootInventory,
        artifacts_by_hash: &HashMap<ArtifactHash, &TufArtifactMeta>,
    ) -> Self {
        let file_name = zone.kind().artifact_in_install_dataset();

        match &zone.image_source {
            BlueprintZoneImageSource::InstallDataset => {}
            BlueprintZoneImageSource::Artifact { version, hash } => {
                return NoopConvertZoneInfo {
                    zone_id: zone.id,
                    kind: zone.kind(),
                    status: NoopConvertZoneStatus::AlreadyArtifact {
                        version: version.clone(),
                        hash: *hash,
                    },
                };
            }
        }

        let Some(artifact) = zone_manifest.artifacts.get(file_name) else {
            return NoopConvertZoneInfo {
                zone_id: zone.id,
                kind: zone.kind(),
                status: NoopConvertZoneStatus::Ineligible(
                    NoopConvertZoneIneligibleReason::NotInManifest,
                ),
            };
        };
        if let Err(message) = &artifact.status {
            // The artifact is somehow invalid and corrupt.
            return NoopConvertZoneInfo {
                zone_id: zone.id,
                kind: zone.kind(),
                status: NoopConvertZoneStatus::Ineligible(
                    NoopConvertZoneIneligibleReason::ArtifactError {
                        message: message.to_owned(),
                    },
                ),
            };
        }

        // Does the hash match what's in the TUF repo?
        let Some(&tuf_artifact) =
            artifacts_by_hash.get(&artifact.expected_hash)
        else {
            return NoopConvertZoneInfo {
                zone_id: zone.id,
                kind: zone.kind(),
                status: NoopConvertZoneStatus::Ineligible(
                    NoopConvertZoneIneligibleReason::NotInTufRepo {
                        expected_hash: artifact.expected_hash,
                    },
                ),
            };
        };

        NoopConvertZoneInfo {
            zone_id: zone.id,
            kind: zone.kind(),
            status: NoopConvertZoneStatus::Eligible(
                BlueprintZoneImageSource::from_available_artifact(tuf_artifact),
            ),
        }
    }

    fn log_to(&self, log: &slog::Logger) {
        let log = log.new(o!(
            "zone_id" => self.zone_id.to_string(),
            "kind" => self.kind.report_str(),
            "file_name" => self.kind.artifact_in_install_dataset(),
        ));
        match &self.status {
            NoopConvertZoneStatus::AlreadyArtifact { version, hash } => {
                // Use debug to avoid spamming reconfigurator-cli output for
                // this generally expected case.
                debug!(
                    log,
                    "zone has its image source set to Artifact already";
                    "version" => %version,
                    "hash" => %hash,
                );
            }
            NoopConvertZoneStatus::Eligible(new_image_source) => {
                debug!(
                    log,
                    "zone may be eligible for noop image source conversion";
                    "new_image_source" => %new_image_source,
                );
            }
            NoopConvertZoneStatus::Ineligible(
                NoopConvertZoneIneligibleReason::NotInManifest,
            ) => {
                // This case shouldn't generally happen in production, but it
                // can currently occur in the reconfigurator-cli since our
                // simulated systems don't have a zone manifest without them
                // being initialized. Log this at the DEBUG level to avoid
                // spamming reconfigurator-cli output.
                debug!(
                    log,
                    "blueprint zone not found in zone manifest, \
                     ignoring for noop checks (how is the zone set to \
                     InstallDataset in the blueprint then?)",
                );
            }
            NoopConvertZoneStatus::Ineligible(
                NoopConvertZoneIneligibleReason::ArtifactError { message },
            ) => {
                warn!(
                    log,
                    "zone manifest inventory indicated install dataset \
                     artifact is invalid, not using artifact (this is \
                     abnormal)";
                    "message" => %message,
                );
            }
            NoopConvertZoneStatus::Ineligible(
                NoopConvertZoneIneligibleReason::NotInTufRepo { expected_hash },
            ) => {
                // If a MUPdate happens, sleds should all be MUPdated to the
                // same version, so the TUF repo is expected to contain all the
                // hashes. The only time that isn't the case is right after a
                // MUPdate when the TUF repo hasn't been uploaded yet. This
                // isn't quite a warning or error case, so log this at the INFO
                // level.
                info!(
                    log,
                    "install dataset artifact hash not found in TUF repo, \
                     ignoring for noop checks";
                    "expected_hash" => %expected_hash,
                );
            }
        }
    }
}

impl IdOrdItem for NoopConvertZoneInfo {
    type Key<'a> = OmicronZoneUuid;

    fn key(&self) -> Self::Key<'_> {
        self.zone_id
    }

    id_upcast!();
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertZoneStatus {
    AlreadyArtifact { version: BlueprintArtifactVersion, hash: ArtifactHash },
    Ineligible(NoopConvertZoneIneligibleReason),
    Eligible(BlueprintZoneImageSource),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertZoneIneligibleReason {
    NotInManifest,
    ArtifactError { message: String },
    NotInTufRepo { expected_hash: ArtifactHash },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NoopConvertHostPhase2Slots {
    pub(crate) slot_a: NoopConvertHostPhase2Contents,
    pub(crate) slot_b: NoopConvertHostPhase2Contents,
}

impl NoopConvertHostPhase2Slots {
    fn new(
        current: BlueprintHostPhase2DesiredSlots,
        contents: Option<&BootPartitionContents>,
        artifacts_by_hash: &HashMap<ArtifactHash, &TufArtifactMeta>,
    ) -> Self {
        Self {
            slot_a: NoopConvertHostPhase2Contents::new(
                current.slot_a,
                contents.map(|c| &c.slot_a),
                artifacts_by_hash,
            ),
            slot_b: NoopConvertHostPhase2Contents::new(
                current.slot_b,
                contents.map(|c| &c.slot_b),
                artifacts_by_hash,
            ),
        }
    }

    /// Returns true if both slots are already set to Artifact.
    pub(crate) fn both_already_artifact(&self) -> bool {
        self.slot_a.is_already_artifact() && self.slot_b.is_already_artifact()
    }

    fn log_to(&self, log: &slog::Logger) {
        {
            let log = log.new(o!("slot" => "a"));
            self.slot_a.log_to(&log);
        }
        {
            let log = log.new(o!("slot" => "b"));
            self.slot_b.log_to(&log);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertHostPhase2Contents {
    AlreadyArtifact { version: BlueprintArtifactVersion, hash: ArtifactHash },
    Ineligible(NoopConvertHostPhase2IneligibleReason),
    Eligible(BlueprintHostPhase2DesiredContents),
}

impl NoopConvertHostPhase2Contents {
    fn new(
        current: BlueprintHostPhase2DesiredContents,
        details: Option<&Result<BootPartitionDetails, String>>,
        artifacts_by_hash: &HashMap<ArtifactHash, &TufArtifactMeta>,
    ) -> Self {
        match current {
            BlueprintHostPhase2DesiredContents::Artifact { version, hash } => {
                // The desired contents are already set to Artifact, so no
                // changes need to be made.
                Self::AlreadyArtifact { version, hash }
            }
            BlueprintHostPhase2DesiredContents::CurrentContents => {
                // Do the boot partition contents reported by inventory match
                // what's in the TUF repo?
                match details {
                    Some(Ok(details)) => {
                        // Use details.artifact_hash (which matches what's in
                        // the TUF repo), NOT details.header.sha256 (which is
                        // the hash of what comes after the header).
                        if let Some(meta) =
                            artifacts_by_hash.get(&details.artifact_hash)
                        {
                            let version = BlueprintArtifactVersion::Available {
                                version: meta.id.version.clone(),
                            };
                            let desired =
                                BlueprintHostPhase2DesiredContents::Artifact {
                                    version,
                                    hash: meta.hash,
                                };
                            Self::Eligible(desired)
                        } else {
                            Self::Ineligible(
                                NoopConvertHostPhase2IneligibleReason::NotInTufRepo {
                                    expected_hash: details.artifact_hash,
                                }
                            )
                        }
                    }
                    Some(Err(error)) => Self::Ineligible(
                        NoopConvertHostPhase2IneligibleReason::InventoryError {
                            message: error.to_string(),
                        },
                    ),
                    None => Self::Ineligible(
                        NoopConvertHostPhase2IneligibleReason::NoInventory,
                    ),
                }
            }
        }
    }

    pub(crate) fn is_already_artifact(&self) -> bool {
        matches!(self, Self::AlreadyArtifact { .. })
    }

    pub(crate) fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible(_))
    }

    fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::AlreadyArtifact { version, hash } => {
                // Use debug to avoid spamming reconfigurator-cli output for
                // this generally expected case.
                debug!(
                    log,
                    "host phase 2 desired contents set to Artifact already";
                    "version" => %version,
                    "hash" => %hash,
                );
            }
            Self::Eligible(new_desired_contents) => {
                debug!(
                    log,
                    "host phase 2 desired contents may be eligible \
                     for noop image source conversion";
                    "new_desired_contents" => %new_desired_contents,
                );
            }
            Self::Ineligible(
                NoopConvertHostPhase2IneligibleReason::NoInventory,
            ) => {
                // This can happen at the very beginning of startup before the
                // reconciler is first run.
                warn!(
                    log,
                    "no BootPartitionDetails information available in inventory, \
                     cannot perform noop checks",
                );
            }
            Self::Ineligible(
                NoopConvertHostPhase2IneligibleReason::InventoryError {
                    message,
                },
            ) => {
                warn!(
                    log,
                    "BootPartitionDetails inventory reported error, \
                     cannot perform noop checks";
                    "message" => %message,
                );
            }
            Self::Ineligible(
                NoopConvertHostPhase2IneligibleReason::NotInTufRepo {
                    expected_hash,
                },
            ) => {
                // If a MUPdate happens, sleds should all be MUPdated to the
                // same version, so the TUF repo is expected to contain all the
                // hashes. This isn't the case:
                // * right after a MUPdate when the TUF repo hasn't been
                //   uploaded yet
                // * potentially on the non-boot disk?
                //
                // This isn't quite a warning or error case, so log this at the
                // INFO level.
                info!(
                    log,
                    "BootPartitionDetails inventory hash not found in TUF repo, \
                     ignoring for noop checks";
                    "expected_hash" => %expected_hash,
                );
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertHostPhase2IneligibleReason {
    /// Inventory was missing information about host phase 2 contents.
    NoInventory,
    /// Inventory reported an error while fetching host phase 2 contents.
    InventoryError { message: String },
    /// An artifact matching the host phase 2 contents was not found in the TUF
    /// repository.
    NotInTufRepo { expected_hash: ArtifactHash },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertMeasurementContents {
    AlreadyArtifact { artifacts: BTreeSet<BlueprintSingleMeasurement> },
    Ineligible(NoopConvertMeasurementsIneligibleReason),
    Eligible(BlueprintMeasurementSetDesiredContents),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NoopConvertMeasurementsIneligibleReason {
    //NotInManifest,
    //ArtifactError { message: String },
    NotInTufRepo { expected_hash: ArtifactHash },
}

impl NoopConvertMeasurementContents {
    fn new(
        current: BlueprintMeasurementSetDesiredContents,
        measurement_manifest: &MeasurementManifestBootInventory,
        artifacts_by_hash: &HashMap<ArtifactHash, &TufArtifactMeta>,
    ) -> Self {
        match current {
            BlueprintMeasurementSetDesiredContents::Artifacts { artifacts } => {
                Self::AlreadyArtifact { artifacts }
            }
            BlueprintMeasurementSetDesiredContents::InstallDataset => {
                let mut artifacts = BTreeSet::new();
                for artifact in &measurement_manifest.artifacts {
                    if let Some(meta) =
                        artifacts_by_hash.get(&artifact.expected_hash)
                    {
                        artifacts.insert(BlueprintSingleMeasurement {
                            version: BlueprintArtifactVersion::Available {
                                version: meta.id.version.clone(),
                            },
                            hash: meta.hash,
                            // Noooo don't get rid of the install dataset
                            prune: false,
                        });
                    } else {
                        return Self::Ineligible(
                            NoopConvertMeasurementsIneligibleReason::NotInTufRepo { expected_hash: artifact.expected_hash }
                        );
                    }
                }
                Self::Eligible(
                    BlueprintMeasurementSetDesiredContents::Artifacts {
                        artifacts,
                    },
                )
            }
        }
    }

    pub(crate) fn is_already_artifact(&self) -> bool {
        matches!(self, Self::AlreadyArtifact { .. })
    }

    pub(crate) fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible(_))
    }

    fn log_to(&self, log: &slog::Logger) {
        match self {
            NoopConvertMeasurementContents::AlreadyArtifact {
                artifacts: _,
            } => {
                // Use debug to avoid spamming reconfigurator-cli output for
                // this generally expected case.
                debug!(
                    log,
                    "measurement has its image source set to Artifact already";
                    //"artifacts" => %artifacts,
                    //"version" => %version,
                    //"hash" => %hash,
                );
            }
            NoopConvertMeasurementContents::Eligible(new_image_source) => {
                debug!(
                    log,
                    "measurement may be eligible for noop image source conversion";
                    // XXX FIXME????
                    "new_image_source" => %new_image_source,
                );
            }
            NoopConvertMeasurementContents::Ineligible(
                NoopConvertMeasurementsIneligibleReason::NotInTufRepo {
                    expected_hash,
                },
            ) => {
                // If a MUPdate happens, sleds should all be MUPdated to the
                // same version, so the TUF repo is expected to contain all the
                // hashes. The only time that isn't the case is right after a
                // MUPdate when the TUF repo hasn't been uploaded yet. This
                // isn't quite a warning or error case, so log this at the INFO
                // level.
                info!(
                    log,
                    "install dataset artifact hash not found in TUF repo, \
                     ignoring for noop checks";
                    "expected_hash" => %expected_hash,
                );
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NoopConvertMeasurements {
    pub(crate) measurements: NoopConvertMeasurementContents,
}

impl NoopConvertMeasurements {
    fn new(
        measurement: BlueprintMeasurementsDesiredContents,
        measurement_manifest: &MeasurementManifestBootInventory,
        artifacts_by_hash: &HashMap<ArtifactHash, &TufArtifactMeta>,
    ) -> Self {
        Self {
            measurements: NoopConvertMeasurementContents::new(
                measurement.measurements.into(),
                measurement_manifest,
                artifacts_by_hash,
            ),
        }
    }

    /// Returns true if both slots are already set to Artifact.
    pub(crate) fn both_already_artifact(&self) -> bool {
            self.measurements.is_already_artifact()
    }

    fn log_to(&self, log: &slog::Logger) {
        {
            let log = log.new(o!("set" => "current"));
            self.measurements.log_to(&log);
        }
    }
}
