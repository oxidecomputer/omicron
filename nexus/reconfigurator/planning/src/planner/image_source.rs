// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::HashMap, fmt};

use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::{
    deployment::{
        BlueprintZoneDisposition, BlueprintZoneImageSource,
        BlueprintZoneImageVersion, PlanningInput, SledFilter,
        TargetReleaseDescription,
    },
    inventory::Collection,
};
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

            // Out of these, which zones' hashes (as reported in the zone
            // manifest) match the corresponding ones in the TUF repo?
            let zones = blueprint
                .current_sled_zones(
                    sled_id,
                    BlueprintZoneDisposition::is_in_service,
                )
                .map(|z| {
                    let file_name = z.kind().artifact_in_install_dataset();

                    match &z.image_source {
                        BlueprintZoneImageSource::InstallDataset => {}
                        BlueprintZoneImageSource::Artifact {
                            version,
                            hash,
                        } => {
                            return NoopConvertZoneInfo {
                                zone_id: z.id,
                                kind: z.kind(),
                                status:
                                    NoopConvertZoneStatus::AlreadyArtifact {
                                        version: version.clone(),
                                        hash: *hash,
                                    },
                            };
                        }
                    }

                    let Some(artifact) = zone_manifest.artifacts.get(file_name)
                    else {
                        return NoopConvertZoneInfo {
                            zone_id: z.id,
                            kind: z.kind(),
                            status: NoopConvertZoneStatus::Ineligible(
                                NoopConvertZoneIneligibleReason::NotInManifest,
                            ),
                        };
                    };
                    if let Err(message) = &artifact.status {
                        // The artifact is somehow invalid and corrupt.
                        return NoopConvertZoneInfo {
                            zone_id: z.id,
                            kind: z.kind(),
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
                            zone_id: z.id,
                            kind: z.kind(),
                            status: NoopConvertZoneStatus::Ineligible(
                                NoopConvertZoneIneligibleReason::NotInTufRepo {
                                    expected_hash: artifact.expected_hash,
                                },
                            ),
                        };
                    };

                    NoopConvertZoneInfo {
                        zone_id: z.id,
                        kind: z.kind(),
                        status: NoopConvertZoneStatus::Eligible(
                            BlueprintZoneImageSource::from_available_artifact(
                                tuf_artifact,
                            ),
                        ),
                    }
                })
                .collect();

            sleds
                .insert_unique(NoopConvertSledInfo {
                    sled_id,
                    status: NoopConvertSledStatus::MaybeEligible(
                        NoopConvertSledMaybeEligible {
                            mupdate_override_id: blueprint
                                .sled_get_remove_mupdate_override(sled_id)?,
                            zones,
                        },
                    ),
                })
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
}

#[derive(Clone, Debug)]
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

    /// The sled might be eligible for conversion in case `mupdate_override_id`
    /// is `None`.
    MaybeEligible(NoopConvertSledMaybeEligible),
}

impl NoopConvertSledStatus {
    fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::Ineligible(reason) => {
                // The slog macros require that the log level is determined at
                // compile time, but we want the different enum variants here to
                // be logged at different levels. Hence this mess.
                match reason {
                    NoopConvertSledIneligibleReason::NotInInventory => {
                        info!(
                            log,
                            "skipped noop image source check on sled";
                            "reason" => %reason,
                        )
                    }
                    NoopConvertSledIneligibleReason::ManifestError {
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
            Self::MaybeEligible(sled) => {
                sled.log_to(log);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NoopConvertSledMaybeEligible {
    // A sled is eligible for conversion if the mupdate_override_id is None
    //
    // The code is structured in this manner because the planner's mupdate
    // override step requires access to the sled's zones, and is responsible for
    // clearing this field if it also clears the mupdate override in the
    // blueprint.
    pub(crate) mupdate_override_id: Option<MupdateOverrideUuid>,
    pub(crate) zones: IdOrdMap<NoopConvertZoneInfo>,
}

impl NoopConvertSledMaybeEligible {
    pub(crate) fn zone_counts(&self) -> NoopConvertZoneCounts {
        let mut num_already_artifact = 0;
        let mut num_maybe_eligible = 0;
        let mut num_ineligible = 0;

        for zone in &self.zones {
            match &zone.status {
                NoopConvertZoneStatus::AlreadyArtifact { .. } => {
                    num_already_artifact += 1;
                }
                NoopConvertZoneStatus::Eligible(_) => {
                    num_maybe_eligible += 1;
                }
                NoopConvertZoneStatus::Ineligible(_) => {
                    num_ineligible += 1;
                }
            }
        }

        NoopConvertZoneCounts {
            num_total: self.zones.len(),
            num_already_artifact,
            num_maybe_eligible,
            num_ineligible,
        }
    }

    fn log_to(&self, log: &slog::Logger) {
        let zone_counts = self.zone_counts();

        if let Some(override_id) = self.mupdate_override_id {
            info!(
                log,
                "performed noop image source checks on sled, \
                 but no conversions will occur because \
                 remove_mupdate_override is set in the blueprint";
                "mupdate_override_id" => %override_id,
                "num_total" => zone_counts.num_total,
                "num_already_artifact" => zone_counts.num_already_artifact,
                // This counts the number of zones that would be eligible if
                // remove_mupdate_override were not set.
                "num_would_be_eligible" => zone_counts.num_maybe_eligible,
                "num_ineligible" => zone_counts.num_ineligible,
            );
        } else {
            info!(
                log,
                "performed noop image source checks on sled";
                "num_total" => zone_counts.num_total,
                "num_already_artifact" => zone_counts.num_already_artifact,
                // Since mupdate_override_id is None, maybe-eligible zones are
                // truly eligible.
                "num_eligible" => zone_counts.num_maybe_eligible,
                "num_ineligible" => zone_counts.num_ineligible,
            );
        }

        for zone in &self.zones {
            zone.log_to(log);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NoopConvertZoneCounts {
    pub(crate) num_total: usize,
    pub(crate) num_already_artifact: usize,
    pub(crate) num_maybe_eligible: usize,
    pub(crate) num_ineligible: usize,
}

impl NoopConvertZoneCounts {
    pub(crate) fn num_install_dataset(&self) -> usize {
        self.num_maybe_eligible + self.num_ineligible
    }
}

#[derive(Clone, Debug)]
pub(crate) enum NoopConvertSledIneligibleReason {
    /// This sled is missing from inventory.
    NotInInventory,

    /// An error occurred retrieving the sled's install dataset zone manifest.
    ManifestError { message: String },
}

impl fmt::Display for NoopConvertSledIneligibleReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInInventory => write!(f, "sled not found in inventory"),
            Self::ManifestError { message } => {
                write!(f, "error retrieving zone manifest: {}", message)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NoopConvertZoneInfo {
    pub(crate) zone_id: OmicronZoneUuid,
    pub(crate) kind: ZoneKind,
    pub(crate) status: NoopConvertZoneStatus,
}

impl NoopConvertZoneInfo {
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

#[derive(Clone, Debug)]
pub(crate) enum NoopConvertZoneStatus {
    AlreadyArtifact { version: BlueprintZoneImageVersion, hash: ArtifactHash },
    Ineligible(NoopConvertZoneIneligibleReason),
    Eligible(BlueprintZoneImageSource),
}

#[derive(Clone, Debug)]
pub(crate) enum NoopConvertZoneIneligibleReason {
    NotInManifest,
    ArtifactError { message: String },
    NotInTufRepo { expected_hash: ArtifactHash },
}
