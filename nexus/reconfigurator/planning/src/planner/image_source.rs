// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

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
    GlobalIneligible { reason: NoopConvertGlobalIneligibleReason },

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
            return Ok(Self::GlobalIneligible {
                reason: NoopConvertGlobalIneligibleReason::NoTufRepo,
            });
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

            // Does the blueprint have the remove_mupdate_override field set for
            // this sled? If it does, we don't want to touch the zones on this
            // sled (they should all be InstallDataset until the
            // remove_mupdate_override field is cleared).
            if let Some(override_id) =
                blueprint.sled_get_remove_mupdate_override(sled_id)?
            {
                sleds
                    .insert_unique(NoopConvertSledInfo {
                        sled_id,
                        status: NoopConvertSledStatus::Ineligible(
                            NoopConvertSledIneligibleReason::BpMupdateOverride {
                                override_id,
                            },
                        ),
                    })
                    .expect("sled IDs are unique");
                continue;
            }

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
                        // The blueprint indicates that a zone should be present
                        // that isn't in the install dataset. This might be an old
                        // install dataset with a zone kind known to this version of
                        // Nexus that isn't present in it.
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
                    status: NoopConvertSledStatus::MaybeEligible {
                        mupdate_override_id: blueprint
                            .sled_get_remove_mupdate_override(sled_id)?,
                        zones,
                    },
                })
                .expect("sled IDs are unique");
        }

        Ok(Self::GlobalEligible { sleds })
    }

    pub(crate) fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::GlobalIneligible {
                reason: NoopConvertGlobalIneligibleReason::NoTufRepo,
            } => {
                info!(
                    log,
                    "skipping noop image source check for all sleds \
                     (no current TUF repo)",
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
    /// No TUF repository is available.
    NoTufRepo,
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
    MaybeEligible {
        mupdate_override_id: Option<MupdateOverrideUuid>,
        zones: IdOrdMap<NoopConvertZoneInfo>,
    },
}

impl NoopConvertSledStatus {
    fn log_to(&self, log: &slog::Logger) {
        match self {
            Self::Ineligible(
                NoopConvertSledIneligibleReason::NotInInventory,
            ) => {
                info!(
                    log,
                    "skipped noop image source check \
                     (sled not present in latest inventory collection)"
                );
            }
            Self::Ineligible(
                NoopConvertSledIneligibleReason::ManifestError { message },
            ) => {
                // This is a string so we don't use InlineErrorChain::new.
                let message: &str = message;
                warn!(
                    log,
                    "skipped noop image source check since \
                     sled-agent encountered error retrieving zone manifest \
                     (this is abnormal)";
                    "error" => %message,
                );
            }
            Self::Ineligible(
                NoopConvertSledIneligibleReason::BpMupdateOverride {
                    override_id,
                },
            ) => {
                info!(
                    log,
                    "skipped noop image source check on sled \
                     (blueprint has get_remove_mupdate_override set for sled)";
                    "bp_remove_mupdate_override_id" => %override_id,
                );
            }
            Self::MaybeEligible { mupdate_override_id, zones } => {
                let zone_counts = NoopConvertZoneCounts::new(zones);

                if let Some(override_id) = mupdate_override_id {
                    info!(
                        log,
                        "performed noop image source checks on sled \
                         (maybe_eligible_zones will become eligible if \
                         mupdate_override_id is cleared in a planning step)";
                        "mupdate_override_id" => %override_id,
                        "num_total" => zone_counts.num_total,
                        "num_already_artifact" => zone_counts.num_already_artifact,
                        "num_maybe_eligible" => zone_counts.num_maybe_eligible,
                        "num_ineligible" => zone_counts.num_ineligible,
                    );
                } else {
                    info!(
                        log,
                        "performed noop image source checks on sled";
                        "num_total" => zone_counts.num_total,
                        "num_already_artifact" => zone_counts.num_already_artifact,
                        // Since mupdate_override_id is None, eligible zones are
                        // truly eligible.
                        "num_eligible" => zone_counts.num_maybe_eligible,
                        "num_ineligible" => zone_counts.num_ineligible,
                    );
                }

                for zone in zones {
                    zone.log_to(log);
                }
            }
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
    pub(crate) fn new(zones: &IdOrdMap<NoopConvertZoneInfo>) -> Self {
        let mut num_already_artifact = 0;
        let mut num_maybe_eligible = 0;
        let mut num_ineligible = 0;

        for zone in zones {
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

        Self {
            num_total: zones.len(),
            num_already_artifact,
            num_maybe_eligible,
            num_ineligible,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum NoopConvertSledIneligibleReason {
    /// This sled is missing from inventory.
    NotInInventory,

    /// An error occurred retrieving the sled's install dataset zone manifest.
    ManifestError { message: String },

    /// The blueprint for this sled has remove_mupdate_override set.
    BpMupdateOverride { override_id: MupdateOverrideUuid },
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
                info!(
                    log,
                    "zone is eligible for noop image source conversion";
                    "new_image_source" => %new_image_source,
                );
            }
            NoopConvertZoneStatus::Ineligible(
                NoopConvertZoneIneligibleReason::NotInManifest,
            ) => {
                // This case shouldn't generally happen, but it can currently
                // happen with rkadm, as well as in the reconfigurator-cli. Mark
                // it as debug to avoid spamming reconfigurator-cli output.
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
                // Sleds should all be MUPdated to the same version, so the TUF
                // repo is expected to contain all the hashes. The only time
                // that isn't the case is right after a MUPdate when the TUF
                // repo hasn't been uploaded yet. This isn't quite a warning or
                // error case, so log this at INFO level.
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
