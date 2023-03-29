// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use tui::style::Style;

use crate::ui::defaults::style;

use super::{ComponentId, ParsableComponentId, ALL_COMPONENT_IDS};
use omicron_common::api::internal::nexus::KnownArtifactKind;
use serde::{Deserialize, Serialize};
use slog::{warn, Logger};
use std::collections::BTreeMap;
use std::fmt::Display;
use wicketd_client::types::{
    ArtifactId, SemverVersion, UpdateEventKind, UpdateLog, UpdateLogAll,
    UpdateNormalEventKind, UpdateStateKind, UpdateTerminalEventKind, HostPhase2Progress,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackUpdateState {
    pub items: BTreeMap<ComponentId, BTreeMap<KnownArtifactKind, UpdateState>>,
    pub system_version: Option<SemverVersion>,
    pub artifacts: Vec<ArtifactId>,
    pub artifact_versions: BTreeMap<KnownArtifactKind, SemverVersion>,
    pub logs: BTreeMap<ComponentId, UpdateLog>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateState {
    Waiting,
    Preparing(Option<UpdateProgress>),
    Downloading(Option<UpdateProgress>),
    Updating(Option<UpdateProgress>),
    Resetting,
    Updated,
    Failed,
}

impl Display for UpdateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateState::Waiting => write!(f, "WAITING"),
            UpdateState::Updated => write!(f, "UPDATED"),
            UpdateState::Preparing(progress) => {
                if let Some(progress) = progress {
                    write!(f, "PREPARING ({progress})")
                } else {
                    write!(f, "PREPARING")
                }
            }
            UpdateState::Downloading(progress) => {
                if let Some(progress) = progress {
                    write!(f, "DOWNLOADING ({progress})")
                } else {
                    write!(f, "DOWNLOADING")
                }
            }
            UpdateState::Updating(progress) => {
                if let Some(progress) = progress {
                    write!(f, "UPDATING ({progress})")
                } else {
                    write!(f, "UPDATING")
                }
            }
            UpdateState::Resetting => {
                write!(f, "RESETTING")
            }
            UpdateState::Failed => write!(f, "FAILED"),
        }
    }
}

impl UpdateState {
    pub fn style(&self) -> Style {
        match self {
            UpdateState::Waiting => style::deselected(),
            UpdateState::Updated => style::successful_update(),
            UpdateState::Preparing(_)
            | UpdateState::Downloading(_)
            | UpdateState::Updating(_)
            | UpdateState::Resetting => style::start_update(),
            UpdateState::Failed => style::failed_update(),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct UpdateProgress {
    current: u64,
    total: u64,
}

impl UpdateProgress {
    fn new(current: u64, total: u64) -> Option<Self> {
        // The total should never be zero, but if it is, don't fail -- hide the
        // percentage instead.
        if total == 0 {
            None
        } else {
            // Current should never be higher than total. If it is, clamp it to 100%.
            Some(Self { current: current.min(total), total })
        }
    }
}

impl Display for UpdateProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Show progress as a percentage.
        write!(
            f,
            "{}%",
            ((self.current as f64 / self.total as f64) * 100f64) as u64
        )
    }
}

impl RackUpdateState {
    pub fn new() -> Self {
        RackUpdateState {
            system_version: None,
            items: ALL_COMPONENT_IDS
                .iter()
                .map(|id| match id {
                    ComponentId::Sled(_) => (
                        *id,
                        BTreeMap::from([
                            (KnownArtifactKind::Host, UpdateState::Waiting),
                            (
                                KnownArtifactKind::ControlPlane,
                                UpdateState::Waiting,
                            ),
                            (
                                KnownArtifactKind::GimletRot,
                                UpdateState::Waiting,
                            ),
                            (KnownArtifactKind::GimletSp, UpdateState::Waiting),
                        ]),
                    ),
                    ComponentId::Switch(_) => (
                        *id,
                        BTreeMap::from([
                            (
                                KnownArtifactKind::SwitchRot,
                                UpdateState::Waiting,
                            ),
                            (KnownArtifactKind::SwitchSp, UpdateState::Waiting),
                        ]),
                    ),
                    ComponentId::Psc(_) => (
                        *id,
                        BTreeMap::from([
                            (KnownArtifactKind::PscRot, UpdateState::Waiting),
                            (KnownArtifactKind::PscSp, UpdateState::Waiting),
                        ]),
                    ),
                })
                .collect(),
            artifacts: vec![],
            artifact_versions: BTreeMap::default(),
            logs: BTreeMap::default(),
        }
    }

    pub fn update_logs(&mut self, logger: &Logger, logs: UpdateLogAll) {
        for (sp_type, logs) in logs.sps {
            for (i, log) in logs {
                let Ok(id) = ComponentId::try_from(ParsableComponentId {
                    sp_type: &sp_type,
                    i: &i,
                }) else {
                    warn!(logger, "Invalid ComponentId in UpdateLog: {} {}", &sp_type, &i);
                    continue;
                };
                self.update_items(&id, &log);
                self.logs.insert(id, log);
            }
        }
    }

    pub fn update_artifacts(
        &mut self,
        system_version: Option<SemverVersion>,
        artifacts: Vec<ArtifactId>,
    ) {
        self.system_version = system_version;
        self.artifacts = artifacts;
        self.artifact_versions.clear();
        for id in &mut self.artifacts {
            if let Ok(known) = id.kind.parse() {
                self.artifact_versions.insert(known, id.version.clone());
            }
        }
    }

    /// Scan through `log` and update the components status given by `id`
    pub fn update_items(&mut self, id: &ComponentId, log: &UpdateLog) {
        let items = self.items.get_mut(id).unwrap();
        if log.events.is_empty() {
            // Reset all items to default
            for (_, state) in items.iter_mut() {
                *state = UpdateState::Waiting;
            }
        }

        // Mark artifacts as either 'succeeded' or `failed' by looking in
        // the event log.
        for event in &log.events {
            match &event.kind {
                UpdateEventKind::Normal(normal) => match normal {
                    UpdateNormalEventKind::SpResetComplete => {
                        let known = Some(id.sp_known_artifact_kind());
                        update_artifact_state(
                            items,
                            known,
                            UpdateState::Updated,
                        );
                    }
                    UpdateNormalEventKind::ArtifactUpdateComplete {
                        artifact,
                    } => {
                        // We specifically don't want to mark the Host
                        // complete, if the trampoline has completed, so don't
                        // use `artifact_to_known_artifact_kind`
                        let known = artifact.kind.parse().ok();
                        update_artifact_state(
                            items,
                            known,
                            UpdateState::Updated,
                        );
                    }
                    UpdateNormalEventKind::InstallinatorEvent(
                        _completion_event_kind,
                    ) => {

                        // TODO: Do we want to update state
                        // with intermediate events?
                        //
                        // This would be useful for reporting progress
                        // percentages, but that's about it.
                    }
                },
                UpdateEventKind::Terminal(terminal) => match terminal {
                    UpdateTerminalEventKind::SpResetFailed { .. } => {
                        let known = Some(id.sp_known_artifact_kind());
                        update_artifact_state(
                            items,
                            known,
                            UpdateState::Failed,
                        );
                    }
                    UpdateTerminalEventKind::ArtifactUpdateFailed {
                        artifact,
                        ..
                    } => {
                        let known = artifact_to_known_artifact_kind(artifact);
                        update_artifact_state(
                            items,
                            known,
                            UpdateState::Failed,
                        );
                    }
                },
            }
        }

        // Mark any known artifacts as updating
        let Some(state) = &log.current else {
            return;
        };
        use UpdateStateKind::*;
        match &state.kind {
            WaitingForProgress { .. } => {
                // Nothing to do here
            }
            ResettingSp => {
                update_artifact_state(
                    items,
                    Some(id.sp_known_artifact_kind()),
                    UpdateState::Resetting,
                );
            }
            SendingArtifactToMgs { artifact }
            | WaitingForStatus { artifact } => update_artifact_state(
                items,
                artifact_to_known_artifact_kind(&artifact),
                UpdateState::Updating(None),
            ),

            PreparingForArtifact { artifact, progress } => {
                let progress = progress.as_ref().and_then(|p| {
                    UpdateProgress::new(p.current as u64, p.total as u64)
                });
                update_artifact_state(
                    items,
                    artifact_to_known_artifact_kind(&artifact),
                    UpdateState::Preparing(progress),
                )
            }
            ArtifactDownloadProgress {
                kind,
                downloaded_bytes,
                total_bytes,
                ..
            } => {
                if let Some(KnownArtifactKind::Host) = artifact_kind_to_known_kind(kind) {
                    // This is a host artifact -- it is produced by 
                }
                update_artifact_state(
                    items,
                    artifact_kind_to_known_kind(kind),
                    UpdateState::Downloading(UpdateProgress::new(
                        *downloaded_bytes,
                        *total_bytes,
                    )));
            }
            ArtifactWriteProgress {
                kind, written_bytes, total_bytes, ..
            } => update_artifact_state(
                items,
                artifact_kind_to_known_kind(kind),
                UpdateState::Updating(UpdateProgress::new(
                    *written_bytes,
                    *total_bytes,
                )),
            ),
            WaitingForTrampolineImageDelivery { artifact, progress } => {
                match progress {
                    HostPhase2Progress::Available { offset, total_size, .. }
                }
                artifact_to_known_artifact_kind(&artifact)
            }
            SettingHostPowerState { .. }
            | InstallinatorFormatProgress { .. }
            | SettingInstallinatorOptions
            | SettingHostStartupOptions => {
                // Should we bother doing something here?
                None
            }
        };
        update_artifact_state(items, known, UpdateState::Updating);
    }
}

// For a given Component's artifacts, update it's state
// to reflect what is currently known from the returned log.
fn update_artifact_state(
    items: &mut BTreeMap<KnownArtifactKind, UpdateState>,
    known: Option<KnownArtifactKind>,
    new_state: UpdateState,
) {
    if let Some(known) = &known {
        if let Some(state) = items.get_mut(known) {
            *state = new_state;
        }
    }
}

// Take an `ArtifactId` and return a `KnownArtifactKind` if there is one.
//
// We don't expose some `KnownArtifactKind`s like `Trampoline`,
// since those are artifacts of the install process and not relevant
// directly to the customer. For cases, like `Trampoline`, we convert
// to an appropriate `KnownArtiactKind`, like `Host`.
fn artifact_to_known_artifact_kind(
    artifact: &ArtifactId,
) -> Option<KnownArtifactKind> {
    artifact.kind.parse().ok().map(|known| match known {
        KnownArtifactKind::Trampoline => {
            // We don't expose the trampoline to the user
            KnownArtifactKind::Host
        }
        known => known,
    })
}

// Take an `ArtifactKind` and return a `KnownArtifactKind` if there is one.
// We don't expose some `KnownArtifactKind`s like `Trampoline`,
// since those are artifacts of the install process and not relevant
// directly to the customer. For cases, like `Trampoline`, we convert
// to an appropriate `KnownArtiactKind`, like `Host`.
fn artifact_kind_to_known_kind(kind: &String) -> Option<KnownArtifactKind> {
    kind.parse().ok().map(|known| match known {
        KnownArtifactKind::Trampoline => {
            // We don't expose the trampoline to the user
            KnownArtifactKind::Host
        }
        known => known,
    })
}

#[allow(unused)]
pub fn artifact_title(kind: KnownArtifactKind) -> &'static str {
    use KnownArtifactKind::*;
    match kind {
        GimletSp | PscSp | SwitchSp => "SP",
        GimletRot | PscRot | SwitchRot => "ROT",
        Host => "HOST",
        Trampoline => "TRAMPOLINE",
        ControlPlane => "CONTROL PLANE",
    }
}
