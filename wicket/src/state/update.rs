// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use tui::style::Style;

use crate::ui::defaults::style;

use super::{ComponentId, ALL_COMPONENT_IDS};
use omicron_common::api::internal::nexus::KnownArtifactKind;
use std::collections::BTreeMap;
use std::fmt::Display;
use wicketd_client::types::{
    ArtifactId, SemverVersion, UpdateEventKind, UpdateLog, UpdateLogAll,
    UpdateNormalEventKind, UpdateTerminalEventKind,
};

#[derive(Debug)]
pub struct RackUpdateState {
    pub items: BTreeMap<ComponentId, BTreeMap<KnownArtifactKind, UpdateState>>,
    pub artifacts: Vec<ArtifactId>,
    pub artifact_versions: BTreeMap<KnownArtifactKind, SemverVersion>,
    pub logs: BTreeMap<ComponentId, UpdateLog>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UpdateState {
    Waiting,
    Updated,
    Updating,
    Failed,
}

impl Display for UpdateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateState::Waiting => write!(f, "WAITING"),
            UpdateState::Updated => write!(f, "UPDATED"),
            UpdateState::Updating => write!(f, "UPDATING"),
            UpdateState::Failed => write!(f, "FAILED"),
        }
    }
}

impl UpdateState {
    pub fn style(&self) -> Style {
        match self {
            UpdateState::Waiting => style::deselected(),
            UpdateState::Updated => style::successful_update(),
            UpdateState::Updating => style::start_update(),
            UpdateState::Failed => style::failed_update(),
        }
    }
}

impl RackUpdateState {
    pub fn new() -> Self {
        RackUpdateState {
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

    pub fn update_logs(&mut self, logs: UpdateLogAll) {
        for (sp_type, logs) in logs.sps {
            // TODO: Handle more than sleds
            if sp_type == "sled" {
                for (i, log) in logs {
                    // TODO: Sanity check slot number
                    let id = ComponentId::Sled(i.parse().unwrap());
                    self.update_items(&id, &log);
                    self.logs.insert(id, log);
                }
            } else if sp_type == "switch" {
                for (i, log) in logs {
                    // TODO: Sanity check slot number
                    let id = ComponentId::Sled(i.parse().unwrap());
                    self.update_items(&id, &log);
                    self.logs.insert(id, log);
                }
            } else if sp_type == "power" {
                for (i, log) in logs {
                    // TODO: Sanity check slot number
                    let id = ComponentId::Sled(i.parse().unwrap());
                    self.update_items(&id, &log);
                    self.logs.insert(id, log);
                }
            }
        }
    }

    pub fn update_artifacts(&mut self, artifacts: Vec<ArtifactId>) {
        self.artifacts = artifacts;
        self.artifact_versions.clear();
        for id in &mut self.artifacts {
            if let Ok(known) = id.kind.parse() {
                self.artifact_versions.insert(known, id.version.clone());
            }
        }
    }

    pub fn update_items(&mut self, id: &ComponentId, log: &UpdateLog) {
        let items = self.items.get_mut(id).unwrap();
        if log.events.is_empty() {
            // Reset all items to default
            for (_, state) in items.iter_mut() {
                *state = UpdateState::Waiting;
            }
        }

        // TODO: Deal with log.current

        for event in &log.events {
            match &event.kind {
                UpdateEventKind::Normal(normal) => match normal {
                    UpdateNormalEventKind::SpResetComplete => {
                        items
                            .get_mut(&id.sp_known_artifact_kind())
                            .map(|state| *state = UpdateState::Updated);
                    }
                    UpdateNormalEventKind::ArtifactUpdateComplete {
                        artifact,
                    } => {
                        if let Ok(known) = artifact.kind.parse() {
                            items
                                .get_mut(&known)
                                .map(|state| *state = UpdateState::Updated);
                        }
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
                        items
                            .get_mut(&id.sp_known_artifact_kind())
                            .map(|state| *state = UpdateState::Failed);
                    }
                    UpdateTerminalEventKind::ArtifactUpdateFailed {
                        artifact,
                        ..
                    } => {
                        if let Ok(known) = artifact.kind.parse() {
                            let known = match known {
                                KnownArtifactKind::Trampoline
                                | KnownArtifactKind::Host => {
                                    // We don't expose the trampoline to the user
                                    KnownArtifactKind::Host
                                }
                                known => known,
                            };
                            items
                                .get_mut(&known)
                                .map(|state| *state = UpdateState::Failed);
                        }
                    }
                },
            }
        }
    }
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
