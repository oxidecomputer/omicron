// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use tui::style::Style;

use crate::ui::defaults::style;

use super::{ComponentId, ALL_COMPONENT_IDS};
use std::collections::BTreeMap;
use std::fmt::Display;
use wicketd_client::types::ArtifactId;

#[derive(Debug)]
pub struct RackUpdateState {
    pub items:
        BTreeMap<ComponentId, BTreeMap<FinalInstallArtifact, UpdateState>>,
    pub artifacts: Vec<ArtifactId>,
    pub final_artifact_versions: BTreeMap<FinalInstallArtifact, String>,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum FinalInstallArtifact {
    // Sled Artifacts
    GimletSp,
    GimletRot,
    Host,
    ControlPlane,

    // PSC Artifacts
    PscSp,
    PscRot,

    // Switch Artifacts
    SwitchSp,
    SwitchRot,
}

impl Display for FinalInstallArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FinalInstallArtifact::Host => write!(f, "HOST"),
            FinalInstallArtifact::ControlPlane => write!(f, "CONTROL PLANE"),
            FinalInstallArtifact::GimletRot => write!(f, "ROOT OF TRUST"),
            FinalInstallArtifact::GimletSp => write!(f, "SERVICE PROCESSOR"),
            FinalInstallArtifact::PscSp => write!(f, "ROOT OF TRUST"),
            FinalInstallArtifact::PscRot => write!(f, "SERVICE PROCESSOR"),
            FinalInstallArtifact::SwitchSp => write!(f, "ROOT OF TRUST"),
            FinalInstallArtifact::SwitchRot => write!(f, "SERVICE PROCESSOR"),
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
                            (FinalInstallArtifact::Host, UpdateState::Waiting),
                            (
                                FinalInstallArtifact::ControlPlane,
                                UpdateState::Waiting,
                            ),
                            (
                                FinalInstallArtifact::GimletRot,
                                UpdateState::Waiting,
                            ),
                            (
                                FinalInstallArtifact::GimletSp,
                                UpdateState::Waiting,
                            ),
                        ]),
                    ),
                    ComponentId::Switch(_) => (
                        *id,
                        BTreeMap::from([
                            (
                                FinalInstallArtifact::SwitchRot,
                                UpdateState::Waiting,
                            ),
                            (
                                FinalInstallArtifact::SwitchSp,
                                UpdateState::Waiting,
                            ),
                        ]),
                    ),
                    ComponentId::Psc(_) => (
                        *id,
                        BTreeMap::from([
                            (
                                FinalInstallArtifact::PscRot,
                                UpdateState::Waiting,
                            ),
                            (FinalInstallArtifact::PscSp, UpdateState::Waiting),
                        ]),
                    ),
                })
                .collect(),
            artifacts: vec![],
            final_artifact_versions: BTreeMap::default(),
        }
    }

    pub fn update_artifacts(&mut self, artifacts: Vec<ArtifactId>) {
        self.artifacts = artifacts;
        self.final_artifact_versions.clear();
        for id in &mut self.artifacts {
            match id.kind.as_str() {
                "gimlet_sp" => {
                    self.final_artifact_versions.insert(
                        FinalInstallArtifact::GimletSp,
                        id.version.clone(),
                    );
                }
                "gimlet_rot" => {
                    self.final_artifact_versions.insert(
                        FinalInstallArtifact::GimletRot,
                        id.version.clone(),
                    );
                }
                "psc_sp" => {
                    self.final_artifact_versions.insert(
                        FinalInstallArtifact::PscSp,
                        id.version.clone(),
                    );
                }
                "psc_rot" => {
                    self.final_artifact_versions.insert(
                        FinalInstallArtifact::PscRot,
                        id.version.clone(),
                    );
                }
                "switch_sp" => {
                    self.final_artifact_versions.insert(
                        FinalInstallArtifact::SwitchSp,
                        id.version.clone(),
                    );
                }
                "switch_rot" => {
                    self.final_artifact_versions.insert(
                        FinalInstallArtifact::SwitchRot,
                        id.version.clone(),
                    );
                }
                "host" => {
                    self.final_artifact_versions
                        .insert(FinalInstallArtifact::Host, id.version.clone());
                }
                "control_plane" => {
                    self.final_artifact_versions.insert(
                        FinalInstallArtifact::ControlPlane,
                        id.version.clone(),
                    );
                }
                _ => (),
            }
        }
    }
}
