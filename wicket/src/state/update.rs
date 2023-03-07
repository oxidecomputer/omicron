// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use tui::style::Style;

use crate::ui::defaults::style;

use super::{ComponentId, ALL_COMPONENT_IDS};
use omicron_common::api::internal::nexus::KnownArtifactKind;
use std::collections::BTreeMap;
use std::fmt::Display;
use wicketd_client::types::ArtifactId;

#[derive(Debug)]
pub struct RackUpdateState {
    pub items: BTreeMap<ComponentId, BTreeMap<KnownArtifactKind, UpdateState>>,
    pub artifacts: Vec<ArtifactId>,
    pub artifact_versions: BTreeMap<KnownArtifactKind, String>,
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
}

#[allow(unused)]
pub fn artifact_title(kind: KnownArtifactKind) -> &'static str {
    type K = KnownArtifactKind;
    match kind {
        K::GimletSp | K::PscSp | K::SwitchSp => "SP",
        K::GimletRot | K::PscRot | K::SwitchRot => "ROT",
        K::Host => "HOST",
        K::Trampoline => "TRAMPOLINE",
        K::ControlPlane => "CONTROL PLANE",
    }
}
