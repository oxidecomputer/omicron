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
    Sp,
    RoT,
    Host,
    ControlPlane,
}

impl Display for FinalInstallArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FinalInstallArtifact::Host => write!(f, "HOST"),
            FinalInstallArtifact::ControlPlane => write!(f, "CONTROL PLANE"),
            FinalInstallArtifact::RoT => write!(f, "ROOT OF TRUST"),
            FinalInstallArtifact::Sp => write!(f, "SERVICE PROCESSOR"),
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
                            (FinalInstallArtifact::RoT, UpdateState::Waiting),
                            (FinalInstallArtifact::Sp, UpdateState::Waiting),
                        ]),
                    ),
                    ComponentId::Switch(_) | ComponentId::Psc(_) => (
                        *id,
                        BTreeMap::from([
                            (FinalInstallArtifact::RoT, UpdateState::Waiting),
                            (FinalInstallArtifact::Sp, UpdateState::Waiting),
                        ]),
                    ),
                })
                .collect(),
            artifacts: vec![],
        }
    }
}
