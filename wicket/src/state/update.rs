// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{ComponentId, ALL_COMPONENT_IDS};
use std::collections::BTreeMap;
use std::fmt::Display;

pub struct RackUpdateState {
    items: BTreeMap<ComponentId, BTreeMap<FinalInstallArtifact, UpdateState>>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UpdateState {
    Waiting,
    Updated,
    Updating,
    Failed,
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
            FinalInstallArtifact::RoT => write!(f, "RoT"),
            FinalInstallArtifact::Sp => write!(f, "Sp"),
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
        }
    }
}
