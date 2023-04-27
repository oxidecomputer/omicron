// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use tui::style::Style;
use wicket_common::update_events::{
    EventReport, ProgressEventKind, StepEventKind, UpdateComponent,
    UpdateStepId,
};

use crate::ui::defaults::style;

use super::{ComponentId, ParsableComponentId, ALL_COMPONENT_IDS};
use omicron_common::api::internal::nexus::KnownArtifactKind;
use serde::{Deserialize, Serialize};
use slog::{warn, Logger};
use std::collections::BTreeMap;
use std::fmt::Display;
use wicketd_client::types::{ArtifactId, EventReportAll, SemverVersion};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackUpdateState {
    pub items: BTreeMap<ComponentId, BTreeMap<UpdateComponent, UpdateState>>,
    pub system_version: Option<SemverVersion>,
    pub artifacts: Vec<ArtifactId>,
    pub artifact_versions: BTreeMap<KnownArtifactKind, SemverVersion>,
    pub event_reports: BTreeMap<ComponentId, EventReport>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            system_version: None,
            items: ALL_COMPONENT_IDS
                .iter()
                .map(|id| match id {
                    ComponentId::Sled(_) => (
                        *id,
                        BTreeMap::from([
                            (UpdateComponent::Host, UpdateState::Waiting),
                            (UpdateComponent::Rot, UpdateState::Waiting),
                            (UpdateComponent::Sp, UpdateState::Waiting),
                        ]),
                    ),
                    ComponentId::Switch(_) => (
                        *id,
                        BTreeMap::from([
                            (UpdateComponent::Rot, UpdateState::Waiting),
                            (UpdateComponent::Sp, UpdateState::Waiting),
                        ]),
                    ),
                    ComponentId::Psc(_) => (
                        *id,
                        BTreeMap::from([
                            (UpdateComponent::Rot, UpdateState::Waiting),
                            (UpdateComponent::Sp, UpdateState::Waiting),
                        ]),
                    ),
                })
                .collect(),
            artifacts: vec![],
            artifact_versions: BTreeMap::default(),
            event_reports: BTreeMap::default(),
        }
    }

    pub fn update_logs(&mut self, logger: &Logger, reports: EventReportAll) {
        for (sp_type, logs) in reports.sps {
            for (i, log) in logs {
                let Ok(id) = ComponentId::try_from(ParsableComponentId {
                    sp_type: &sp_type,
                    i: &i,
                }) else {
                    warn!(logger, "Invalid ComponentId in EventReport: {} {}", &sp_type, &i);
                    continue;
                };
                self.update_items(&id, &log);
                self.event_reports.insert(id, log);
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
    pub fn update_items(
        &mut self,
        id: &ComponentId,
        event_report: &EventReport,
    ) {
        let items = self.items.get_mut(id).unwrap();
        if event_report.step_events.is_empty() {
            // Reset all items to default
            for (_, state) in items.iter_mut() {
                *state = UpdateState::Waiting;
            }
        }

        // Mark artifacts as either 'succeeded' or `failed' by looking in
        // the event log.
        for event in &event_report.step_events {
            match &event.kind {
                StepEventKind::NoStepsDefined
                | StepEventKind::ExecutionStarted { .. }
                | StepEventKind::ProgressReset { .. }
                | StepEventKind::AttemptRetry { .. }
                | StepEventKind::ExecutionCompleted { .. }
                | StepEventKind::Nested { .. }
                | StepEventKind::Unknown => (),

                StepEventKind::StepCompleted { step, .. } => {
                    let updated_component = match step.info.id {
                        UpdateStepId::ResettingSp => Some(UpdateComponent::Sp),
                        UpdateStepId::RunningInstallinator => {
                            Some(UpdateComponent::Host)
                        }
                        // TODO how do we know when the RoT update is done?
                        // (Maybe need a `ResettingRot` step id?)
                        _ => None,
                    };
                    update_component_state(
                        items,
                        updated_component,
                        UpdateState::Updated,
                    );
                }
                StepEventKind::ExecutionFailed { failed_step, .. } => {
                    update_component_state(
                        items,
                        Some(failed_step.info.component),
                        UpdateState::Failed,
                    );
                }
            }
        }

        // Mark any known artifacts as updating
        for progress_event in &event_report.progress_events {
            let component = match &progress_event.kind {
                ProgressEventKind::WaitingForProgress { step, .. }
                | ProgressEventKind::Progress { step, .. }
                | ProgressEventKind::Nested { step, .. } => {
                    Some(step.info.component)
                }
                ProgressEventKind::Unknown => None,
            };
            update_component_state(items, component, UpdateState::Updating);
        }
    }
}

// For a given Component's artifacts, update it's state
// to reflect what is currently known from the returned log.
fn update_component_state(
    items: &mut BTreeMap<UpdateComponent, UpdateState>,
    component: Option<UpdateComponent>,
    new_state: UpdateState,
) {
    if let Some(component) = &component {
        if let Some(state) = items.get_mut(component) {
            *state = new_state;
        }
    }
}

#[allow(unused)]
pub fn update_component_title(component: UpdateComponent) -> &'static str {
    match component {
        UpdateComponent::Rot => "ROT",
        UpdateComponent::Sp => "SP",
        UpdateComponent::Host => "HOST",
    }
}
