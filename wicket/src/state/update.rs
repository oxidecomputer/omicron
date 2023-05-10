// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use tui::style::Style;
use wicket_common::update_events::{
    EventReport, ProgressEventKind, StepEventKind, UpdateComponent,
};

use crate::{events::EventReportMap, ui::defaults::style};

use super::{ComponentId, ParsableComponentId, ALL_COMPONENT_IDS};
use omicron_common::api::internal::nexus::KnownArtifactKind;
use serde::{Deserialize, Serialize};
use slog::{warn, Logger};
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use wicketd_client::types::{ArtifactId, SemverVersion};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackUpdateState {
    pub items: BTreeMap<ComponentId, UpdateItem>,
    pub system_version: Option<SemverVersion>,
    pub artifacts: Vec<ArtifactId>,
    pub artifact_versions: BTreeMap<KnownArtifactKind, SemverVersion>,
    // The update item currently selected is recorded in
    // state.rack_state.selected.
    pub status_view_displayed: bool,
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
                        UpdateItem::new(vec![
                            UpdateComponent::Rot,
                            UpdateComponent::Sp,
                            UpdateComponent::Host,
                        ]),
                    ),
                    ComponentId::Switch(_) => (
                        *id,
                        UpdateItem::new(vec![
                            UpdateComponent::Rot,
                            UpdateComponent::Sp,
                        ]),
                    ),
                    ComponentId::Psc(_) => (
                        *id,
                        UpdateItem::new(vec![
                            UpdateComponent::Rot,
                            UpdateComponent::Sp,
                        ]),
                    ),
                })
                .collect(),
            artifacts: vec![],
            artifact_versions: BTreeMap::default(),
            status_view_displayed: false,
        }
    }

    pub fn item_state(&self, component: ComponentId) -> UpdateItemState {
        if self.artifacts.is_empty() {
            UpdateItemState::AwaitingRepository
        } else {
            match &self.items[&component].state {
                UpdateItemStateImpl::NotStarted => UpdateItemState::NotStarted,
                UpdateItemStateImpl::RunningOrCompleted {
                    event_report,
                    ..
                } => UpdateItemState::RunningOrCompleted { event_report },
            }
        }
    }

    pub fn update_artifacts_and_reports(
        &mut self,
        logger: &Logger,
        system_version: Option<SemverVersion>,
        artifacts: Vec<ArtifactId>,
        reports: EventReportMap,
    ) {
        self.system_version = system_version;
        self.artifacts = artifacts;
        self.artifact_versions.clear();
        for id in &mut self.artifacts {
            if let Ok(known) = id.kind.parse() {
                self.artifact_versions.insert(known, id.version.clone());
            }
        }

        let mut updated_component_ids = HashSet::new();

        for (sp_type, logs) in reports {
            for (i, log) in logs {
                let Ok(id) = ComponentId::try_from(ParsableComponentId {
                    sp_type: &sp_type,
                    i: &i,
                }) else {
                    warn!(logger, "Invalid ComponentId in EventReport: {} {}", &sp_type, &i);
                    continue;
                };
                let item_state = self.items.get_mut(&id).unwrap();
                item_state.update(log);
                updated_component_ids.insert(id);
            }
        }

        // Reset all component IDs that weren't updated.
        for (id, item_state) in &mut self.items {
            if !updated_component_ids.contains(id) {
                item_state.reset();
            }
        }
    }
}

/// The current status of an updating item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateItemState<'a> {
    /// The update repository needs to be uploaded.
    AwaitingRepository,

    /// The update repository has been uploaded but the update hasn't been
    /// started yet.
    NotStarted,

    /// The update is running, or has completed or failed.
    RunningOrCompleted {
        /// The latest event report.
        event_report: &'a EventReport,
    },
    // TODO: detect other states:
    // * cannot be updated (e.g. attempting to update the scrimlet wicket is
    //   currently running on)
    // * already up to date.
}

/// Internal state for an individual item inside a `RackUpdateState`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateItem {
    // A list of all components within this ID.
    components: Vec<UpdateComponent>,
    state: UpdateItemStateImpl,
}

impl UpdateItem {
    pub fn new(components: Vec<UpdateComponent>) -> Self {
        Self { components, state: UpdateItemStateImpl::NotStarted }
    }

    pub fn is_running(&self) -> bool {
        matches!(self.state, UpdateItemStateImpl::RunningOrCompleted { .. })
    }

    pub fn event_report(&self) -> Option<&EventReport> {
        match &self.state {
            UpdateItemStateImpl::NotStarted => None,
            UpdateItemStateImpl::RunningOrCompleted {
                event_report, ..
            } => Some(event_report),
        }
    }

    fn reset(&mut self) {
        self.state = UpdateItemStateImpl::NotStarted;
        return;
    }

    fn update(&mut self, new_event_report: EventReport) {
        if new_event_report.step_events.is_empty() {
            self.reset();
            return;
        }

        match &mut self.state {
            state @ UpdateItemStateImpl::NotStarted => {
                // Transition to the running state.
                let components = self
                    .components
                    .iter()
                    .copied()
                    .map(|component| (component, UpdateRunningState::Waiting))
                    .collect();
                *state = UpdateItemStateImpl::RunningOrCompleted {
                    components,
                    event_report: new_event_report,
                };
            }
            UpdateItemStateImpl::RunningOrCompleted {
                event_report, ..
            } => {
                *event_report = new_event_report;
            }
        }

        let (components, event_report) = match &mut self.state {
            UpdateItemStateImpl::RunningOrCompleted {
                components,
                event_report,
                ..
            } => (components, &*event_report),
            UpdateItemStateImpl::NotStarted => {
                unreachable!(
                    "above block means it's always in the Running state"
                )
            }
        };

        // Mark artifacts as either 'succeeded' or `failed' by looking in
        // the event log.
        for event in &event_report.step_events {
            match &event.kind {
                StepEventKind::NoStepsDefined
                | StepEventKind::ExecutionStarted { .. }
                | StepEventKind::ProgressReset { .. }
                | StepEventKind::AttemptRetry { .. }
                | StepEventKind::Nested { .. }
                | StepEventKind::Unknown => (),

                StepEventKind::ExecutionCompleted {
                    last_step: step, ..
                }
                | StepEventKind::StepCompleted { step, .. } => {
                    if step.info.is_last_step_in_component() {
                        update_component_state(
                            components,
                            Some(step.info.component),
                            UpdateRunningState::Updated,
                        );
                    }
                }
                StepEventKind::ExecutionFailed { failed_step, .. } => {
                    update_component_state(
                        components,
                        Some(failed_step.info.component),
                        UpdateRunningState::Failed,
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
            update_component_state(
                components,
                component,
                UpdateRunningState::Updating,
            );
        }
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (UpdateComponent, UpdateState)> + '_ {
        self.components.iter().map(|component| match &self.state {
            UpdateItemStateImpl::NotStarted => {
                (*component, UpdateState::NotStarted)
            }
            UpdateItemStateImpl::RunningOrCompleted { components, .. } => {
                (*component, UpdateState::Running(components[component]))
            }
        })
    }
}

pub enum UpdateState {
    NotStarted,
    Running(UpdateRunningState),
}

impl Display for UpdateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted => write!(f, "NOT STARTED"),
            Self::Running(state) => write!(f, "{state}"),
        }
    }
}

impl UpdateState {
    pub fn style(&self) -> Style {
        match self {
            UpdateState::NotStarted => style::deselected(),
            UpdateState::Running(state) => state.style(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum UpdateItemStateImpl {
    NotStarted,
    RunningOrCompleted {
        event_report: EventReport,
        components: BTreeMap<UpdateComponent, UpdateRunningState>,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateRunningState {
    Waiting,
    Updated,
    Updating,
    Failed,
}

impl Display for UpdateRunningState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateRunningState::Waiting => write!(f, "WAITING"),
            UpdateRunningState::Updated => write!(f, "UPDATED"),
            UpdateRunningState::Updating => write!(f, "UPDATING"),
            UpdateRunningState::Failed => write!(f, "FAILED"),
        }
    }
}

impl UpdateRunningState {
    pub fn style(&self) -> Style {
        match self {
            UpdateRunningState::Waiting => style::deselected(),
            UpdateRunningState::Updated => style::successful_update(),
            UpdateRunningState::Updating => style::start_update(),
            UpdateRunningState::Failed => style::failed_update(),
        }
    }
}

// For a given Component's artifacts, update it's state
// to reflect what is currently known from the returned log.
fn update_component_state(
    items: &mut BTreeMap<UpdateComponent, UpdateRunningState>,
    component: Option<UpdateComponent>,
    new_state: UpdateRunningState,
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
