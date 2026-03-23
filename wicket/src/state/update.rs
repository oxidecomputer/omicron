// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use ratatui::style::Style;
use wicket_common::rack_update::{ClearUpdateStateOptions, StartUpdateOptions};
use wicket_common::update_events::{
    EventReport, ProgressEventKind, StepEventKind, UpdateComponent,
    UpdateStepId,
};

use crate::helpers::{get_update_simulated_result, get_update_test_error};
use crate::{
    events::{ArtifactData, EventReportMap},
    ui::defaults::style,
};

use super::{ALL_COMPONENT_IDS, ComponentId, ParsableComponentId};
use semver::Version;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::BTreeMap;
use std::fmt::Display;
use tufaceous_artifact::{ArtifactVersion, KnownArtifactKind};

// Represents a version and the signature (optional) associated
// with a particular artifact. This allows for multiple versions
// with different versions to be present in the repo. Note
// sign is currently only used for RoT artifacts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactVersions {
    pub version: ArtifactVersion,
    pub sign: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RackUpdateState {
    pub items: BTreeMap<ComponentId, UpdateItem>,
    pub system_version: Option<Version>,
    pub artifacts: Vec<ArtifactData>,
    pub artifact_versions: BTreeMap<KnownArtifactKind, Vec<ArtifactVersions>>,
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
                        UpdateItem::new(
                            *id,
                            vec![
                                UpdateComponent::Rot,
                                UpdateComponent::RotBootloader,
                                UpdateComponent::Sp,
                                UpdateComponent::Host,
                            ],
                        ),
                    ),
                    ComponentId::Switch(_) => (
                        *id,
                        UpdateItem::new(
                            *id,
                            vec![
                                UpdateComponent::Rot,
                                UpdateComponent::RotBootloader,
                                UpdateComponent::Sp,
                            ],
                        ),
                    ),
                    ComponentId::Psc(_) => (
                        *id,
                        UpdateItem::new(
                            *id,
                            vec![
                                UpdateComponent::Rot,
                                UpdateComponent::RotBootloader,
                                UpdateComponent::Sp,
                            ],
                        ),
                    ),
                })
                .collect(),
            artifacts: vec![],
            artifact_versions: BTreeMap::default(),
            status_view_displayed: false,
        }
    }

    pub fn item_state(&self, component: ComponentId) -> UpdateItemState<'_> {
        if self.artifacts.is_empty() {
            UpdateItemState::AwaitingRepository
        } else {
            match &self.items[&component].state {
                UpdateItemStateImpl::NotStarted => UpdateItemState::NotStarted,
                UpdateItemStateImpl::UpdateStarted => {
                    UpdateItemState::UpdateStarted
                }
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
        system_version: Option<Version>,
        artifacts: Vec<ArtifactData>,
        reports: EventReportMap,
    ) {
        self.system_version = system_version;
        self.artifacts = artifacts;
        self.artifact_versions.clear();
        for a in &mut self.artifacts {
            if let Some(known) = a.id.kind.to_known() {
                self.artifact_versions.entry(known).or_default().push(
                    ArtifactVersions {
                        version: a.id.version.clone(),
                        sign: a.sign.clone(),
                    },
                );
            }
        }

        let reports = parse_event_report_map(logger, reports);
        // Reset all component IDs that aren't in the event report map.
        for (id, item) in &mut self.items {
            if !reports.contains_key(id) {
                item.reset();
            }
        }

        for (id, report) in reports {
            let item_state = self.items.get_mut(&id).unwrap();
            item_state.update(report);
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

    /// The update has been started, but event reports have not been received
    /// yet.
    UpdateStarted,

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
    component_id: ComponentId,
    components: Vec<UpdateComponent>,
    state: UpdateItemStateImpl,
}

impl UpdateItem {
    pub fn new(
        component_id: ComponentId,
        components: Vec<UpdateComponent>,
    ) -> Self {
        Self {
            component_id,
            components,
            state: UpdateItemStateImpl::NotStarted,
        }
    }

    pub fn is_running(&self) -> bool {
        matches!(self.state, UpdateItemStateImpl::RunningOrCompleted { .. })
    }

    pub fn event_report(&self) -> Option<&EventReport> {
        match &self.state {
            UpdateItemStateImpl::NotStarted
            | UpdateItemStateImpl::UpdateStarted => None,
            UpdateItemStateImpl::RunningOrCompleted {
                event_report, ..
            } => Some(event_report),
        }
    }

    /// Resets the state to "not started". This is called when:
    ///
    /// * A new TUF repo is uploaded.
    /// * wicketd stops returning event reports for this component, for any
    ///   other reason.
    fn reset(&mut self) {
        self.state = UpdateItemStateImpl::NotStarted;
    }

    fn update(&mut self, new_event_report: EventReport) {
        if new_event_report.step_events.is_empty() {
            self.reset();
            return;
        }

        match &mut self.state {
            state @ UpdateItemStateImpl::NotStarted
            | state @ UpdateItemStateImpl::UpdateStarted => {
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
            UpdateItemStateImpl::NotStarted
            | UpdateItemStateImpl::UpdateStarted => {
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
                    last_step: step,
                    last_outcome: outcome,
                    ..
                }
                | StepEventKind::StepCompleted { step, outcome, .. } => {
                    if step.info.is_last_step_in_component() {
                        // The RoT (and bootloader) and SP components each
                        // have two steps in them. If the second step
                        // ("Updating RoT Bootloader/RoT/SP") is
                        // skipped, then treat the component as skipped.
                        if matches!(
                            step.info.component,
                            UpdateComponent::Sp
                                | UpdateComponent::Rot
                                | UpdateComponent::RotBootloader
                        ) {
                            assert_eq!(
                                step.info.id,
                                UpdateStepId::SpComponentUpdate,
                                "The last step must be the SpComponentUpdate step"
                            );
                            if outcome.is_skipped() {
                                update_component_state(
                                    components,
                                    Some(step.info.component),
                                    UpdateRunningState::Skipped,
                                );
                                continue;
                            }
                        }
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
                StepEventKind::ExecutionAborted { aborted_step, .. } => {
                    update_component_state(
                        components,
                        Some(aborted_step.info.component),
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

    pub fn components(&self) -> &[UpdateComponent] {
        &self.components
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (UpdateComponent, UpdateState)> + '_ {
        self.components.iter().map(|component| {
            let state = match &self.state {
                UpdateItemStateImpl::NotStarted => UpdateState::NotStarted,
                UpdateItemStateImpl::UpdateStarted => UpdateState::Starting,
                UpdateItemStateImpl::RunningOrCompleted {
                    components, ..
                } => UpdateState::Running(components[component]),
            };
            (*component, state)
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub enum UpdateState {
    NotStarted,
    Starting,
    FailedToStart,
    Running(UpdateRunningState),
}

impl Display for UpdateState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted => write!(f, "NOT STARTED"),
            Self::Starting => write!(f, "STARTING"),
            Self::FailedToStart => write!(f, "FAILED TO START"),
            Self::Running(state) => write!(f, "{state}"),
        }
    }
}

impl UpdateState {
    pub fn style(&self) -> Style {
        match self {
            UpdateState::NotStarted | UpdateState::Starting => {
                style::deselected()
            }
            UpdateState::FailedToStart => style::failed_update(),
            UpdateState::Running(state) => state.style(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum UpdateItemStateImpl {
    NotStarted,
    UpdateStarted,
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
    Skipped,
    Failed,
    Aborted,
}

impl Display for UpdateRunningState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateRunningState::Waiting => write!(f, "WAITING"),
            UpdateRunningState::Updated => write!(f, "UPDATED"),
            UpdateRunningState::Updating => write!(f, "UPDATING"),
            UpdateRunningState::Skipped => write!(f, "SKIPPED"),
            UpdateRunningState::Failed => write!(f, "FAILED"),
            UpdateRunningState::Aborted => write!(f, "ABORTED"),
        }
    }
}

impl UpdateRunningState {
    pub fn style(&self) -> Style {
        match self {
            UpdateRunningState::Waiting => style::deselected(),
            UpdateRunningState::Updated => style::successful_update(),
            UpdateRunningState::Updating | UpdateRunningState::Skipped => {
                style::start_update()
            }
            UpdateRunningState::Failed | UpdateRunningState::Aborted => {
                style::failed_update()
            }
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
        UpdateComponent::RotBootloader => "ROT_BOOTLOADER",
        UpdateComponent::Rot => "ROT",
        UpdateComponent::Sp => "SP",
        UpdateComponent::Host => "HOST",
    }
}

pub struct CreateStartUpdateOptions {
    pub(crate) force_update_rot_bootloader: bool,
    pub(crate) force_update_rot: bool,
    pub(crate) force_update_sp: bool,
}

impl CreateStartUpdateOptions {
    pub fn to_start_update_options(&self) -> Result<StartUpdateOptions> {
        let test_error =
            get_update_test_error("WICKET_TEST_START_UPDATE_ERROR")?;

        // This is a debug environment variable used to
        // add a test step.
        let test_step_seconds =
            std::env::var("WICKET_UPDATE_TEST_STEP_SECONDS").ok().map(|v| {
                v.parse().expect(
                    "parsed WICKET_UPDATE_TEST_STEP_SECONDS \
                            as a u64",
                )
            });
        let test_simulate_rot_bootloader_result = get_update_simulated_result(
            "WICKET_UPDATE_TEST_SIMULATE_ROT_BOOTLOADER_RESULT",
        )?;
        let test_simulate_rot_result = get_update_simulated_result(
            "WICKET_UPDATE_TEST_SIMULATE_ROT_RESULT",
        )?;
        let test_simulate_sp_result = get_update_simulated_result(
            "WICKET_UPDATE_TEST_SIMULATE_SP_RESULT",
        )?;

        Ok(StartUpdateOptions {
            test_error,
            test_step_seconds,
            test_simulate_rot_bootloader_result,
            test_simulate_rot_result,
            test_simulate_sp_result,
            skip_rot_bootloader_version_check: self.force_update_rot_bootloader,
            skip_rot_version_check: self.force_update_rot,
            skip_sp_version_check: self.force_update_sp,
        })
    }
}

pub struct CreateClearUpdateStateOptions {}

impl CreateClearUpdateStateOptions {
    pub fn to_clear_update_state_options(
        &self,
    ) -> Result<ClearUpdateStateOptions> {
        let test_error =
            get_update_test_error("WICKET_TEST_CLEAR_UPDATE_STATE_ERROR")?;

        Ok(ClearUpdateStateOptions { test_error })
    }
}

/// Converts an `EventReportMap` to a map by component ID.
pub fn parse_event_report_map(
    log: &Logger,
    reports: EventReportMap,
) -> BTreeMap<ComponentId, EventReport> {
    let mut component_id_map = BTreeMap::new();
    for (sp_type, logs) in reports {
        for (i, event_report) in logs {
            let Ok(id) = ComponentId::try_from(ParsableComponentId {
                sp_type: &sp_type,
                i: &i,
            }) else {
                slog::warn!(
                    log,
                    "Invalid ComponentId in EventReportMap: {} {}",
                    &sp_type,
                    &i
                );
                continue;
            };
            component_id_map.insert(id, event_report);
        }
    }

    component_id_map
}
