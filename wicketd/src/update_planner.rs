// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::artifacts::WicketdArtifactStore;
use crate::http_entrypoints::ComponentUpdateTerminalState;
use crate::mgs::MgsHandle;
use anyhow::anyhow;
use anyhow::Context;
use buf_list::BufList;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use omicron_common::api::internal::nexus::UpdateArtifactKind;
use slog::error;
use slog::info;
use slog::o;
use slog::Logger;
use thiserror::Error;

#[derive(Debug)]
pub(crate) struct UpdatePlanner {
    mgs_handle: MgsHandle,
    artifact_store: WicketdArtifactStore,
    log: Logger,
}

impl UpdatePlanner {
    pub(crate) fn new(
        mgs_handle: MgsHandle,
        artifact_store: WicketdArtifactStore,
        log: &Logger,
    ) -> Self {
        let log = log.new(o!("component" => "wicketd update planner"));
        Self { mgs_handle, artifact_store, log }
    }

    pub(crate) fn start(
        &self,
        sp: SpIdentifier,
    ) -> Result<(), UpdatePlanError> {
        let plan = UpdatePlan::new(sp.type_, &self.artifact_store)?;

        let update_driver = UpdateDriver {
            sp,
            mgs_handle: self.mgs_handle.clone(),
            artifact_store: self.artifact_store.clone(),
            log: self.log.new(o!("sp" => format!("{sp:?}"))),
        };

        tokio::spawn(update_driver.run(plan));

        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum UpdatePlanError {
    #[error(
        "invalid TUF repository: more than one artifact present of kind {0:?}"
    )]
    DuplicateArtifacts(UpdateArtifactKind),
    #[error("invalid TUF repository: no artifact present of kind {0:?}")]
    MissingArtifact(UpdateArtifactKind),
}

#[derive(Debug)]
struct UpdatePlan {
    sp: UpdateArtifactId,
}

impl UpdatePlan {
    fn new(
        sp_type: SpType,
        artifact_store: &WicketdArtifactStore,
    ) -> Result<Self, UpdatePlanError> {
        // Given the list of available artifacts, ensure we have exactly 1
        // choice for each kind we need.
        let artifacts = artifact_store.artifact_ids();

        let sp_kind = match sp_type {
            SpType::Sled => UpdateArtifactKind::GimletSp,
            SpType::Power => UpdateArtifactKind::PscSp,
            SpType::Switch => UpdateArtifactKind::SwitchSp,
        };

        let sp = Self::find_exactly_one(sp_kind, &artifacts)?;

        Ok(Self { sp })
    }

    fn find_exactly_one(
        kind: UpdateArtifactKind,
        artifacts: &[UpdateArtifactId],
    ) -> Result<UpdateArtifactId, UpdatePlanError> {
        let mut found = None;
        for artifact in artifacts.iter().filter(|id| id.kind == kind) {
            if found.replace(artifact.clone()).is_some() {
                return Err(UpdatePlanError::DuplicateArtifacts(kind));
            }
        }

        found.ok_or(UpdatePlanError::MissingArtifact(kind))
    }
}

#[derive(Debug)]
struct UpdateDriver {
    sp: SpIdentifier,
    mgs_handle: MgsHandle,
    artifact_store: WicketdArtifactStore,
    log: Logger,
}

impl UpdateDriver {
    async fn run(self, plan: UpdatePlan) {
        if let Err(err) = self.run_impl(plan).await {
            error!(self.log, "update failed"; "err" => %err);
        }
    }

    async fn run_impl(&self, plan: UpdatePlan) -> anyhow::Result<()> {
        self.update_sp(plan.sp).await?;

        info!(self.log, "all updates complete; resetting SP");
        self.mgs_handle
            .sp_reset(self.sp)
            .await
            .context("failed to reset SP")?;

        Ok(())
    }

    async fn update_sp(
        &self,
        artifact: UpdateArtifactId,
    ) -> anyhow::Result<()> {
        info!(self.log, "starting SP update");
        let data = self.artifact_data(&artifact)?;
        let completion_rx = self
            .mgs_handle
            .start_component_update(self.sp, 0, artifact, data)
            .await
            .context("failed to start update")?;

        info!(self.log, "waiting for SP update to complete");
        let state = completion_rx
            .await
            .context("update task died without signaling completion")?;
        match state {
            ComponentUpdateTerminalState::Complete => Ok(()),
            ComponentUpdateTerminalState::UpdateTaskPanicked => {
                Err(anyhow!("update task panicked"))
            }
            ComponentUpdateTerminalState::Failed { reason } => {
                Err(anyhow!("update failed: {reason}"))
            }
        }
    }

    fn artifact_data(&self, id: &UpdateArtifactId) -> anyhow::Result<BufList> {
        self.artifact_store.get(id).with_context(|| {
            format!("missing artifact {id:?}: did the TUF repository change?")
        })
    }
}
