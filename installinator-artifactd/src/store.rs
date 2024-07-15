// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::fmt;

use async_trait::async_trait;
use dropshot::HttpError;
use hyper::Body;
use installinator_common::EventReport;
use omicron_common::update::ArtifactHashId;
use slog::Logger;
use uuid::Uuid;

/// Represents a way to fetch artifacts.
#[async_trait]
pub trait ArtifactGetter: fmt::Debug + Send + Sync + 'static {
    /// Gets an artifact by hash, returning it as a [`Body`].
    async fn get_by_hash(&self, id: &ArtifactHashId) -> Option<(u64, Body)>;

    /// Reports update progress events from the installinator.
    async fn report_progress(
        &self,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<EventReportStatus, HttpError>;
}

/// The status returned by [`ArtifactGetter::report_progress`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[must_use]
pub enum EventReportStatus {
    /// This report was processed by the server.
    Processed,

    /// The update ID was not recognized by the server.
    UnrecognizedUpdateId,

    /// The progress receiver is closed.
    ReceiverClosed,
}

/// The artifact store -- a simple wrapper around a dynamic [`ArtifactGetter`] that does some basic
/// logging.
#[derive(Debug)]
pub(crate) struct ArtifactStore {
    log: Logger,
    getter: Box<dyn ArtifactGetter>,
    // TODO: implement this
}

impl ArtifactStore {
    pub(crate) fn new<Getter: ArtifactGetter>(
        getter: Getter,
        log: &Logger,
    ) -> Self {
        let log = log.new(slog::o!("component" => "artifact store"));
        Self { log, getter: Box::new(getter) }
    }

    pub(crate) async fn get_artifact_by_hash(
        &self,
        id: &ArtifactHashId,
    ) -> Option<(u64, Body)> {
        slog::debug!(self.log, "Artifact requested by hash: {:?}", id);
        self.getter.get_by_hash(id).await
    }

    pub(crate) async fn report_progress(
        &self,
        update_id: Uuid,
        report: EventReport,
    ) -> Result<EventReportStatus, HttpError> {
        slog::debug!(self.log, "Report for {update_id}: {report:?}");
        self.getter.report_progress(update_id, report).await
    }
}
