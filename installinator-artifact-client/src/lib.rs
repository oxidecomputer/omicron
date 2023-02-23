// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to installinator-artifactd.

progenitor::generate_api!(
    spec = "../openapi/installinator-artifactd.json",
    inner_type = slog::Logger,
    pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
        slog::debug!(log, "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
    }),
    post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
        slog::debug!(log, "client response"; "result" => ?result);
    }),
    derives = [schemars::JsonSchema],
);

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;

impl From<installinator_common::ProgressReport> for types::ProgressReport {
    fn from(value: installinator_common::ProgressReport) -> Self {
        Self {
            total_elapsed: value.total_elapsed.into(),
            events: value.events.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<installinator_common::ReportEvent> for types::ReportEvent {
    fn from(value: installinator_common::ReportEvent) -> Self {
        Self {
            total_elapsed: value.total_elapsed.into(),
            kind: value.kind.into(),
        }
    }
}
impl From<installinator_common::ReportEventKind> for types::ReportEventKind {
    fn from(value: installinator_common::ReportEventKind) -> Self {
        match value {
            installinator_common::ReportEventKind::DownloadProgress {
                attempt,
                kind,
                peer,
                downloaded_bytes,
                elapsed,
            } => Self::DownloadProgress {
                attempt: attempt as u32,
                kind: kind.to_string(),
                peer: peer.to_string(),
                downloaded_bytes,
                elapsed: elapsed.into(),
            },
            installinator_common::ReportEventKind::DownloadFailed {
                attempt,
                kind,
                peer,
                downloaded_bytes,
                elapsed,
                message,
            } => Self::DownloadFailed {
                attempt: attempt as u32,
                kind: kind.to_string(),
                peer: peer.to_string(),
                downloaded_bytes,
                elapsed: elapsed.into(),
                message,
            },
            installinator_common::ReportEventKind::DownloadCompleted {
                attempt,
                kind,
                peer,
                artifact_size,
                elapsed,
            } => Self::DownloadCompleted {
                attempt: attempt as u32,
                kind: kind.to_string(),
                peer: peer.to_string(),
                artifact_size,
                elapsed: elapsed.into(),
            },
            installinator_common::ReportEventKind::WriteProgress {
                attempt,
                kind,
                destination,
                written_bytes,
                total_bytes,
                elapsed,
            } => Self::WriteProgress {
                attempt: attempt as u32,
                kind: kind.to_string(),
                destination: destination.into_string(),
                written_bytes,
                total_bytes,
                elapsed: elapsed.into(),
            },
            installinator_common::ReportEventKind::WriteFailed {
                attempt,
                kind,
                destination,
                written_bytes,
                total_bytes,
                elapsed,
                message,
            } => Self::WriteFailed {
                attempt: attempt as u32,
                kind: kind.to_string(),
                destination: destination.into_string(),
                written_bytes,
                total_bytes,
                elapsed: elapsed.into(),
                message,
            },
            installinator_common::ReportEventKind::WriteCompleted {
                attempt,
                kind,
                destination,
                artifact_size,
                elapsed,
            } => Self::WriteCompleted {
                attempt: attempt as u32,
                kind: kind.to_string(),
                destination: destination.into_string(),
                artifact_size,
                elapsed: elapsed.into(),
            },
        }
    }
}

impl From<std::time::Duration> for types::Duration {
    fn from(s: std::time::Duration) -> Self {
        Self { secs: s.as_secs(), nanos: s.subsec_nanos() }
    }
}
