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
            completion_events: value
                .completion_events
                .into_iter()
                .map(Into::into)
                .collect(),
            progress_events: value
                .progress_events
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<installinator_common::CompletionEvent> for types::CompletionEvent {
    fn from(value: installinator_common::CompletionEvent) -> Self {
        Self {
            total_elapsed: value.total_elapsed.into(),
            kind: value.kind.into(),
        }
    }
}

impl From<installinator_common::CompletionEventKind>
    for types::CompletionEventKind
{
    fn from(value: installinator_common::CompletionEventKind) -> Self {
        match value {
            installinator_common::CompletionEventKind::DownloadFailed {
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
            installinator_common::CompletionEventKind::DownloadCompleted {
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
            installinator_common::CompletionEventKind::WriteFailed {
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
            installinator_common::CompletionEventKind::WriteCompleted {
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
            installinator_common::CompletionEventKind::Completed => {
                Self::Completed
            }
        }
    }
}

impl From<installinator_common::ProgressEvent> for types::ProgressEvent {
    fn from(value: installinator_common::ProgressEvent) -> Self {
        Self {
            total_elapsed: value.total_elapsed.into(),
            kind: value.kind.into(),
        }
    }
}

impl From<installinator_common::ProgressEventKind>
    for types::ProgressEventKind
{
    fn from(value: installinator_common::ProgressEventKind) -> Self {
        match value {
            installinator_common::ProgressEventKind::DownloadProgress {
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
            installinator_common::ProgressEventKind::WriteProgress {
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
        }
    }
}

impl From<std::time::Duration> for types::Duration {
    fn from(s: std::time::Duration) -> Self {
        Self { secs: s.as_secs(), nanos: s.subsec_nanos() }
    }
}
