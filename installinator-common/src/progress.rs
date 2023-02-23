// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{net::SocketAddrV6, time::Duration};

use camino::Utf8PathBuf;
use omicron_common::update::ArtifactKind;
use schemars::{
    gen::SchemaGenerator,
    schema::{Schema, SchemaObject},
    JsonSchema,
};
use serde::Deserialize;

/// A report, consisting of a list of events and some more metadata.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub struct ProgressReport {
    /// How long it's been since the installinator started running.
    pub total_elapsed: Duration,

    /// A list of events.
    pub events: Vec<ReportEvent>,
}

/// An individual progress report event.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub struct ReportEvent {
    /// When this event was generated, in duration since the start of the program.
    ///
    /// This can be compared against [`Report::total_elapsed`] to figure out how
    /// old the event was.
    pub total_elapsed: Duration,

    /// The kind of event this is.
    pub kind: ReportEventKind,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum ReportEventKind {
    /// An artifact is being downloaded.
    DownloadProgress {
        /// The download attempt currently occurring.
        attempt: usize,

        /// The kind of artifact being downloaded.
        kind: ArtifactKind,

        /// The peer from which the artifact is being downloaded.
        peer: SocketAddrV6,

        /// The number of bytes downloaded so far.
        downloaded_bytes: u64,

        /// How long it's been since the download started.
        elapsed: Duration,
    },

    /// The download of an artifact failed.
    DownloadFailed {
        /// The download attempt that failed.
        attempt: usize,

        /// The kind of artifact being downloaded.
        kind: ArtifactKind,

        /// The peer the artifact was being downloaded from, if any.
        peer: SocketAddrV6,

        /// The number of bytes downloaded before failure.
        downloaded_bytes: u64,

        /// How long the download took before failing.
        elapsed: Duration,

        /// A message indicating the reason for failure.
        message: String,
    },

    /// An artifact download was completed.
    DownloadCompleted {
        /// The download attempt that completed.
        attempt: usize,

        /// The artifact downloaded.
        kind: ArtifactKind,

        /// The peer the artifact was downloaded from.
        peer: SocketAddrV6,

        /// The number of bytes downloaded.
        artifact_size: u64,

        /// How long the download took.
        elapsed: Duration,
    },

    /// An artifact is being written out.
    WriteProgress {
        /// The write attempt currently occurring.
        attempt: usize,

        /// The kind of artifact being written out.
        kind: ArtifactKind,

        /// The destination the artifact is being written out to.
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,

        /// The number of bytes written out.
        written_bytes: u64,

        /// The total number of bytes to write out.
        total_bytes: u64,

        /// How long it's been since the write started.
        elapsed: Duration,
    },

    /// Failed to write an artifact.
    WriteFailed {
        /// The write attempt that failed.
        attempt: usize,

        /// The kind of artifact being written out.
        kind: ArtifactKind,

        /// The destination the artifact is being written out to.
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,

        /// The number of bytes written out before failure.
        written_bytes: u64,

        /// The total number of bytes that should have been written out.
        total_bytes: u64,

        /// How long the write took before it failed.
        elapsed: Duration,

        /// A message indicating why the write failed.
        message: String,
    },

    /// Completed writing an artifact.
    WriteCompleted {
        /// The write attempt that completed.
        attempt: usize,

        /// The kind of artifact that was written out.
        kind: ArtifactKind,

        /// The destination for the artifact.
        #[schemars(schema_with = "path_schema")]
        destination: Utf8PathBuf,

        /// The number of bytes written out.
        artifact_size: u64,

        /// How long the write took to complete.
        elapsed: Duration,
    },
}

impl ReportEventKind {
    /// Returns true if this is an event that marks the completion of an attempt.
    pub fn is_completion_event(&self) -> bool {
        match self {
            Self::DownloadFailed { .. }
            | Self::DownloadCompleted { .. }
            | Self::WriteFailed { .. }
            | Self::WriteCompleted { .. } => true,
            Self::DownloadProgress { .. } | Self::WriteProgress { .. } => false,
        }
    }
}

fn path_schema(gen: &mut SchemaGenerator) -> Schema {
    let mut schema: SchemaObject = <String>::json_schema(gen).into();
    schema.format = Some(format!("path"));
    schema.into()
}
