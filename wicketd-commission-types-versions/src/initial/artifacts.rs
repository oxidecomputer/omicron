// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use omicron_common::update::ArtifactId;
use schemars::JsonSchema;
use semver::Version;
use serde::Serialize;
use tufaceous_artifact::ArtifactHashId;
use wicket_common::inventory::SpType;
use wicket_common::update_events::EventReport;

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct InstallableArtifacts {
    pub artifact_id: ArtifactId,
    pub installable: Vec<ArtifactHashId>,
    pub sign: Option<Vec<u8>>,
}

/// The response to a `get_artifacts` call: the system version, and the list of
/// all artifacts currently held by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetArtifactsAndEventReportsResponse {
    pub system_version: Option<Version>,

    /// Map of artifacts we ingested from the most-recently-uploaded TUF
    /// repository to a list of artifacts we're serving over the bootstrap
    /// network. In some cases the list of artifacts being served will have
    /// length 1 (when we're serving the artifact directly); in other cases the
    /// artifact in the TUF repo contains multiple nested artifacts inside it
    /// (e.g., RoT artifacts contain both A and B images), and we serve the list
    /// of extracted artifacts but not the original combination.
    ///
    /// Conceptually, this is a `BTreeMap<ArtifactId, Vec<ArtifactHashId>>`, but
    /// JSON requires string keys for maps, so we give back a vec of pairs
    /// instead.
    pub artifacts: Vec<InstallableArtifacts>,

    pub event_reports: BTreeMap<SpType, BTreeMap<u16, EventReport>>,
}
