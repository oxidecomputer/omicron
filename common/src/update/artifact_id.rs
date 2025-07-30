// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fmt;

use daft::Diffable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::{Artifact, ArtifactKind, ArtifactVersion};

/// An identifier for an artifact.
//
// The kind is [`ArtifactKind`], indicating that it might represent an artifact
// whose kind is unknown.
// TODO: move this to tufaceous-artifact in the future
#[derive(
    Debug,
    Diffable,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct ArtifactId {
    /// The artifact's name.
    pub name: String,

    /// The artifact's version.
    #[schemars(with = "String")]
    pub version: ArtifactVersion,

    /// The kind of artifact this is.
    pub kind: ArtifactKind,
    // TODO-K: This where we want the rkth?
    // clean up, this approach is a bit shit
    // maybe put all of caboose information?
    //
    // If this is an artifact with a signed binary
    // include the sign.
    // pub sign: Option<String>,
}

/// Used for user-friendly messages.
impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} v{} ({})", self.name, self.version, self.kind)
    }
}

impl From<Artifact> for ArtifactId {
    fn from(artifact: Artifact) -> Self {
        ArtifactId {
            name: artifact.name,
            version: artifact.version,
            kind: artifact.kind,
            // TODO-K: A drawback of having the sign live here,
            // is that we'd have to alter Artifact maybe?
            //sign: artifact.sign,
        }
    }
}
