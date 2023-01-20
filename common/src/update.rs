use crate::api::internal::nexus::UpdateArtifactKind;
use serde::{Deserialize, Serialize};

/// Description of the `artifacts.json` target found in rack update
/// repositories.
///
/// Currently, this has a single top-level field; this gives us an escape hatch
/// in the future if we need to change the schema in a non-backwards-compatible
/// way.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ArtifactsDocument {
    pub artifacts: Vec<Artifact>,
}

/// Describes an artifact available in the repository.
///
/// See also [`crate::api::internal::nexus::UpdateArtifact`], which is used
/// internally between Nexus and Sled Agent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Artifact {
    pub name: String,
    pub version: String,
    pub kind: ArtifactKind,
    pub target: String,
}

/// The kind of artifact we are dealing with.
///
/// To ensure older versions of Nexus can work with update repositories that
/// describe artifact kinds it is not yet aware of, this has a fallback
/// `Unknown` variant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ArtifactKind {
    Known(UpdateArtifactKind),
    Unknown(String),
}

impl ArtifactKind {
    pub fn known(&self) -> Option<UpdateArtifactKind> {
        match self {
            ArtifactKind::Known(kind) => Some(*kind),
            ArtifactKind::Unknown(_) => None,
        }
    }
}

impl From<UpdateArtifactKind> for ArtifactKind {
    fn from(kind: UpdateArtifactKind) -> Self {
        ArtifactKind::Known(kind)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::internal::nexus::UpdateArtifactKind;
    use crate::update::ArtifactKind;

    #[test]
    fn serde_artifact_kind() {
        assert_eq!(
            serde_json::from_str::<ArtifactKind>("\"zone\"").unwrap(),
            ArtifactKind::Known(UpdateArtifactKind::Zone)
        );
        assert_eq!(
            serde_json::from_str::<ArtifactKind>("\"fhqwhgads\"").unwrap(),
            ArtifactKind::Unknown("fhqwhgads".to_string())
        );
        assert!(serde_json::from_str::<ArtifactKind>("null").is_err());

        assert_eq!(
            serde_json::to_string(&ArtifactKind::Known(
                UpdateArtifactKind::Zone
            ))
            .unwrap(),
            "\"zone\""
        );
        assert_eq!(
            serde_json::to_string(&ArtifactKind::Unknown(
                "fhqwhgads".to_string()
            ))
            .unwrap(),
            "\"fhqwhgads\""
        );
    }
}
