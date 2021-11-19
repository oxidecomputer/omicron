use parse_display::Display;
use serde::Deserialize;

// Schema for the `artifacts.json` target in the TUF update repository.
#[derive(Clone, Debug, Deserialize)]
pub struct ArtifactsDocument {
    pub artifacts: Vec<UpdateArtifact>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpdateArtifact {
    pub name: String,
    pub version: i64,
    pub kind: UpdateArtifactKind,
    pub target: String,
}

#[derive(Clone, Debug, Display, Deserialize)]
#[display(style = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum UpdateArtifactKind {
    Zone,
}
