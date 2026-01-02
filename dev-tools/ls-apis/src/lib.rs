// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collect information about Progenitor-based APIs

mod api_metadata;
mod cargo;
pub mod plural;
mod system_apis;
mod workspaces;

pub use api_metadata::AllApiMetadata;
pub use api_metadata::ApiConsumerStatus;
pub use api_metadata::ApiExpectedConsumer;
pub use api_metadata::ApiMetadata;
pub use api_metadata::VersionedHow;
pub use system_apis::ApiDependencyFilter;
pub use system_apis::FailedConsumerCheck;
pub use system_apis::SystemApis;

use anyhow::{Context, Result};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::borrow::Borrow;

#[macro_use]
extern crate newtype_derive;

#[derive(Clone, Deserialize, Hash, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct ClientPackageName(String);
NewtypeDebug! { () pub struct ClientPackageName(String); }
NewtypeDeref! { () pub struct ClientPackageName(String); }
NewtypeDerefMut! { () pub struct ClientPackageName(String); }
NewtypeDisplay! { () pub struct ClientPackageName(String); }
NewtypeFrom! { () pub struct ClientPackageName(String); }
impl Borrow<str> for ClientPackageName {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct DeploymentUnitName(String);
NewtypeDebug! { () pub struct DeploymentUnitName(String); }
NewtypeDeref! { () pub struct DeploymentUnitName(String); }
NewtypeDerefMut! { () pub struct DeploymentUnitName(String); }
NewtypeDisplay! { () pub struct DeploymentUnitName(String); }
NewtypeFrom! { () pub struct DeploymentUnitName(String); }

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct ServerPackageName(String);
NewtypeDebug! { () pub struct ServerPackageName(String); }
NewtypeDeref! { () pub struct ServerPackageName(String); }
NewtypeDerefMut! { () pub struct ServerPackageName(String); }
NewtypeDisplay! { () pub struct ServerPackageName(String); }
NewtypeFrom! { () pub struct ServerPackageName(String); }
impl Borrow<str> for ServerPackageName {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Deserialize, Hash, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct ServerComponentName(String);
NewtypeDebug! { () pub struct ServerComponentName(String); }
NewtypeDeref! { () pub struct ServerComponentName(String); }
NewtypeDerefMut! { () pub struct ServerComponentName(String); }
NewtypeDisplay! { () pub struct ServerComponentName(String); }
NewtypeFrom! { () pub struct ServerComponentName(String); }
impl Borrow<String> for ServerPackageName {
    fn borrow(&self) -> &String {
        &self.0
    }
}

/// Parameters for loading information about system APIs
pub struct LoadArgs {
    /// path to developer-maintained API metadata
    pub api_manifest_path: Utf8PathBuf,
}

fn parse_toml_file<T: DeserializeOwned>(path: &Utf8Path) -> Result<T> {
    let s = std::fs::read_to_string(path)
        .with_context(|| format!("read {:?}", path))?;
    toml::from_str(&s).with_context(|| format!("parse {:?}", path))
}
