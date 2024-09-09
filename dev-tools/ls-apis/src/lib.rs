// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compile information about Progenitor-based APIs

use serde::Deserialize;

mod api_metadata;
mod cargo;
mod helpers;

pub use api_metadata::AllApiMetadata;
pub use helpers::Apis;
pub use helpers::ApisHelper;
pub use helpers::LoadArgs;
use std::borrow::Borrow;

#[macro_use]
extern crate newtype_derive;

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct ClientPackageName(String);
NewtypeDebug! { () pub struct ClientPackageName(String); }
NewtypeDeref! { () pub struct ClientPackageName(String); }
NewtypeDerefMut! { () pub struct ClientPackageName(String); }
NewtypeDisplay! { () pub struct ClientPackageName(String); }
NewtypeFrom! { () pub struct ClientPackageName(String); }

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

#[derive(Clone, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
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
