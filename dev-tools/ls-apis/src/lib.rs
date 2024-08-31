// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compile information about Progenitor-based APIs

use serde::Deserialize;

mod api_metadata;
mod cargo;
mod helpers;

pub use helpers::Apis;
pub use helpers::ApisHelper;
pub use helpers::LoadArgs;

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
pub struct DeploymentUnit(String);
NewtypeDebug! { () pub struct DeploymentUnit(String); }
NewtypeDeref! { () pub struct DeploymentUnit(String); }
NewtypeDerefMut! { () pub struct DeploymentUnit(String); }
NewtypeDisplay! { () pub struct DeploymentUnit(String); }
NewtypeFrom! { () pub struct DeploymentUnit(String); }

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
pub struct ServerComponent(String);
NewtypeDebug! { () pub struct ServerComponent(String); }
NewtypeDeref! { () pub struct ServerComponent(String); }
NewtypeDerefMut! { () pub struct ServerComponent(String); }
NewtypeDisplay! { () pub struct ServerComponent(String); }
NewtypeFrom! { () pub struct ServerComponent(String); }
