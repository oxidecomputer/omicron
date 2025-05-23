// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{Alert, AlertClass};
use schemars::JsonSchema;
use serde::Serialize;

pub fn register_all(registry: &mut crate::AlertSchemaRegistry) {
    registry.register::<Foo>();
    registry.register::<FooV2>();
    registry.register::<FooBar>();
    registry.register::<FooBaz>();
    registry.register::<QuuxBar>();
}

//
// Define test alert class types
//
#[derive(Debug, Serialize, JsonSchema)]
pub struct Foo {
    pub hello_world: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FooV2 {
    pub hello_world: bool,
    pub new_thing: usize,
}

impl Alert for Foo {
    const CLASS: AlertClass = AlertClass::TestFoo;
    const VERSION: u32 = 1;
}

impl Alert for FooV2 {
    const CLASS: AlertClass = AlertClass::TestFoo;
    const VERSION: u32 = 2;
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FooBar {
    pub hello: &'static str,
}

impl Alert for FooBar {
    const CLASS: AlertClass = AlertClass::TestFooBar;
    const VERSION: u32 = 1;
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FooBaz {
    pub hello: &'static str,
}

impl Alert for FooBaz {
    const CLASS: AlertClass = AlertClass::TestFooBaz;
    const VERSION: u32 = 1;
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct QuuxBar {
    pub a: bool,
    pub b: usize,
}

impl Alert for QuuxBar {
    const CLASS: AlertClass = AlertClass::TestQuuxBar;
    const VERSION: u32 = 1;
}
