use crate::{Event, EventClass};
use schemars::JsonSchema;
use serde::Serialize;

pub fn register_all(registry: &mut crate::EventSchemaRegistry) {
    registry.register::<Foo>();
    registry.register::<FooBar>();
    registry.register::<FooBaz>();
    registry.register::<QuuxBar>();
}

//
// Define test event class types
//
#[derive(Debug, Serialize, JsonSchema)]
pub struct Foo {
    pub hello_world: bool,
}

impl Event for Foo {
    const CLASS: EventClass = EventClass::TestFoo;
    const VERSION: u32 = 1;
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FooBar {
    pub hello: &'static str,
}

impl Event for FooBar {
    const CLASS: EventClass = EventClass::TestFooBar;
    const VERSION: u32 = 1;
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FooBaz {
    pub hello: &'static str,
}

impl Event for FooBaz {
    const CLASS: EventClass = EventClass::TestFooBaz;
    const VERSION: u32 = 1;
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct QuuxBar {
    pub a: bool,
    pub b: usize,
}

impl Event for QuuxBar {
    const CLASS: EventClass = EventClass::TestQuuxBar;
    const VERSION: u32 = 1;
}
