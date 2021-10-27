//! Authorization facilities

use crate::authn;
use crate::db;
use crate::db::identity::Resource;
use anyhow::Context;
use oso::Oso;
use oso::PolarClass;

pub const OMICRON_AUTHZ_CONFIG: &str = include_str!("omicron.polar");

/// Server-wide authorization context
pub struct Authz {
    pub oso: Oso, // XXX should not be "pub"
}

impl Authz {
    /// Construct an authorization context
    ///
    /// # Panics
    ///
    /// This function panics if we could not load the compiled-in Polar
    /// configuration.  That should be impossible outside of development.
    pub fn new() -> Authz {
        let oso = make_omicron_oso().expect("initializing Oso");
        Authz { oso }
    }
}

fn make_omicron_oso() -> Result<Oso, anyhow::Error> {
    let mut oso = Oso::new();
    let classes = [Actor::get_polar_class(), Organization::get_polar_class()];
    for c in classes {
        oso.register_class(c).context("registering class")?;
    }
    oso.load_str(OMICRON_AUTHZ_CONFIG)
        .context("loading built-in Polar (Oso) config")?;
    Ok(oso)
}

/*
 * Define newtypes for the various types that we want to impl PolarClass for.
 * This is needed because is_allowed() consumes these types, not references.  So
 * if we want to call is_allowed() and still reference the original type, we
 * need to have copied whatever information is_allowed() needs.
 */

#[derive(Debug, Eq, PartialEq)]
pub struct Actor(String);
impl From<&authn::Actor> for Actor {
    fn from(omicron_actor: &authn::Actor) -> Self {
        // We store the string rather than the uuid because PolarClass is not
        // impl'd for Uuids.  (We could instead use a Vec<u8> for the raw bytes,
        // but is that really better?)
        Actor(omicron_actor.0.to_string())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Organization(String);
impl From<&db::model::Organization> for Organization {
    fn from(omicron_organization: &db::model::Organization) -> Self {
        Organization(omicron_organization.id().to_string())
    }
}

impl oso::PolarClass for Actor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            // "Actor" is reserved in Polar
            .name("Actor2")
            .set_equality_check(|a1: &Actor, a2: &Actor| a1 == a2)
            .add_attribute_getter("id", |a: &Actor| a.0.clone())
    }
}

impl oso::PolarClass for Organization {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder().set_equality_check(|a1, a2| a1 == a2)
    }
}
