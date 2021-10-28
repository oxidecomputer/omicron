//! Types and impls used for integration with Oso

use crate::authn;
use crate::db;
use crate::db::identity::Resource;
use anyhow::Context;
use oso::Oso;
use oso::PolarClass;
use std::fmt;

pub const OMICRON_AUTHZ_CONFIG: &str = include_str!("omicron.polar");

pub fn make_omicron_oso() -> Result<Oso, anyhow::Error> {
    let mut oso = Oso::new();
    let classes = [
        Action::get_polar_class(),
        AnyActor::get_polar_class(),
        AuthenticatedActor::get_polar_class(),
        Database::get_polar_class(),
        Organization::get_polar_class(),
    ];
    for c in classes {
        oso.register_class(c).context("registering class")?;
    }
    oso.load_str(OMICRON_AUTHZ_CONFIG)
        .context("loading built-in Polar (Oso) config")?;
    Ok(oso)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Query, // only used for [`Database`]
    Read,  // general "read" action
}

// We cannot derive(PolarClass) because we need to define equality.
impl oso::PolarClass for Action {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .name("Action")
            .set_equality_check(|a1, a2| a1 == a2)
            .add_method("to_perm", |a: &Action| {
                match a {
                    Action::Query => Perm::Query,
                    Action::Read => Perm::Read,
                }
                .to_string()
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Perm {
    Read,
    Query,
}

impl fmt::Display for Perm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Perm::Read => "read",
            Perm::Query => "query",
        })
    }
}

// XXX
//// We cannot derive(PolarClass) because we need to define Perm::from()
//impl oso::PolarClass for Perm {
//    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
//        oso::Class::builder()
//            .name("Perm")
//            .add_class_method("from_action", |a: &Action| {
//                match a {
//                    Action::Query => Perm::Read,
//                    Action::Read => Perm::Read,
//                }
//                .to_string()
//            })
//            .set_equality_check(|a1, a2| a1 == a2)
//    }
//}

// Define newtypes for the various types that we want to impl PolarClass for.
// This is needed because is_allowed() consumes these types, not references.  So
// if we want to call is_allowed() and still reference the original type, we
// need to have copied whatever information is_allowed() needs.

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnyActor(bool, Option<String>);
impl From<&authn::Context> for AnyActor {
    fn from(authn: &authn::Context) -> Self {
        let actor = authn.actor();
        AnyActor(actor.is_some(), actor.map(|a| a.0.to_string()))
    }
}

impl oso::PolarClass for AnyActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            // "Actor" is reserved in Polar
            .name("AnyActor")
            .set_equality_check(|a1: &AnyActor, a2: &AnyActor| a1 == a2)
            .add_attribute_getter("authenticated", |a: &AnyActor| a.0)
            .add_attribute_getter("authn_actor", |a: &AnyActor| {
                a.1.as_ref().map(|a| AuthenticatedActor(a.clone()))
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedActor(String);
impl From<&authn::Actor> for AuthenticatedActor {
    fn from(omicron_actor: &authn::Actor) -> Self {
        // We store the string rather than the uuid because PolarClass is not
        // impl'd for Uuids.  (We could instead use a Vec<u8> for the raw bytes,
        // but is that really better?)
        AuthenticatedActor(omicron_actor.0.to_string())
    }
}

impl oso::PolarClass for AuthenticatedActor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            // "Actor" is reserved in Polar
            .name("AuthenticatedActor")
            .set_equality_check(
                |a1: &AuthenticatedActor, a2: &AuthenticatedActor| a1 == a2,
            )
            .add_attribute_getter("id", |a: &AuthenticatedActor| a.0.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Organization(String);
impl From<&db::model::Organization> for Organization {
    fn from(omicron_organization: &db::model::Organization) -> Self {
        Organization(omicron_organization.id().to_string())
    }
}

impl oso::PolarClass for Organization {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder().set_equality_check(|a1, a2| a1 == a2)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, oso::PolarClass)]
pub struct Database;
pub const DATABASE: Database = Database;
