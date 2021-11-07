//! Types and impls used for integration with Oso

// Most of the types here are used in the Polar configuration, which means they
// must impl [`oso::PolarClass`].  There is a derive(PolarClass), but it's
// pretty limited: it doesn't define an equality operator even when the type
// itself impls PartialEq and Eq.  It also doesn't let you define methods.  We
// may want to define our own macro(s) to avoid having to impl this by hand
// everywhere.
//
// Many of these are newtypes that wrap existing types, either simple types from
// Nexus (like `authn::Context`) or database model types.  The main reason is
// that [`Oso::is_allowed()`] consumes owned values, not references.  Consumers
// that want to do an authz check almost always want to keep using the thing
// they had.  We could instead require that the database model types all impl
// `Clone`, impl `PolarClass` on those types, and then just copy them when we
// pass them to `is_allowed()`.  Using newtypes is a way to capture just the
// parts we need for authorization.

use crate::authn;
use crate::db;
use crate::db::identity::Resource;
use anyhow::Context;
use oso::Oso;
use oso::PolarClass;
use std::fmt;

/// Polar configuration describing control plane authorization rules
pub const OMICRON_AUTHZ_CONFIG: &str = include_str!("omicron.polar");

/// Returns an Oso handle suitable for authorizing using Omicron's authorization
/// rules
pub fn make_omicron_oso() -> Result<Oso, anyhow::Error> {
    let mut oso = Oso::new();
    let classes = [
        Action::get_polar_class(),
        AnyActor::get_polar_class(),
        AuthenticatedActor::get_polar_class(),
        Database::get_polar_class(),
        Fleet::get_polar_class(),
        Organization::get_polar_class(),
    ];
    for c in classes {
        oso.register_class(c).context("registering class")?;
    }
    oso.load_str(OMICRON_AUTHZ_CONFIG)
        .context("loading built-in Polar (Oso) config")?;
    Ok(oso)
}

//
// Helper types
// See the note above about why we don't use derive(PolarClass).
//

/// Describes an action being authorized
///
/// There's currently just one enum of Actions for all of Omicron.  We expect
/// most objects to support mosty the same set of actions, except that we use
/// distinct variants for the various "create" actions because it's possible a
/// resource might support more than one different thing that you can create
/// (e.g., a Project will support CreateInstance and CreateDisk).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Query, // only used for [`Database`]
    Read,  // general "read" action
    CreateOrganization,
}

impl oso::PolarClass for Action {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .name("Action")
            .set_equality_check(|a1, a2| a1 == a2)
            .add_method("to_perm", |a: &Action| {
                match a {
                    Action::Query => Perm::Query,
                    Action::Read => Perm::Read,
                    Action::CreateOrganization => Perm::CreateOrganization,
                }
                .to_string()
            })
    }
}

/// Describes a permission used in the Polar configuration
///
/// Note that Polar (appears to) require that all permissions actually be
/// strings in the configuration.  This type is used only in Rust.  It doesn't
/// even impl [`PolarClass`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Perm {
    Query,
    Read,
    CreateOrganization,
}

impl fmt::Display for Perm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // This implementation MUST be kept in sync with the Polar configuration
        // for Omicron, which uses literal strings for permissions.
        f.write_str(match self {
            Perm::Read => "read",
            Perm::Query => "query",
            Perm::CreateOrganization => "create_organization",
        })
    }
}

/// Represents [`authn::Context`] (which is either an authenticated or
/// unauthenticated actor) for Polar
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
            .name("AnyActor")
            .set_equality_check(|a1: &AnyActor, a2: &AnyActor| a1 == a2)
            .add_attribute_getter("authenticated", |a: &AnyActor| a.0)
            .add_attribute_getter("authn_actor", |a: &AnyActor| {
                a.1.as_ref().map(|a| AuthenticatedActor(a.clone()))
            })
    }
}

/// Represents an authenticated [`authn::Context`] for Polar
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
            .add_attribute_getter("is_test_user", |a: &AuthenticatedActor| {
                a.0 == authn::TEST_USER_UUID_PRIVILEGED
            })
            .set_equality_check(
                |a1: &AuthenticatedActor, a2: &AuthenticatedActor| a1 == a2,
            )
            .add_attribute_getter("id", |a: &AuthenticatedActor| a.0.clone())
    }
}

//
// Newtypes for model types that are exposed to Polar
// These all impl [`oso::PolarClass`].
// See the note above about why we use newtypes and why we don't use
// derive(PolarClass).
//

/// Represents the whole Oxide fleet to Polar (used essentially to mean
/// "global").  See RFD 24.
#[derive(Clone, Debug, Eq, PartialEq, oso::PolarClass)]
pub struct Fleet;
pub const FLEET: Fleet = Fleet;

/// Wraps [`db::model::Organization`] for Polar
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

/// Represents the database itself to Polar (so that we can have roles with no
/// access to the database at all)
#[derive(Clone, Debug, Eq, PartialEq, oso::PolarClass)]
pub struct Database;
pub const DATABASE: Database = Database;
