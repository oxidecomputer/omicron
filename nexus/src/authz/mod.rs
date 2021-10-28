//! Authorization facilities

use crate::authn;
use crate::db;
use crate::db::identity::Resource;
use anyhow::Context as _;
use omicron_common::api::external::Error;
use oso::Oso;
use oso::PolarClass;
use std::sync::Arc;

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
    let classes = [
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

/// Operation-specific authorization context
pub struct Context {
    authn: Arc<authn::Context>,
    authz: Arc<Authz>,
}

impl Context {
    pub fn new(authn: Arc<authn::Context>, authz: Arc<Authz>) -> Context {
        Context { authn, authz }
    }

    /// Check whether the actor performing this request is authorized for
    /// `action` on `resource`.
    pub fn authorize<Action, Resource>(
        &self,
        action: Action,
        resource: Resource,
    ) -> Result<(), Error>
    where
        Action: oso::ToPolar,
        Resource: oso::ToPolar,
    {
        let actor = AnyActor::from(&*self.authn);
        match self.authz.oso.is_allowed(actor, action, resource) {
            Err(error) => Err(Error::internal_error(&format!(
                "failed to compute authorization: {:#}",
                error
            ))),
            // XXX In some cases, this should be NotFound.  These are cases
            // where "action" is a read permission.  But we cannot construct the
            // appropriate NotFound here without more information: the resource
            // type and how it was looked up.  In practice, maybe all of the
            // cases that should work this way ought to use query filtering
            // instead, so we can safely assume Forbidden here.  That remains to
            // be seen.
            Ok(false) => Err(Error::Forbidden),
            Ok(true) => Ok(()),
        }
    }
}
