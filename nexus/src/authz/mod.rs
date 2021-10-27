//! Authorization facilities

use crate::authn;
use oso::Oso;
use oso::PolarClass;

pub const OMICRON_AUTHZ_CONFIG: &str = include_str!("omicron.polar");

/// Server-wide authorization context
pub struct Authz {
    oso: Oso,
}

impl Authz {
    /// Construct an authorization context
    ///
    /// # Panics
    ///
    /// This function panics if we could not load the compiled-in Polar
    /// configuration.  That should be impossible outside of development.
    pub fn new() -> Authz {
        let mut oso = Oso::new();
        oso.register_class(authn::Actor::get_polar_class())
            .expect("failed to register class");
        oso.load_str(OMICRON_AUTHZ_CONFIG).expect("invalid Polar (OSO) config");
        Authz { oso }
    }
}

impl oso::PolarClass for authn::Actor {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .name("Actor2")
            .add_attribute_getter("id", |actor: &authn::Actor| {
                actor.0.to_string()
            })
    }
}
