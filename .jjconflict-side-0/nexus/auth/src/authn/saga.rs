// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Serialization of authentication context for sagas
// TODO-security: This is not fully baked.  Right now, we serialize the actor
// id.  We should think through that a little more.  Should we instead preload a
// bunch of roles and then serialize that, for example?

use crate::authn;
use crate::context::OpContext;
use serde::Deserialize;
use serde::Serialize;

/// Serialized form of an `OpContext`
// NOTE: This structure must be versioned carefully.  (That's true of all saga
// structures, but this one has a particularly large blast radius.)
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Serialized {
    kind: authn::Kind,
}

impl Serialized {
    pub fn for_opctx(opctx: &OpContext) -> Serialized {
        Serialized { kind: opctx.authn.kind.clone() }
    }

    pub fn to_authn(&self) -> authn::Context {
        authn::Context { kind: self.kind.clone(), schemes_tried: vec![] }
    }
}
