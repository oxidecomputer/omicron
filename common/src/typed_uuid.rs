// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use newtype_uuid::{TypedUuidKind, TypedUuidTag};
use schemars::JsonSchema;

macro_rules! impl_typed_uuid_kind {
    ($($kind:ident => $tag:literal),* $(,)?) => {
        $(
            #[derive(JsonSchema)]
            pub enum $kind {}

            impl TypedUuidKind for $kind {
                #[inline]
                fn tag() -> TypedUuidTag {
                    TypedUuidTag::new($tag)
                }
            }
        )*
    };
}

impl_typed_uuid_kind! {
    LoopbackAddressKind => "loopback_address",
}
