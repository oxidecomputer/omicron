// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#![cfg_attr(not(feature = "std"), no_std)]

//! A registry for UUID kinds used in Omicron and related projects.
//!
//! See the readme to this crate for more information.

use newtype_uuid::{TypedUuidKind, TypedUuidTag};
#[cfg(feature = "schemars08")]
use schemars::JsonSchema;

macro_rules! impl_typed_uuid_kind {
    ($($kind:ident => $tag:literal),* $(,)?) => {
        $(
            #[cfg_attr(feature = "schemars08", derive(JsonSchema))]
            pub enum $kind {}

            impl TypedUuidKind for $kind {
                #[inline]
                fn tag() -> TypedUuidTag {
                    // `const` ensures that tags are validated at compile-time.
                    const TAG: TypedUuidTag = TypedUuidTag::new($tag);
                    TAG
                }
            }
        )*
    };
}

// NOTE:
//
// This should generally be an append-only list. Removing items from this list
// will not break things for now (because newtype-uuid does not currently alter
// any serialization formats), but it may involve some degree of churn across
// repos.
//
// Please keep this list in alphabetical order.

impl_typed_uuid_kind! {
    LoopbackAddressKind => "loopback_address",
    TufRepoKind => "tuf_repo",
}
