// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A registry for generation kinds used in Omicron and related projects.
//!
//! See this crate's `README.adoc` for more information.

#![cfg_attr(not(feature = "std"), no_std)]

// Export these types so that other users don't have to pull in
// oxide-generation.
#[doc(no_inline)]
pub use oxide_generation::{
    Generation, GenerationNegativeError, GenerationOverflowError,
    GenericGeneration, ParseError, TagError, TypedGeneration,
    TypedGenerationKind, TypedGenerationTag,
};

use oxide_generation_macros::impl_typed_generation_kinds;

// NOTE:
//
// This should generally be an append-only list. Removing items from this list
// will not break things for now (because oxide-generation does not
// currently alter any serialization formats), but it may involve some degree of
// churn across repos.
//
// Please keep this list in alphabetical order.
impl_typed_generation_kinds! {
    settings = {
        schemars08 = {
            attrs = [#[cfg(feature = "schemars08")]],
            rust_type = {
                crate = "omicron-generation-kinds",
                version = "*",
                path = "omicron_generation_kinds",
            },
        },
    },
    kinds = {
        Alert = {},
        SagaAdopt = {},
        SagaReassignment = {},
        SupportBundle = {},
        TargetRelease = {},
    },
}
