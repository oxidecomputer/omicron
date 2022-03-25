// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating lookup structures and related functions
//!
//! See nexus/src/db/lookup.rs.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::spanned::Spanned;
use syn::{
    Data, DataStruct, DeriveInput, Error, Fields, Ident, ItemStruct, Lit, Meta,
};

#[derive(serde::Deserialize)]
struct Config {
    ancestors: Vec<String>,
}

pub fn lookup_resource(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let config = match serde_tokenstream::from_tokenstream::<Config>(
        &TokenStream::from(attr),
    ) {
        Ok(c) => c,
        Err(err) => return err.to_compile_error().into(),
    };

    let item = syn::parse_macro_input!(input as ItemStruct);
    let name = &item.ident;
    let generics = &item.generics;
    let fields = &item.fields;
    let parent = config.ancestors.first().unwrap_or("LookupPath");
    println!("{}", fields.to_token_stream());

    quote! {
        struct #name #generics {
            // #fields
        }
    }.into()
}
