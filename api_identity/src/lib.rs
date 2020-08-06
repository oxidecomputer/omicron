// Copyright 2020 Oxide Computer Company
/*!
 * This macro is a helper to generate an accessor for the identity of any
 * `ApiObject`.
 */

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

/**
 * Generates an "identity()" accessor for any `ApiObject` having an `identity`
 * field.
 */
#[proc_macro_derive(ApiObjectIdentity)]
pub fn api_identity(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match do_api_identity(item.into()) {
        Ok(result) => result.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn do_api_identity(item: TokenStream) -> Result<TokenStream, syn::Error> {
    let ast: ItemStruct = syn::parse2(item)?;
    let name = &ast.ident;
    let stream = quote! {
        impl ApiObjectIdentity for #name {
            fn identity(&self) -> &ApiIdentityMetadata {
                &self.identity
            }
        }
    };

    Ok(stream)
}

/* TODO-coverage tests */
