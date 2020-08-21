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

#[cfg(test)]
mod test {
    use super::do_api_identity;
    use quote::quote;

    #[test]
    fn test_identity() {
        let ret = do_api_identity(
            quote! {
                struct Foo { identity: ApiIdentityMetadata }
            }
            .into(),
        );

        let expected = quote! {
            impl ApiObjectIdentity for Foo {
                fn identity(&self) -> &ApiIdentityMetadata {
                    &self.identity
                }
            }
        };

        assert_eq!(expected.to_string(), ret.unwrap().to_string());
    }
}
