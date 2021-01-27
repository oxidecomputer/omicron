// Copyright 2020 Oxide Computer Company
/*!
 * This macro is a helper to generate an accessor for the identity of any
 * `ApiObject`.
 */

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;
use syn::Fields;
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

    if !match ast.fields {
        Fields::Named(ref fields) => fields.named.iter().any(
            |field| matches!(&field.ident, Some(ident) if *ident == "identity"),
        ),
        _ => false,
    } {
        return Err(syn::Error::new_spanned(
            ast,
            "deriving ApiObjectIdentity on a struct requires that it have an \
             `identity` field",
        ));
    };

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

    #[test]
    fn test_identity_no_field() {
        let ret = do_api_identity(
            quote! {
                struct Foo {}
            }
            .into(),
        );

        let error = ret.unwrap_err();
        assert!(error.to_string().starts_with("deriving ApiObjectIdentity"));
    }
}
