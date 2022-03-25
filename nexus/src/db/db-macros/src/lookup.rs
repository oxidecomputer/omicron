// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating lookup structures and related functions
//!
//! See nexus/src/db/lookup.rs.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ItemStruct;

#[derive(serde::Deserialize)]
struct Config {
    ancestors: Vec<String>,
}

pub fn lookup_resource(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match do_lookup_resource(attr.into(), input.into()) {
        Ok(output) => output.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn do_lookup_resource(
    attr: TokenStream,
    input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let config = serde_tokenstream::from_tokenstream::<Config>(&attr)?;

    // TODO
    // - validate no generics and no fields?
    let raw_input = input.clone();
    let item: ItemStruct = syn::parse2(input)?;
    let resource_name = &item.ident;
    let resource_as_snake = format_ident!(
        "{}",
        heck::AsSnakeCase(resource_name.to_string()).to_string()
    );
    let authz_resource = quote! { authz::#resource_name };
    let model_resource = quote! { model::#resource_name };

    // It's important that even if there's only one item in this list, it should
    // still have a trailing comma.
    let authz_ancestors_types_vec: Vec<_> = config
        .ancestors
        .iter()
        .map(|a| {
            let name = format_ident!("{}", a);
            quote! { authz::#name, }
        })
        .collect();
    let authz_ancestors_values_vec: Vec<_> = config
        .ancestors
        .iter()
        .map(|a| {
            let v = format_ident!(
                "authz_{}",
                heck::AsSnakeCase(a.to_string()).to_string()
            );
            quote! { #v , }
        })
        .collect();

    let mut authz_path_types_vec = authz_ancestors_types_vec.clone();
    authz_path_types_vec.push(authz_resource.clone());

    let mut authz_path_values_vec = authz_ancestors_values_vec.clone();
    authz_path_values_vec.push(quote! { authz_self, });

    let authz_ancestors_types = quote! {
        #(#authz_ancestors_types_vec)*
    };
    let authz_ancestors_values = quote! {
        #(#authz_ancestors_values_vec)*
    };
    let authz_path_types = quote! {
        #(#authz_path_types_vec)*
    };
    let authz_path_values = quote! {
        #(#authz_path_values_vec)*
    };
    let root_name = format_ident!("Root");

    let (
        parent_resource_name,
        parent_lookup_arg,
        parent_filter,
        authz_ancestors_values_assign,
        parent_authz,
    ) = match config.ancestors.first() {
        Some(parent_resource_name) => {
            let parent_snake_str =
                heck::AsSnakeCase(parent_resource_name).to_string();
            let parent_id = format_ident!("{}_id", parent_snake_str,);
            let parent_resource_name =
                format_ident!("{}", parent_resource_name);
            let parent_lookup_arg =
                quote! { authz_parent: &authz::#parent_resource_name };
            let parent_filter =
                quote! { .filter(dsl::#parent_id.eq(authz_parent.id())) };
            let authz_ancestors_values_assign = quote! {
                let (#authz_ancestors_values _) =
                    #parent_resource_name::lookup_by_id_no_authz(
                        _opctx, datastore, db_row.#parent_id
                    ).await?;
            };
            let parent_authz = &authz_ancestors_values_vec[0];
            (
                parent_resource_name,
                parent_lookup_arg,
                parent_filter,
                authz_ancestors_values_assign,
                quote! { #parent_authz },
            )
        }
        None => (
            format_ident!("Root"),
            quote! {},
            quote! {},
            quote! {},
            quote! { authz::FLEET },
        ),
    };

    Ok(quote! {
        pub struct #resource_name<'a> {
            key: Key<'a, #parent_resource_name>
        }

        impl #resource_name<'a> {
            // Do NOT make this function public.  It should instead be wrapped
            // by functions that perform authz checks.
            async fn lookup_by_id_no_authz(
                _opctx: &OpContext,
                datastore: &DataStore,
                id: Uuid,
            ) -> LookupResult<(#authz_path_types, #model_resource)> {
                use db::schema::#resource_as_snake::dsl;

                // TODO-security This could use pool_authorized() instead.
                // However, it will change the response code for this case:
                // unauthenticated users will get a 401 rather than a 404
                // because we'll kick them out sooner than we used to -- they
                // won't even be able to make this database query.  That's a
                // good thing but this change can be deferred to a follow-up PR.
                let conn = datastore.pool();
                let db_row = dsl::#resource_as_snake
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::id.eq(id))
                    .select(model::#resource_name::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::#resource_name,
                                LookupType::ById(id)
                            )
                        )
                    })?;
                #authz_ancestors_values_assign
                let authz_self = self.make_authz(
                    &#parent_authz
                    &db_row,
                    LookupType::ById(id)
                );
                Ok((#authz_path_values db_row))
            }
        }
    })
}

mod test {
    use super::do_lookup_resource;
    use quote::quote;

    #[test]
    fn test_lookup_resource() {
        // XXX-dap this should actually do something
        eprintln!(
            "{}",
            do_lookup_resource(
                quote! { ancestors = [] },
                quote! { struct Organization; },
            )
            .unwrap(),
        );

        eprintln!(
            "{}",
            do_lookup_resource(
                quote! { ancestors = [ "Organization" ] },
                quote! { struct Project; },
            )
            .unwrap(),
        );

        eprintln!(
            "{}",
            do_lookup_resource(
                quote! { ancestors = [ "Organization", "Project" ] },
                quote! { struct Instance; },
            )
            .unwrap(),
        );
    }
}
