// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating lookup structures and related functions
//!
//! See nexus/src/db/lookup.rs.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Arguments for [`lookup_resource!`]
// NOTE: this is only "pub" for the `cargo doc` link on [`lookup_resource!`].
#[derive(serde::Deserialize)]
pub struct Config {
    /// Name of the resource (PascalCase)
    name: String,
    /// ordered list of resources that are ancestors of this resource, starting
    /// with the top of the hierarchy
    /// (e.g., for an Instance, this would be `[ "Organization", "Project" ]`
    ancestors: Vec<String>,
    /// unordered list of resources that are direct children of this resource
    /// (e.g., for a Project, these would include "Instance" and "Disk")
    children: Vec<String>,
    /// describes how the authz object for a resource is constructed from its
    /// parent's authz object
    authz_kind: AuthzKind,
}

/// Describes how the authz object for a resource is constructed from its
/// parent's authz object
///
/// By "authz object", we mean the objects in nexus/src/authz/api_resources.rs.
///
/// This ought to be made more uniform with more typed authz objects, but that's
/// not the way they work today.
#[derive(serde::Deserialize)]
enum AuthzKind {
    /// The authz object is constructed using
    /// `authz_parent.child_generic(ResourceType, Uuid, LookupType)`
    Generic,

    /// The authz object is constructed using
    /// `authz_parent.$resource_type(Uuid, LookupType)`.
    Typed,
}

/// Implementation of [`lookup_resource!]'.
pub fn lookup_resource(input: TokenStream) -> Result<TokenStream, syn::Error> {
    let config = serde_tokenstream::from_tokenstream::<Config>(&input)?;
    let resource_name = format_ident!("{}", config.name);
    let resource_as_snake = format_ident!(
        "{}",
        heck::AsSnakeCase(resource_name.to_string()).to_string()
    );
    let authz_resource = quote! { authz::#resource_name, };
    let model_resource = quote! { model::#resource_name };

    // It's important that even if there's only one item in this list, it should
    // still have a trailing comma.
    let mut authz_path_types_vec: Vec<_> = config
        .ancestors
        .iter()
        .map(|a| {
            let name = format_ident!("{}", a);
            quote! { authz::#name, }
        })
        .collect();
    authz_path_types_vec.push(authz_resource.clone());

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
    let mut authz_path_values_vec = authz_ancestors_values_vec.clone();
    authz_path_values_vec.push(quote! { authz_self, });

    let authz_ancestors_values = quote! {
        #(#authz_ancestors_values_vec)*
    };
    let authz_path_types = quote! {
        #(#authz_path_types_vec)*
    };
    let authz_path_values = quote! {
        #(#authz_path_values_vec)*
    };

    let (
        parent_resource_name,
        parent_lookup_arg,
        parent_lookup_arg_value,
        parent_lookup_arg_value_deref,
        parent_filter,
        authz_ancestors_values_assign,
        parent_authz,
        parent_authz_type,
        authz_ancestors_values_assign_lookup,
    ) = match config.ancestors.last() {
        Some(parent_resource_name) => {
            let parent_snake_str =
                heck::AsSnakeCase(parent_resource_name).to_string();
            let parent_id = format_ident!("{}_id", parent_snake_str,);
            let parent_resource_name =
                format_ident!("{}", parent_resource_name);
            let parent_authz_type = quote! { &authz::#parent_resource_name };
            let authz_parent = format_ident!("authz_{}", parent_snake_str);
            let parent_lookup_arg =
                quote! { #authz_parent: #parent_authz_type, };
            let parent_lookup_arg_value = quote! { &#authz_parent , };
            let parent_lookup_arg_value_deref = quote! { #authz_parent , };
            let parent_filter =
                quote! { .filter(dsl::#parent_id.eq( #authz_parent.id())) };
            let authz_ancestors_values_assign = quote! {
                let (#authz_ancestors_values _) =
                    #parent_resource_name::lookup_by_id_no_authz(
                        _opctx, datastore, db_row.#parent_id
                    ).await?;
            };
            let authz_ancestors_values_assign_lookup = quote! {
                let (#authz_ancestors_values) = parent.lookup().await?;
            };
            let parent_authz = &authz_ancestors_values_vec
                [authz_ancestors_values_vec.len() - 1];
            (
                parent_resource_name,
                parent_lookup_arg,
                parent_lookup_arg_value,
                parent_lookup_arg_value_deref,
                parent_filter,
                authz_ancestors_values_assign,
                quote! { #parent_authz },
                parent_authz_type,
                authz_ancestors_values_assign_lookup,
            )
        }
        None => (
            format_ident!("Root"),
            quote! {},
            quote! {},
            quote! {},
            quote! {},
            quote! {},
            quote! { authz::FLEET, },
            quote! { &authz::Fleet },
            quote! {},
        ),
    };

    let (mkauthz_func, mkauthz_arg) = match &config.authz_kind {
        AuthzKind::Generic => (
            format_ident!("child_generic"),
            quote! { ResourceType::#resource_name, },
        ),
        AuthzKind::Typed => (resource_as_snake.clone(), quote! {}),
    };

    let child_names: Vec<_> =
        config.children.iter().map(|c| format_ident!("{}", c)).collect();
    let child_by_names: Vec<_> = config
        .children
        .iter()
        .map(|c| format_ident!("{}_name", heck::AsSnakeCase(c).to_string()))
        .collect();
    let doc_struct = format!(
        "Selects a resource of type {} (or any of its children, using the \
        functions on this struct) for lookup or fetch",
        &config.name,
    );
    let child_docs: Vec<_> = config
        .children
        .iter()
        .map(|child| {
            format!(
                "Select a resource of type {} within this {}, \
                identified by its name",
                child, &config.name
            )
        })
        .collect();

    Ok(quote! {
        #[doc = #doc_struct]
        pub struct #resource_name<'a> {
            key: Key<'a, #parent_resource_name<'a>>
        }

        impl<'a> #resource_name<'a> {
            /// Getting the LookupPath for this lookup
            ///
            /// This is used when we actually query the database.  At that
            /// point, we need the `OpContext` and `DataStore` that are being
            /// used for this lookup.
            fn lookup_root(&self) -> &LookupPath<'a> {
                match &self.key {
                    Key::Name(parent, _) => parent.lookup_root(),
                    Key::Id(root, _) => root.lookup_root(),
                }
            }

            /// Build the `authz` object for this resource
            fn make_authz(
                authz_parent: #parent_authz_type,
                db_row: &#model_resource,
                lookup_type: LookupType,
            ) -> authz::#resource_name {
                authz_parent.#mkauthz_func(
                    #mkauthz_arg
                    db_row.id(),
                    lookup_type
                )
            }

            /// Fetch the record corresponding to the selected resource
            ///
            /// This is equivalent to `fetch_for(authz::Action::Read)`.
            pub async fn fetch(
                &self,
            ) -> LookupResult<(#authz_path_types #model_resource)> {
                self.fetch_for(authz::Action::Read).await
            }

            /// Fetch the record corresponding to the selected resource and
            /// check whether the caller is allowed to do the specified `action`
            ///
            /// The return value is a tuple that also includes the `authz`
            /// objects for all resources along the path to this one (i.e., all
            /// parent resources) and the authz object for this resource itself.
            /// These objects are useful for identifying those resources by
            /// id, for doing other authz checks, or for looking up related
            /// objects.
            pub async fn fetch_for(
                &self,
                action: authz::Action,
            ) -> LookupResult<(#authz_path_types #model_resource)> {
                let lookup = self.lookup_root();
                let opctx = &lookup.opctx;
                let datastore = &lookup.datastore;

                match &self.key {
                    Key::Name(parent, name) => {
                        #authz_ancestors_values_assign_lookup
                        let (authz_self, db_row) = Self::fetch_by_name_for(
                            opctx,
                            datastore,
                            #parent_lookup_arg_value
                            *name,
                            action,
                        ).await?;
                        Ok((#authz_path_values db_row))
                    }
                    Key::Id(_, id) => {
                        Self::fetch_by_id_for(
                            opctx,
                            datastore,
                            *id,
                            action,
                        ).await
                    }

                }
            }

            /// Fetch an `authz` object for the selected resource and check
            /// whether the caller is allowed to do the specified `action`
            ///
            /// The return value is a tuple that also includes the `authz`
            /// objects for all resources along the path to this one (i.e., all
            /// parent resources) and the authz object for this resource itself.
            /// These objects are useful for identifying those resources by
            /// id, for doing other authz checks, or for looking up related
            /// objects.
            pub async fn lookup_for(
                &self,
                action: authz::Action,
            ) -> LookupResult<(#authz_path_types)> {
                let lookup = self.lookup_root();
                let opctx = &lookup.opctx;
                let (#authz_path_values) = self.lookup().await?;
                opctx.authorize(action, &authz_self).await?;
                Ok((#authz_path_values))
            }

            /// Fetch the "authz" objects for the selected resource and all its
            /// parents
            ///
            /// This function does not check whether the caller has permission
            /// to read this information.  That's why it's not `pub`.  Outside
            /// this module, you want `lookup_for(authz::Action)`.
            // Do NOT make this function public.  It's a helper for fetch() and
            // lookup_for().  It's exposed in a safer way via lookup_for().
            async fn lookup(
                &self,
            ) -> LookupResult<(#authz_path_types)> {
                let lookup = self.lookup_root();
                let opctx = &lookup.opctx;
                let datastore = &lookup.datastore;

                match &self.key {
                    Key::Name(parent, name) => {
                        // When doing a by-name lookup, we have to look up the
                        // parent first.  Since this is recursive, we wind up
                        // hitting the database once for each item in the path,
                        // in order descending from the root of the tree.  (So
                        // we'll look up Organization, then Project, then
                        // Instance, etc.)
                        // TODO-performance Instead of doing database queries at
                        // each level of recursion, we could be building up one
                        // big "join" query and hit the database just once.
                        #authz_ancestors_values_assign_lookup
                        let (authz_self, _) = Self::lookup_by_name_no_authz(
                            opctx,
                            datastore,
                            #parent_lookup_arg_value
                            *name
                        ).await?;
                        Ok((#authz_path_values))
                    }
                    Key::Id(_, id) => {
                        // When doing a by-id lookup, we start directly with the
                        // resource we're looking up.  But we still want to
                        // return a full path of authz objects.  So we look up
                        // the parent by id, then its parent, etc.  Like the
                        // by-name case, we wind up hitting the database once
                        // for each item in the path, but in the reverse order.
                        // So we'll look up the Instance, then the Project, then
                        // the Organization.
                        // TODO-performance Instead of doing database queries at
                        // each level of recursion, we could be building up one
                        // big "join" query and hit the database just once.
                        let (#authz_path_values _) =
                            Self::lookup_by_id_no_authz(
                                opctx,
                                datastore,
                                *id
                            ).await?;
                        Ok((#authz_path_values))
                    }
                }
            }

            /// Fetch the database row for a resource by doing a lookup by name,
            /// possibly within a collection
            ///
            /// This function checks whether the caller has permissions to read
            /// the requested data.  However, it's not intended to be used
            /// outside this module.  See `fetch_for(authz::Action)`.
            // Do NOT make this function public.
            async fn fetch_by_name_for(
                opctx: &OpContext,
                datastore: &DataStore,
                #parent_lookup_arg
                name: &Name,
                action: authz::Action,
            ) -> LookupResult<(#authz_resource #model_resource)> {
                let (authz_self, db_row) = Self::lookup_by_name_no_authz(
                    opctx,
                    datastore,
                    #parent_lookup_arg_value_deref
                    name
                ).await?;
                opctx.authorize(action, &authz_self).await?;
                Ok((authz_self, db_row))
            }

            /// Lowest-level function for looking up a resource in the database
            /// by name, possibly within a collection
            ///
            /// This function does not check whether the caller has permission
            /// to read this information.  That's why it's not `pub`.  Outside
            /// this module, you want `fetch()` or `lookup_for(authz::Action)`.
            // Do NOT make this function public.
            async fn lookup_by_name_no_authz(
                _opctx: &OpContext,
                datastore: &DataStore,
                #parent_lookup_arg
                name: &Name,
            ) -> LookupResult<(#authz_resource #model_resource)> {
                use db::schema::#resource_as_snake::dsl;

                // TODO-security See the note about pool_authorized() below.
                let conn = datastore.pool();
                dsl::#resource_as_snake
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::name.eq(name.clone()))
                    #parent_filter
                    .select(model::#resource_name::as_select())
                    .get_result_async(conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::#resource_name,
                                LookupType::ByName(name.as_str().to_string())
                            )
                        )
                    })
                    .map(|db_row| {(
                        Self::make_authz(
                            &#parent_authz
                            &db_row,
                            LookupType::ByName(name.as_str().to_string())
                        ),
                        db_row
                    )})
            }

            /// Fetch the database row for a resource by doing a lookup by id
            ///
            /// This function checks whether the caller has permissions to read
            /// the requested data.  However, it's not intended to be used
            /// outside this module.  See `fetch_for(authz::Action)`.
            // Do NOT make this function public.
            async fn fetch_by_id_for(
                opctx: &OpContext,
                datastore: &DataStore,
                id: Uuid,
                action: authz::Action,
            ) -> LookupResult<(#authz_path_types #model_resource)> {
                let (#authz_path_values db_row) = Self::lookup_by_id_no_authz(
                    opctx,
                    datastore,
                    id
                ).await?;
                opctx.authorize(action, &authz_self).await?;
                Ok((#authz_path_values db_row))
            }

            /// Lowest-level function for looking up a resource in the database
            /// by id
            ///
            /// This function does not check whether the caller has permission
            /// to read this information.  That's why it's not `pub`.  Outside
            /// this module, you want `fetch()` or `lookup_for(authz::Action)`.
            // Do NOT make this function public.
            async fn lookup_by_id_no_authz(
                _opctx: &OpContext,
                datastore: &DataStore,
                id: Uuid,
            ) -> LookupResult<(#authz_path_types #model_resource)> {
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
                let authz_self = Self::make_authz(
                    &#parent_authz
                    &db_row,
                    LookupType::ById(id)
                );
                Ok((#authz_path_values db_row))
            }

            #(
                #[doc = #child_docs]
                pub fn #child_by_names<'b, 'c>(self, name: &'b Name)
                -> #child_names<'c>
                where
                    'a: 'c,
                    'b: 'c,
                {
                    #child_names {
                        key: Key::Name(self, name)
                    }
                }
            )*
        }
    })
}
