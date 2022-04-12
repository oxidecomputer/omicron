// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating lookup structures and related functions
//!
//! See nexus/src/db/lookup.rs.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_tokenstream::ParseWrapper;

//
// INPUT (arguments to the macro)
//

/// Arguments for [`lookup_resource!`]
// NOTE: this is only "pub" for the `cargo doc` link on [`lookup_resource!`].
#[derive(serde::Deserialize)]
pub struct Input {
    /// Name of the resource
    ///
    /// This is taken as the name of the database model type in
    /// `omicron_nexus::db::model`, the name of the authz type in
    /// `omicron_nexus::authz`, and will be the name of the new type created by
    /// this macro.  The snake case version of the name is taken as the name of
    /// the Diesel table interface in `db::schema`.
    ///
    /// This value is typically PascalCase (e.g., "Organization").
    name: String,
    /// ordered list of resources that are ancestors of this resource, starting
    /// with the top of the hierarchy
    /// (e.g., for an Instance, this would be `[ "Organization", "Project" ]`
    ancestors: Vec<String>,
    /// unordered list of resources that are direct children of this resource
    /// (e.g., for a Project, these would include "Instance" and "Disk")
    children: Vec<String>,
    /// whether lookup by name is supported (usually within the parent collection)
    lookup_by_name: bool,
    /// Rust type used for the primary key
    primary_key_type: ParseWrapper<syn::Type>,
}

//
// MACRO STATE
//

/// Configuration for [`lookup_resource()`] and its helper functions
///
/// This is all computable from [`Input`].  This precomputes a bunch of useful
/// identifiers and token streams, which makes the generator functions a lot
/// easier to grok.
pub struct Config {
    // The resource itself that we're generating
    /// Basic information about the resource we're generating
    resource: Resource,

    /// Name of the enum describing how we're looking up this resource
    lookup_enum: syn::Ident,

    // The path to the resource
    /// list of type names for this resource and its parents
    /// (e.g., [`Organization`, `Project`])
    path_types: Vec<syn::Ident>,

    /// list of identifiers used for the authz objects for this resource and its
    /// parents, in the same order as `authz_path_types` (e.g.,
    /// [`authz_organization`, `authz_project`])
    path_authz_names: Vec<syn::Ident>,

    // Child resources
    /// list of names of child resources, in the same form and with the same
    /// assumptions as [`Input::name`] (i.e., typically PascalCase)
    child_resources: Vec<String>,

    // Parent resource, if any
    /// Information about the parent resource, if any
    parent: Option<Resource>,

    /// whether lookup by name is supported
    lookup_by_name: bool,

    /// Rust type used for the primary key
    primary_key_type: syn::Type,
}

impl Config {
    fn for_input(input: Input) -> Config {
        let resource = Resource::for_name(&input.name);
        let lookup_enum = format_ident!("{}Key", resource.name);

        let mut path_types: Vec<_> =
            input.ancestors.iter().map(|a| format_ident!("{}", a)).collect();
        path_types.push(resource.name.clone());

        let mut path_authz_names: Vec<_> = input
            .ancestors
            .iter()
            .map(|a| {
                format_ident!("authz_{}", heck::AsSnakeCase(&a).to_string())
            })
            .collect();
        path_authz_names.push(resource.authz_name.clone());

        let child_resources = input.children;
        let parent = input.ancestors.last().map(|s| Resource::for_name(&s));

        Config {
            resource,
            lookup_enum,
            path_types,
            path_authz_names,
            parent,
            child_resources,
            lookup_by_name: input.lookup_by_name,
            primary_key_type: input.primary_key_type.into_inner(),
        }
    }
}

/// Information about a resource (either the one we're generating or an
/// ancestor in its path)
struct Resource {
    /// PascalCase resource name itself (e.g., `Project`)
    ///
    /// See [`Input::name`] for more on the assumptions and how it's used.
    name: syn::Ident,
    /// snake_case resource name (e.g., `project`)
    name_as_snake: String,
    /// identifier for an authz object for this resource (e.g., `authz_project`)
    authz_name: syn::Ident,
}

impl Resource {
    fn for_name(name: &str) -> Resource {
        let name_as_snake = heck::AsSnakeCase(&name).to_string();
        let name = format_ident!("{}", name);
        let authz_name = format_ident!("authz_{}", name_as_snake);
        Resource { name, authz_name, name_as_snake }
    }
}

//
// MACRO IMPLEMENTATION
//

/// Implementation of [`lookup_resource!`]
pub fn lookup_resource(
    raw_input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input = serde_tokenstream::from_tokenstream::<Input>(&raw_input)?;
    let config = Config::for_input(input);

    let resource_name = &config.resource.name;
    let the_basics = generate_struct(&config);
    let misc_helpers = generate_misc_helpers(&config);
    let child_selectors = generate_child_selectors(&config);
    let lookup_methods = generate_lookup_methods(&config);
    let database_functions = generate_database_functions(&config);

    Ok(quote! {
        #the_basics

        impl<'a> #resource_name<'a> {
            #child_selectors

            #lookup_methods

            #misc_helpers

            #database_functions
        }
    })
}

/// Generates the struct definition for this resource
fn generate_struct(config: &Config) -> TokenStream {
    let resource_name = &config.resource.name;
    let doc_struct = format!(
        "Selects a resource of type {} (or any of its children, using the \
        functions on this struct) for lookup or fetch",
        resource_name
    );
    let pkey_type = &config.primary_key_type;

    /* configure the lookup enum */
    let lookup_enum = &config.lookup_enum;
    let name_variant = if config.lookup_by_name {
        let root_sym = format_ident!("Root");
        let parent_resource_name = config
            .parent
            .as_ref()
            .map(|p| &p.name)
            .unwrap_or_else(|| &root_sym);
        quote! {
            /// We're looking for a resource with the given name in the given
            /// parent collection
            Name(#parent_resource_name<'a>, &'a Name),
        }
    } else {
        quote! {}
    };

    quote! {
        #[doc = #doc_struct]
        pub struct #resource_name<'a> {
            key: #lookup_enum<'a>
        }

        /// Describes how we're looking up this resource
        enum #lookup_enum<'a>{
            #name_variant

            /// We're looking for a resource with the given id
            ///
            /// This has no parent container -- a by-id lookup is always global
            Id(Root<'a>, #pkey_type),
        }
    }
}

/// Generates the child selectors for this resource
///
/// For example, for the "Project" resource with child resources "Instance" and
/// "Disk", this will generate the `Project::instance_name()` and
/// `Project::disk_name()` functions.
fn generate_child_selectors(config: &Config) -> TokenStream {
    let child_resource_types: Vec<_> =
        config.child_resources.iter().map(|c| format_ident!("{}", c)).collect();
    let child_resource_key_types: Vec<_> = config
        .child_resources
        .iter()
        .map(|c| format_ident!("{}Key", c))
        .collect();
    let child_selector_fn_names: Vec<_> = config
        .child_resources
        .iter()
        .map(|c| format_ident!("{}_name", heck::AsSnakeCase(c).to_string()))
        .collect();
    let child_selector_fn_docs: Vec<_> = config
        .child_resources
        .iter()
        .map(|child| {
            format!(
                "Select a resource of type {} within this {}, \
                identified by its name",
                child, config.resource.name,
            )
        })
        .collect();

    quote! {
        #(
            #[doc = #child_selector_fn_docs]
            pub fn #child_selector_fn_names<'b, 'c>(
                self,
                name: &'b Name
            ) -> #child_resource_types<'c>
            where
                'a: 'c,
                'b: 'c,
            {
                #child_resource_types {
                    key: #child_resource_key_types::Name(self, name),
                }
            }
        )*
    }
}

/// Generates the simple helper functions for this resource
fn generate_misc_helpers(config: &Config) -> TokenStream {
    let fleet_name = format_ident!("Fleet");
    let resource_name = &config.resource.name;
    let parent_resource_name =
        config.parent.as_ref().map(|p| &p.name).unwrap_or(&fleet_name);
    let lookup_enum = &config.lookup_enum;

    let name_variant = if config.lookup_by_name {
        quote! { #lookup_enum::Name(parent, _) => parent.lookup_root(), }
    } else {
        quote! {}
    };

    quote! {
        /// Build the `authz` object for this resource
        fn make_authz(
            authz_parent: &authz::#parent_resource_name,
            db_row: &model::#resource_name,
            lookup_type: LookupType,
        ) -> authz::#resource_name {
            authz::#resource_name::new(
                authz_parent.clone(),
                db_row.id(),
                lookup_type
            )
        }

        /// Getting the [`LookupPath`] for this lookup
        ///
        /// This is used when we actually query the database.  At that
        /// point, we need the `OpContext` and `DataStore` that are being
        /// used for this lookup.
        fn lookup_root(&self) -> &LookupPath<'a> {
            match &self.key {
                #name_variant

                #lookup_enum::Id(root, _) => root.lookup_root(),
            }
        }
    }
}

/// Generates the lookup-related methods, including the public ones (`fetch()`,
/// `fetch_for()`, and `lookup_for()`) and the private helper (`lookup()`).
fn generate_lookup_methods(config: &Config) -> TokenStream {
    let path_types = &config.path_types;
    let path_authz_names = &config.path_authz_names;
    let resource_name = &config.resource.name;
    let resource_authz_name = &config.resource.authz_name;
    let lookup_enum = &config.lookup_enum;
    let (ancestors_authz_names_assign, parent_lookup_arg_actual) =
        if let Some(p) = &config.parent {
            let nancestors = config.path_authz_names.len() - 1;
            let ancestors_authz_names = &config.path_authz_names[0..nancestors];
            let parent_authz_name = &p.authz_name;
            (
                quote! {
                    let (#(#ancestors_authz_names,)*) = parent.lookup().await?;
                },
                quote! { &#parent_authz_name, },
            )
        } else {
            (quote! {}, quote! {})
        };

    // Generate the by-name branch of the match arm in "fetch_for()"
    let fetch_for_name_variant = if config.lookup_by_name {
        quote! {
            #lookup_enum::Name(parent, name) => {
                #ancestors_authz_names_assign
                let (#resource_authz_name, db_row) = Self::fetch_by_name_for(
                    opctx,
                    datastore,
                    #parent_lookup_arg_actual
                    *name,
                    action,
                ).await?;
                Ok((#(#path_authz_names,)* db_row))
            }
        }
    } else {
        quote! {}
    };

    // Generate the by-name branch of the match arm in "lookup()"
    let lookup_name_variant = if config.lookup_by_name {
        quote! {
            #lookup_enum::Name(parent, name) => {
                // When doing a by-name lookup, we have to look up the
                // parent first.  Since this is recursive, we wind up
                // hitting the database once for each item in the path,
                // in order descending from the root of the tree.  (So
                // we'll look up Organization, then Project, then
                // Instance, etc.)
                // TODO-performance Instead of doing database queries at
                // each level of recursion, we could be building up one
                // big "join" query and hit the database just once.
                #ancestors_authz_names_assign
                let (#resource_authz_name, _) =
                    Self::lookup_by_name_no_authz(
                        opctx,
                        datastore,
                        #parent_lookup_arg_actual
                        *name
                    ).await?;
                Ok((#(#path_authz_names,)*))
            }
        }
    } else {
        quote! {}
    };

    quote! {
        /// Fetch the record corresponding to the selected resource
        ///
        /// This is equivalent to `fetch_for(authz::Action::Read)`.
        pub async fn fetch(
            &self,
        ) -> LookupResult<(#(authz::#path_types,)* model::#resource_name)> {
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
        ) -> LookupResult<(#(authz::#path_types,)* model::#resource_name)> {
            let lookup = self.lookup_root();
            let opctx = &lookup.opctx;
            let datastore = &lookup.datastore;

            match &self.key {
                #fetch_for_name_variant

                #lookup_enum::Id(_, id) => {
                    Self::fetch_by_id_for(opctx, datastore, *id, action).await
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
        ) -> LookupResult<(#(authz::#path_types,)*)> {
            let lookup = self.lookup_root();
            let opctx = &lookup.opctx;
            let (#(#path_authz_names,)*) = self.lookup().await?;
            opctx.authorize(action, &#resource_authz_name).await?;
            Ok((#(#path_authz_names,)*))
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
        ) -> LookupResult<(#(authz::#path_types,)*)> {
            let lookup = self.lookup_root();
            let opctx = &lookup.opctx;
            let datastore = &lookup.datastore;

            match &self.key {
                #lookup_name_variant

                #lookup_enum::Id(_, id) => {
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
                    let (#(#path_authz_names,)* _) =
                        Self::lookup_by_id_no_authz(
                            opctx,
                            datastore,
                            *id
                        ).await?;
                    Ok((#(#path_authz_names,)*))
                }
            }
        }
    }
}

/// Generates low-level functions to fetch database records for this resource
///
/// These are standalone functions, not methods.  They operate on more primitive
/// objects than do the methods: authz objects (representing parent ids), names,
/// and ids.  They also take the `opctx` and `datastore` directly as arguments.
fn generate_database_functions(config: &Config) -> TokenStream {
    let resource_name = &config.resource.name;
    let resource_authz_name = &config.resource.authz_name;
    let resource_as_snake = format_ident!("{}", &config.resource.name_as_snake);
    let path_types = &config.path_types;
    let path_authz_names = &config.path_authz_names;
    let (
        parent_lookup_arg_formal,
        parent_lookup_arg_actual,
        ancestors_authz_names_assign,
        lookup_filter,
        parent_authz_value,
    ) = if let Some(p) = &config.parent {
        let nancestors = config.path_authz_names.len() - 1;
        let ancestors_authz_names = &config.path_authz_names[0..nancestors];
        let parent_resource_name = &p.name;
        let parent_authz_name = &p.authz_name;
        let parent_id = format_ident!("{}_id", &p.name_as_snake);
        (
            quote! { #parent_authz_name: &authz::#parent_resource_name, },
            quote! { #parent_authz_name, },
            quote! {
                let (#(#ancestors_authz_names,)* _) =
                    #parent_resource_name::lookup_by_id_no_authz(
                        opctx, datastore, db_row.#parent_id
                    ).await?;
            },
            quote! { .filter(dsl::#parent_id.eq(#parent_authz_name.id())) },
            quote! { #parent_authz_name },
        )
    } else {
        (quote! {}, quote! {}, quote! {}, quote! {}, quote! { &authz::FLEET })
    };

    let by_name_funcs = if config.lookup_by_name {
        quote! {
            /// Fetch the database row for a resource by doing a lookup by
            /// name, possibly within a collection
            ///
            /// This function checks whether the caller has permissions to
            /// read the requested data.  However, it's not intended to be
            /// used outside this module.  See `fetch_for(authz::Action)`.
            // Do NOT make this function public.
            async fn fetch_by_name_for(
                opctx: &OpContext,
                datastore: &DataStore,
                #parent_lookup_arg_formal
                name: &Name,
                action: authz::Action,
            ) -> LookupResult<(authz::#resource_name, model::#resource_name)> {
                let (#resource_authz_name, db_row) =
                    Self::lookup_by_name_no_authz(
                        opctx,
                        datastore,
                        #parent_lookup_arg_actual
                        name
                    ).await?;
                opctx.authorize(action, &#resource_authz_name).await?;
                Ok((#resource_authz_name, db_row))
            }

            /// Lowest-level function for looking up a resource in the
            /// database by name, possibly within a collection
            ///
            /// This function does not check whether the caller has
            /// permission to read this information.  That's why it's not
            /// `pub`.  Outside this module, you want `fetch()` or
            /// `lookup_for(authz::Action)`.
            // Do NOT make this function public.
            async fn lookup_by_name_no_authz(
                opctx: &OpContext,
                datastore: &DataStore,
                #parent_lookup_arg_formal
                name: &Name,
            ) -> LookupResult<
                (authz::#resource_name, model::#resource_name)
            > {
                use db::schema::#resource_as_snake::dsl;

                dsl::#resource_as_snake
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::name.eq(name.clone()))
                    #lookup_filter
                    .select(model::#resource_name::as_select())
                    .get_result_async(
                        datastore.pool_authorized(opctx).await?
                    )
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::NotFoundByLookup(
                                ResourceType::#resource_name,
                                LookupType::ByName(
                                    name.as_str().to_string()
                                )
                            )
                        )
                    })
                    .map(|db_row| {(
                        Self::make_authz(
                            #parent_authz_value,
                            &db_row,
                            LookupType::ByName(name.as_str().to_string())
                        ),
                        db_row
                    )})
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #by_name_funcs

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
        ) -> LookupResult<(#(authz::#path_types,)* model::#resource_name)> {
            let (#(#path_authz_names,)* db_row) =
                Self::lookup_by_id_no_authz(
                    opctx,
                    datastore,
                    id
                ).await?;
            opctx.authorize(action, &#resource_authz_name).await?;
            Ok((#(#path_authz_names,)* db_row))
        }

        /// Lowest-level function for looking up a resource in the database
        /// by id
        ///
        /// This function does not check whether the caller has permission
        /// to read this information.  That's why it's not `pub`.  Outside
        /// this module, you want `fetch()` or `lookup_for(authz::Action)`.
        // Do NOT make this function public.
        async fn lookup_by_id_no_authz(
            opctx: &OpContext,
            datastore: &DataStore,
            id: Uuid,
        ) -> LookupResult<(#(authz::#path_types,)* model::#resource_name)> {
            use db::schema::#resource_as_snake::dsl;

            let db_row = dsl::#resource_as_snake
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::id.eq(id))
                .select(model::#resource_name::as_select())
                .get_result_async(datastore.pool_authorized(opctx).await?)
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
            #ancestors_authz_names_assign
            let #resource_authz_name = Self::make_authz(
                &#parent_authz_value,
                &db_row,
                LookupType::ById(id)
            );
            Ok((#(#path_authz_names,)* db_row))
        }
    }
}

// This isn't so much a test (although it does make sure we don't panic on some
// basic cases).  This is a way to dump the output of the macro for some common
// inputs.  This is invaluable for debugging.  If there's a bug where the macro
// generates syntactically invalid Rust, `cargo expand` will often not print the
// macro's output.  Instead, you can paste the output of this test into
// lookup.rs, replacing the call to the macro, then reformat the file, and then
// build it in order to see the compiler error in context.
#[cfg(test)]
mod test {
    use super::lookup_resource;
    use quote::quote;
    use rustfmt_wrapper::rustfmt;

    #[test]
    #[ignore]
    fn test_lookup_dump() {
        let output = lookup_resource(
            quote! {
                name = "Organization",
                ancestors = ["Silo"],
                children = [ "Project" ],
                lookup_by_name = true,
                primary_key_type = Uuid,
            }
            .into(),
        )
        .unwrap();
        println!("{}", rustfmt(output).unwrap());

        let output = lookup_resource(
            quote! {
                name = "Project",
                ancestors = ["Organization"],
                children = [ "Disk", "Instance" ],
                lookup_by_name = true,
                primary_key_type = Uuid,
            }
            .into(),
        )
        .unwrap();
        println!("{}", rustfmt(output).unwrap());

        let output = lookup_resource(
            quote! {
                name = "SiloUser",
                ancestors = [],
                children = [],
                lookup_by_name = false,
                primary_key_type = Uuid,
            }
            .into(),
        )
        .unwrap();
        println!("{}", rustfmt(output).unwrap());

        let output = lookup_resource(
            quote! {
                name = "WhatThing",
                ancestors = [ "WhatThingParent" ],
                children = [],
                lookup_by_name = true,
            }
            .into(),
        )
        .unwrap();
        println!("{}", rustfmt(output).unwrap());
    }
}
