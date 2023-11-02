// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating lookup structures and related functions
//!
//! See nexus/src/db/lookup.rs.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_tokenstream::ParseWrapper;
use std::ops::Deref;

//
// INPUT (arguments to the macro)
//

/// Arguments for [`super::lookup_resource!`]
// NOTE: this is only "pub" for the `cargo doc` link on [`lookup_resource!`].
#[derive(serde::Deserialize)]
pub struct Input {
    /// Name of the resource
    ///
    /// This is taken as the name of the database model type in
    /// `omicron_nexus::db_model`, the name of the authz type in
    /// `omicron_nexus::authz`, and will be the name of the new type created by
    /// this macro.  The snake case version of the name is taken as the name of
    /// the Diesel table interface in `db::schema`.
    ///
    /// This value is typically PascalCase (e.g., "Project").
    name: String,
    /// ordered list of resources that are ancestors of this resource, starting
    /// with the top of the hierarchy
    /// (e.g., for an Instance, this would be `[ "Silo", "Project" ]`
    ancestors: Vec<String>,
    /// unordered list of resources that are direct children of this resource
    /// (e.g., for a Project, these would include "Instance" and "Disk")
    children: Vec<String>,
    /// whether lookup by name is supported (usually within the parent collection)
    lookup_by_name: bool,
    /// Description of the primary key columns
    primary_key_columns: Vec<PrimaryKeyColumn>,
    /// This resources supports soft-deletes
    soft_deletes: bool,
    /// This resource appears under the `Silo` hierarchy, but nevertheless
    /// should be visible to users in other Silos
    ///
    /// This is "false" by default.  If you don't specify this,
    /// `lookup_resource!` determines whether something should be visible based
    /// on whether it's nested under a `Silo`.
    #[serde(default)]
    visible_outside_silo: bool,
}

#[derive(serde::Deserialize)]
struct PrimaryKeyColumn {
    column_name: String,
    rust_type: ParseWrapper<syn::Type>,
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

    /// This resources supports soft-deletes
    soft_deletes: bool,

    /// This resource is inside a Silo and only visible to users in the same
    /// Silo
    silo_restricted: bool,

    // The path to the resource
    /// list of type names for this resource and its parents
    /// (e.g., [`Silo`, `Project`])
    path_types: Vec<syn::Ident>,

    /// list of identifiers used for the authz objects for this resource and its
    /// parents, in the same order as `authz_path_types` (e.g.,
    /// [`authz_silo`, `authz_project`])
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

    /// Description of the primary key columns
    primary_key_columns: Vec<PrimaryKeyColumn>,
}

impl Config {
    fn for_input(input: Input) -> Config {
        let resource = Resource::for_name(&input.name);

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
        let parent = input.ancestors.last().map(|s| Resource::for_name(s));
        let silo_restricted = !input.visible_outside_silo
            && input.ancestors.iter().any(|s| s == "Silo");

        Config {
            resource,
            silo_restricted,
            path_types,
            path_authz_names,
            parent,
            child_resources,
            lookup_by_name: input.lookup_by_name,
            primary_key_columns: input.primary_key_columns,
            soft_deletes: input.soft_deletes,
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

/// Implementation of [`super::lookup_resource!`]
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
    let pkey_types =
        config.primary_key_columns.iter().map(|c| c.rust_type.deref());

    /* configure the lookup enum */
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
            /// Same as [`Self::Name`], but the name is owned rather than borrowed
            OwnedName(#parent_resource_name<'a>, Name),
        }
    } else {
        quote! {}
    };

    quote! {
        #[doc = #doc_struct]
        pub enum #resource_name<'a>{
            /// An error occurred while selecting the resource
            ///
            /// This error will be returned by any lookup/fetch attempts.
            Error(Root<'a>, Error),

            #name_variant

            /// We're looking for a resource with the given primary key
            ///
            /// This has no parent container -- a by-id lookup is always global
            PrimaryKey(Root<'a> #(,#pkey_types)*),
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
    let child_selector_fn_names: Vec<_> = config
        .child_resources
        .iter()
        .map(|c| format_ident!("{}_name", heck::AsSnakeCase(c).to_string()))
        .collect();
    let child_selector_fn_names_owned: Vec<_> = config
        .child_resources
        .iter()
        .map(|c| {
            format_ident!("{}_name_owned", heck::AsSnakeCase(c).to_string())
        })
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
                #child_resource_types::Name(self, name)
            }

            #[doc = #child_selector_fn_docs]
            pub fn #child_selector_fn_names_owned<'c>(
                self,
                name: Name,
            ) -> #child_resource_types<'c>
            where
                'a: 'c,
            {
                #child_resource_types::OwnedName(self, name)
            }
        )*
    }
}

/// Generates the simple helper functions for this resource
fn generate_misc_helpers(config: &Config) -> TokenStream {
    let fleet_name = format_ident!("Fleet");
    let resource_name = &config.resource.name;
    let resource_name_str = resource_name.to_string();
    let parent_resource_name =
        config.parent.as_ref().map(|p| &p.name).unwrap_or(&fleet_name);

    let name_variant = if config.lookup_by_name {
        quote! {
            #resource_name::Name(parent, _)
            | #resource_name::OwnedName(parent, _) => parent.lookup_root(),
        }
    } else {
        quote! {}
    };

    let resource_authz_name = &config.resource.authz_name;
    let silo_check_fn = if config.silo_restricted {
        quote! {
            /// For a "siloed" resource (i.e., one that's nested under "Silo" in
            /// the resource hierarchy), check whether a given resource's Silo
            /// (given by `authz_silo`) matches the Silo of the actor doing the
            /// fetch/lookup (given by `opctx`).
            ///
            /// This check should not be strictly necessary.  We should never
            /// wind up hitting the error conditions here.  That's because in
            /// order to reach this check, we must have done a successful authz
            /// check.  That check should have failed because there's no way to
            /// grant users access to resources in other Silos.  So why do this
            /// check at all?  As a belt-and-suspenders way to make sure we
            /// never return objects to a user that are from a different Silo
            /// than the one they're attached to.  But what do we do if the
            /// check fails?  We definitely want to know about it so that we can
            /// determine if there's an authz bug here, and if so, fix it.
            /// That's why we log this at "error" level.  We also override the
            /// lookup return value with a suitable error indicating the
            /// resource does not exist or the caller did not supply
            /// credentials, just as if they didn't have access to the object.
            // TODO-support These log messages should trigger support cases.
            fn silo_check(
                opctx: &OpContext,
                authz_silo: &authz::Silo,
                #resource_authz_name: &authz::#resource_name,
            ) -> Result<(), Error> {
                let log = &opctx.log;

                let actor_silo_id = match opctx
                    .authn
                    .silo_or_builtin()
                    .internal_context("siloed resource check")
                {
                    Ok(Some(silo)) => silo.id(),
                    Ok(None) => {
                        trace!(
                            log,
                            "successful lookup of siloed resource {:?} \
                            using built-in user",
                            #resource_name_str,
                        );
                        return Ok(());
                    },
                    Err(error) => {
                        error!(
                            log,
                            "unexpected successful lookup of siloed resource \
                            {:?} with no actor in OpContext",
                            #resource_name_str,
                        );
                        return Err(error);
                    }
                };

                let resource_silo_id = authz_silo.id();
                if resource_silo_id != actor_silo_id {
                    use crate::authz::ApiResource;
                    error!(
                        log,
                        "unexpected successful lookup of siloed resource \
                        {:?} in a different Silo from current actor (resource \
                        Silo {}, actor Silo {})",
                        #resource_name_str,
                        resource_silo_id,
                        actor_silo_id,
                    );
                    Err(#resource_authz_name.not_found())
                } else {
                    Ok(())
                }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        /// Build the `authz` object for this resource
        fn make_authz(
            authz_parent: &authz::#parent_resource_name,
            db_row: &nexus_db_model::#resource_name,
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
            match &self {
                #resource_name::Error(root, ..) => root.lookup_root(),

                #name_variant

                #resource_name::PrimaryKey(root, ..) => root.lookup_root(),
            }
        }

        #silo_check_fn
    }
}

/// Generates the lookup-related methods, including the public ones (`fetch()`,
/// `fetch_for()`, and `lookup_for()`) and the private helper (`lookup()`).
fn generate_lookup_methods(config: &Config) -> TokenStream {
    let path_types = &config.path_types;
    let path_authz_names = &config.path_authz_names;
    let resource_name = &config.resource.name;
    let resource_authz_name = &config.resource.authz_name;
    let pkey_names: Vec<_> = config
        .primary_key_columns
        .iter()
        .enumerate()
        .map(|(i, _)| format_ident!("v{}", i))
        .collect();
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
            #resource_name::Name(parent, &ref name)
            | #resource_name::OwnedName(parent, ref name) => {
                #ancestors_authz_names_assign
                let (#resource_authz_name, db_row) = Self::fetch_by_name_for(
                    opctx,
                    datastore,
                    #parent_lookup_arg_actual
                    name,
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
            #resource_name::Name(parent, &ref name)
            | #resource_name::OwnedName(parent, ref name) => {
                // When doing a by-name lookup, we have to look up the
                // parent first.  Since this is recursive, we wind up
                // hitting the database once for each item in the path,
                // in order descending from the root of the tree.  (So
                // we'll look up Project, then Instance, etc.)
                // TODO-performance Instead of doing database queries at
                // each level of recursion, we could be building up one
                // big "join" query and hit the database just once.
                #ancestors_authz_names_assign
                let (#resource_authz_name, _) =
                    Self::lookup_by_name_no_authz(
                        opctx,
                        datastore,
                        #parent_lookup_arg_actual
                        name
                    ).await?;
                Ok((#(#path_authz_names,)*))
            }
        }
    } else {
        quote! {}
    };

    // If this resource is "Siloed", then tack on an extra check that the
    // resource's Silo matches the Silo of the actor doing the fetch/lookup.
    // See the generation of `silo_check()` for details.
    let (silo_check_lookup, silo_check_fetch) = if config.silo_restricted {
        (
            quote! {
                .and_then(|input| {
                    let (
                        ref authz_silo,
                        ..,
                        ref #resource_authz_name,
                    ) = &input;
                    Self::silo_check(opctx, authz_silo, #resource_authz_name)?;
                    Ok(input)
                })
            },
            quote! {
                .and_then(|input| {
                    let (
                        ref authz_silo,
                        ..,
                        ref #resource_authz_name,
                        ref _db_row,
                    ) = &input;
                    Self::silo_check(opctx, authz_silo, #resource_authz_name)?;
                    Ok(input)
                })
            },
        )
    } else {
        (quote! {}, quote! {})
    };

    quote! {
        /// Fetch the record corresponding to the selected resource
        ///
        /// This is equivalent to `fetch_for(authz::Action::Read)`.
        pub async fn fetch(
            &self,
        ) -> LookupResult<(#(authz::#path_types,)* nexus_db_model::#resource_name)> {
            self.fetch_for(authz::Action::Read).await
        }

        /// Turn the Result<T, E> of [`fetch`] into a Result<Option<T>, E>.
        pub async fn optional_fetch(
            &self,
        ) -> LookupResult<Option<(#(authz::#path_types,)* nexus_db_model::#resource_name)>> {
            self.optional_fetch_for(authz::Action::Read).await
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
        ) -> LookupResult<(#(authz::#path_types,)* nexus_db_model::#resource_name)> {
            let lookup = self.lookup_root();
            let opctx = &lookup.opctx;
            let datastore = &lookup.datastore;

            match &self {
                #resource_name::Error(_, error) => Err(error.clone()),

                #fetch_for_name_variant

                #resource_name::PrimaryKey(_, #(#pkey_names,)*) => {
                    Self::fetch_by_id_for(
                        opctx,
                        datastore,
                        #(#pkey_names,)*
                        action
                    ).await
                }
            }
            #silo_check_fetch
        }

        /// Turn the Result<T, E> of [`fetch_for`] into a Result<Option<T>, E>.
        pub async fn optional_fetch_for(
            &self,
            action: authz::Action,
        ) -> LookupResult<Option<(#(authz::#path_types,)* nexus_db_model::#resource_name)>> {
            let result = self.fetch_for(action).await;

            match result {
                Err(Error::ObjectNotFound {
                    type_name: _,
                    lookup_type: _,
                }) => Ok(None),

                _ => {
                    Ok(Some(result?))
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
            #silo_check_lookup
        }

        /// Turn the Result<T, E> of [`lookup_for`] into a Result<Option<T>, E>.
        pub async fn optional_lookup_for(
            &self,
            action: authz::Action,
        ) -> LookupResult<Option<(#(authz::#path_types,)*)>> {
            let result = self.lookup_for(action).await;

            match result {
                Err(Error::ObjectNotFound {
                    type_name: _,
                    lookup_type: _,
                }) => Ok(None),

                _ => {
                    Ok(Some(result?))
                }
            }
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

            match &self {
                #resource_name::Error(_, error) => Err(error.clone()),

                #lookup_name_variant

                #resource_name::PrimaryKey(_, #(#pkey_names,)*) => {
                    // When doing a by-id lookup, we start directly with the
                    // resource we're looking up.  But we still want to
                    // return a full path of authz objects.  So we look up
                    // the parent by id, then its parent, etc.  Like the
                    // by-name case, we wind up hitting the database once
                    // for each item in the path, but in the reverse order.
                    // So we'll look up the Instance, then the Project.
                    // TODO-performance Instead of doing database queries at
                    // each level of recursion, we could be building up one
                    // big "join" query and hit the database just once.
                    let (#(#path_authz_names,)* _) =
                        Self::lookup_by_id_no_authz(
                            opctx,
                            datastore,
                            #(#pkey_names,)*
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
    let pkey_types: Vec<_> = config
        .primary_key_columns
        .iter()
        .map(|c| c.rust_type.deref())
        .collect();
    let pkey_column_names = config
        .primary_key_columns
        .iter()
        .map(|c| format_ident!("{}", c.column_name));
    let pkey_names: Vec<_> = config
        .primary_key_columns
        .iter()
        .enumerate()
        .map(|(i, _)| format_ident!("v{}", i))
        .collect();
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
                        opctx, datastore, &db_row.#parent_id
                    ).await?;
            },
            quote! { .filter(dsl::#parent_id.eq(#parent_authz_name.id())) },
            quote! { #parent_authz_name },
        )
    } else {
        (quote! {}, quote! {}, quote! {}, quote! {}, quote! { &authz::FLEET })
    };

    let soft_delete_filter = if config.soft_deletes {
        quote! { .filter(dsl::time_deleted.is_null()) }
    } else {
        quote! {}
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
            ) -> LookupResult<(authz::#resource_name, nexus_db_model::#resource_name)> {
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
                (authz::#resource_name, nexus_db_model::#resource_name)
            > {
                use db::schema::#resource_as_snake::dsl;

                dsl::#resource_as_snake
                    #soft_delete_filter
                    .filter(dsl::name.eq(name.clone()))
                    #lookup_filter
                    .select(nexus_db_model::#resource_name::as_select())
                    .get_result_async(
                        &*datastore.pool_connection_authorized(opctx).await?
                    )
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(
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

    let lookup_type = if config.primary_key_columns.len() == 1
        && config.primary_key_columns[0].column_name == "id"
    {
        quote! { LookupType::ById(#(#pkey_names.clone())*) }
    } else {
        let fmtstr = config
            .primary_key_columns
            .iter()
            .map(|c| format!("{} = {{:?}}", c.column_name))
            .collect::<Vec<_>>()
            .join(", ");
        quote! { LookupType::ByCompositeId(format!(#fmtstr, #(#pkey_names,)*)) }
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
            #(#pkey_names: &#pkey_types,)*
            action: authz::Action,
        ) -> LookupResult<(#(authz::#path_types,)* nexus_db_model::#resource_name)> {
            let (#(#path_authz_names,)* db_row) =
                Self::lookup_by_id_no_authz(
                    opctx,
                    datastore,
                    #(#pkey_names,)*
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
            #(#pkey_names: &#pkey_types,)*
        ) -> LookupResult<(#(authz::#path_types,)* nexus_db_model::#resource_name)> {
            use db::schema::#resource_as_snake::dsl;

            let db_row = dsl::#resource_as_snake
                #soft_delete_filter
                #(.filter(dsl::#pkey_column_names.eq(#pkey_names.clone())))*
                .select(nexus_db_model::#resource_name::as_select())
                .get_result_async(&*datastore.pool_connection_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::NotFoundByLookup(
                            ResourceType::#resource_name,
                            #lookup_type
                        )
                    )
                })?;
            #ancestors_authz_names_assign
            let #resource_authz_name = Self::make_authz(
                &#parent_authz_value,
                &db_row,
                #lookup_type
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
        let output = lookup_resource(quote! {
            name = "Project",
            ancestors = ["Silo"],
            children = [ "Disk", "Instance" ],
            lookup_by_name = true,
            soft_deletes = true,
            primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
        })
        .unwrap();
        println!("{}", rustfmt(output).unwrap());

        let output = lookup_resource(quote! {
            name = "SiloUser",
            ancestors = [],
            children = [],
            lookup_by_name = false,
            soft_deletes = true,
            primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
        })
        .unwrap();
        println!("{}", rustfmt(output).unwrap());

        let output = lookup_resource(quote! {
            name = "UpdateArtifact",
            ancestors = [],
            children = [],
            lookup_by_name = false,
            soft_deletes = false,
            primary_key_columns = [
                { column_name = "name", rust_type = String },
                { column_name = "version", rust_type = i64 },
                { column_name = "kind", rust_type = KnownArtifactKind }
            ]
        })
        .unwrap();
        println!("{}", rustfmt(output).unwrap());
    }
}
