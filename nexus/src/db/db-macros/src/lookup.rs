// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating lookup structures and related functions
//!
//! See nexus/src/db/lookup.rs.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

//
// INPUT (arguments to the macro)
//

/// Arguments for [`lookup_resource!`]
// NOTE: this is only "pub" for the `cargo doc` link on [`lookup_resource!`].
#[derive(serde::Deserialize)]
pub struct Input {
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

//
// MACRO STATE
//

/// Configuration for [`lookup_resource`] and its helper functions
///
/// This is all computable from [`Input`].  The purpose is to put the various
/// output strings that need to appear in various chunks of output into one
/// place with uniform names and documentation.
pub struct Config {
    // The resource itself that we're generating
    /// Basic information about the resource we're generating
    resource: Resource,

    /// See [`AuthzKind`]
    authz_kind: AuthzKind,

    // The path to the resource
    /// list of `authz` types for this resource and its parents
    /// (e.g., [`authz::Organization`, `authz::Project`])
    path_authz_types: Vec<TokenStream>,

    /// list of identifiers used for the authz objects for this resource and its
    /// parents, in the same order as `authz_path_types`
    /// (e.g., [`authz_organization`, `authz_project`])
    path_authz_names: Vec<syn::Ident>,

    // Child resources
    /// list of names of child resources (PascalCase, raw input to the macro)
    /// (e.g., [`Instance`, `Disk`])
    child_resources: Vec<String>,

    // Parent resource, if any
    /// Information about the parent resource, if any
    parent: Option<Resource>,
}

/// Information about a resource (either the one we're generating or an
/// ancestor in the path)
struct Resource {
    /// PascalCase resource name itself (e.g., `Project`)
    name: syn::Ident,
    /// snake_case resource name (e.g., `project`)
    name_as_snake: String,
    /// name of the `authz` type for this resource (e.g., `authz::Project`)
    authz_type: TokenStream,
    /// identifier for an authz object for this resource (e.g., `authz_project`)
    authz_name: syn::Ident,
    /// name of the `model` type for this resource (e.g., `db::model::Project`)
    model_type: TokenStream,
}

impl Resource {
    fn for_name(name: &str) -> Resource {
        let name_as_snake = heck::AsSnakeCase(&name).to_string();
        let name = format_ident!("{}", name);
        let authz_name = format_ident!("authz_{}", name_as_snake);
        let authz_type = quote! { authz::#name };
        let model_type = quote! { model::#name };
        Resource { name, authz_name, authz_type, name_as_snake, model_type }
    }
}

/// Implementation of [`lookup_resource!]'.
pub fn lookup_resource(
    raw_input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input = serde_tokenstream::from_tokenstream::<Input>(&raw_input)?;
    let config = configure(input);

    let resource_name = &config.resource.name;
    let the_struct = generate_struct(&config);
    let misc_helpers = generate_misc_helpers(&config);
    let child_selectors = generate_child_selectors(&config);
    let lookup_methods = generate_lookup_methods(&config);
    let database_functions = generate_database_functions(&config);

    Ok(quote! {
        #the_struct

        impl<'a> #resource_name<'a> {
            #child_selectors

            #lookup_methods

            #misc_helpers

            #database_functions
        }
    })
}

fn configure(input: Input) -> Config {
    let resource = Resource::for_name(&input.name);

    // XXX-dap TODO Can we just make this an array of the PascalCase
    // identifiers?
    let mut path_authz_types: Vec<_> = input
        .ancestors
        .iter()
        .map(|a| {
            let name = format_ident!("{}", a);
            quote! { authz::#name }
        })
        .collect();
    path_authz_types.push(resource.authz_type.clone());

    let authz_ancestors_values: Vec<_> = input
        .ancestors
        .iter()
        .map(|a| format_ident!("authz_{}", heck::AsSnakeCase(&a).to_string()))
        .collect();
    let mut path_authz_names = authz_ancestors_values.clone();
    path_authz_names.push(resource.authz_name.clone());

    Config {
        resource,
        authz_kind: input.authz_kind,
        path_authz_types,
        path_authz_names,
        parent: input.ancestors.last().map(|s| Resource::for_name(&s)),
        child_resources: input.children,
    }
}

/// Generates the struct definition for this resource
fn generate_struct(config: &Config) -> TokenStream {
    let root_sym = format_ident!("Root");
    let resource_name = &config.resource.name;
    let parent_resource_name =
        config.parent.as_ref().map(|p| &p.name).unwrap_or_else(|| &root_sym);
    let doc_struct = format!(
        "Selects a resource of type {} (or any of its children, using the \
        functions on this struct) for lookup or fetch",
        config.resource.name.to_string(),
    );

    quote! {
        #[doc = #doc_struct]
        pub struct #resource_name<'a> {
            key: Key<'a, #parent_resource_name<'a>>
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
                    key: Key::Name(self, name),
                }
            }
        )*
    }
}

/// Generates the simple helper functions for this resource
fn generate_misc_helpers(config: &Config) -> TokenStream {
    let fleet_type = quote! { authz::Fleet };
    let resource_name = &config.resource.name;
    let resource_authz_type = &config.resource.authz_type;
    let resource_model_type = &config.resource.model_type;
    let parent_authz_type =
        config.parent.as_ref().map(|p| &p.authz_type).unwrap_or(&fleet_type);

    // Given a parent authz type, when we want to construct an authz object for
    // a child resource, there are two different patterns.  We need to pick the
    // right one.  For "typed" resources (`AuthzKind::Typed`), the parent
    // resource has a method with the snake case name of the child resource.
    // For example: `authz_organization.project()`.  For "generic" resources
    // (`AuthzKind::Generic`), the parent has a function called `child_generic`
    // that's used to construct all child resources, and there's an extra
    // `ResourceType` argument to say what resource it is.
    let (mkauthz_func, mkauthz_arg) = match &config.authz_kind {
        AuthzKind::Generic => (
            format_ident!("child_generic"),
            quote! { ResourceType::#resource_name, },
        ),
        AuthzKind::Typed => {
            (format_ident!("{}", config.resource.name_as_snake), quote! {})
        }
    };

    quote! {
        /// Build the `authz` object for this resource
        fn make_authz(
            authz_parent: &#parent_authz_type,
            db_row: &#resource_model_type,
            lookup_type: LookupType,
        ) -> #resource_authz_type {
            authz_parent.#mkauthz_func(
                #mkauthz_arg
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
                Key::Name(parent, _) => parent.lookup_root(),
                Key::Id(root, _) => root.lookup_root(),
            }
        }
    }
}

/// Generates the lookup-related methods, including the public ones (`fetch()`,
/// `fetch_for()`, and `lookup_for()`) and the private helper (`lookup()`).
fn generate_lookup_methods(config: &Config) -> TokenStream {
    let path_authz_names = &config.path_authz_names;
    let path_authz_types = &config.path_authz_types;
    let resource_authz_name = &config.resource.authz_name;
    let resource_model_type = &config.resource.model_type;
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

    quote! {
        /// Fetch the record corresponding to the selected resource
        ///
        /// This is equivalent to `fetch_for(authz::Action::Read)`.
        pub async fn fetch(
            &self,
        ) -> LookupResult<(#(#path_authz_types,)* #resource_model_type)> {
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
        ) -> LookupResult<(#(#path_authz_types,)* #resource_model_type)> {
            let lookup = self.lookup_root();
            let opctx = &lookup.opctx;
            let datastore = &lookup.datastore;

            match &self.key {
                Key::Name(parent, name) => {
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
        ) -> LookupResult<(#(#path_authz_types,)*)> {
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
        ) -> LookupResult<(#(#path_authz_types,)*)> {
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
    let resource_authz_type = &config.resource.authz_type;
    let resource_authz_name = &config.resource.authz_name;
    let resource_model_type = &config.resource.model_type;
    let resource_as_snake = format_ident!("{}", &config.resource.name_as_snake);
    let path_authz_names = &config.path_authz_names;
    let path_authz_types = &config.path_authz_types;
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
        let parent_authz_type = &p.authz_type;
        let parent_id = format_ident!("{}_id", &p.name_as_snake);
        (
            quote! { #parent_authz_name: &#parent_authz_type, },
            quote! { #parent_authz_name, },
            quote! {
                let (#(#ancestors_authz_names,)* _) =
                    #parent_resource_name::lookup_by_id_no_authz(
                        _opctx, datastore, db_row.#parent_id
                    ).await?;
            },
            quote! { .filter(dsl::#parent_id.eq(#parent_authz_name.id())) },
            quote! { #parent_authz_name },
        )
    } else {
        (quote! {}, quote! {}, quote! {}, quote! {}, quote! { &authz::FLEET })
    };

    quote! {
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
            #parent_lookup_arg_formal
            name: &Name,
            action: authz::Action,
        ) -> LookupResult<(#resource_authz_type, #resource_model_type)> {
            let (#resource_authz_name, db_row) = Self::lookup_by_name_no_authz(
                opctx,
                datastore,
                #parent_lookup_arg_actual
                name
            ).await?;
            opctx.authorize(action, &#resource_authz_name).await?;
            Ok((#resource_authz_name, db_row))
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
            #parent_lookup_arg_formal
            name: &Name,
        ) -> LookupResult<(#resource_authz_type, #resource_model_type)> {
            use db::schema::#resource_as_snake::dsl;

            // TODO-security See the note about pool_authorized() below.
            let conn = datastore.pool();
            dsl::#resource_as_snake
                .filter(dsl::time_deleted.is_null())
                .filter(dsl::name.eq(name.clone()))
                #lookup_filter
                .select(#resource_model_type::as_select())
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
                        #parent_authz_value,
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
        ) -> LookupResult<(#(#path_authz_types,)* #resource_model_type)> {
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
            _opctx: &OpContext,
            datastore: &DataStore,
            id: Uuid,
        ) -> LookupResult<(#(#path_authz_types,)* #resource_model_type)> {
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
                .select(#resource_model_type::as_select())
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

#[cfg(test)]
#[test]
fn test_lookup_dump() {
    let output = lookup_resource(
        quote! {
            name = "Organization",
            ancestors = [],
            children = [ "Project" ],
            authz_kind = Typed
        }
        .into(),
    )
    .unwrap();
    println!("{}", output);

    let output = lookup_resource(
        quote! {
            name = "Project",
            ancestors = ["Organization"],
            children = [ "Disk", "Instance" ],
            authz_kind = Typed
        }
        .into(),
    )
    .unwrap();
    println!("{}", output);
}
