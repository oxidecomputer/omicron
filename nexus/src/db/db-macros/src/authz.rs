// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating authz structures and related impls
//!
//! See nexus/src/authz/api_resources.rs

// XXX-dap consider whether this should be upleveled to `resource-macros`?

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_tokenstream::ParseWrapper;

/// Arguments for [`authz_resource!`]
// NOTE: this is only "pub" for the `cargo doc` link on [`authz_resource!`].
#[derive(serde::Deserialize)]
pub struct Input {
    /// Name of the resource
    name: String,
    /// Name of the parent resource
    parent: String,
    /// Rust type for the primary key for this resource
    primary_key: ParseWrapper<syn::Type>,
    /// Whether roles may be attached directly to this resource
    roles_allowed: bool,
    /// How to generate the Polar snippet for this resource
    polar_snippet: PolarSnippet,
}

/// How to generate the Polar snippet for this resource
#[derive(serde::Deserialize)]
enum PolarSnippet {
    /// Don't generate it at all -- it's generated elsewhere
    Custom,

    /// Generate it as a global resource, manipulable only to administrators
    FleetChild,

    /// Generate it as a resource nested within a Project (either directly or
    /// indirectly)
    InProject,
}

/// Implementation of [`authz_resource!`]
pub fn authz_resource(
    raw_input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input = serde_tokenstream::from_tokenstream::<Input>(&raw_input)?;
    let resource_name = format_ident!("{}", input.name);
    let parent_resource_name = format_ident!("{}", input.parent);
    let parent_as_snake = heck::AsSnakeCase(&input.parent).to_string();
    let primary_key_type = &*input.primary_key;
    let (has_role_body, db_resource_body) = if input.roles_allowed {
        (
            quote! { actor.has_role_resource(ResourceType::#resource_name, r.key, &role) },
            quote! { Some((ResourceType::#resource_name, self.key)) },
        )
    } else {
        (quote! { false }, quote! { None })
    };
    // XXX-dap fail if roles_allowed and primary_key != Uuid

    let polar_snippet = match (input.polar_snippet, input.parent.as_str()) {
        (PolarSnippet::Custom, _) => String::new(),

        // The FleetChild case is similar to the InProject case, but we require
        // a different role (and, of course, the parent is the Fleet)
        (PolarSnippet::FleetChild, _) => format!(
            r#"
                resource {} {{
                    permissions = [
                        "list_children",
                        "modify",
                        "read",
                        "create_child",
                    ];
                    
                    relations = {{ parent_fleet: Fleet }};
                    "list_children" if "viewer" on "parent_fleet";
                    "read" if "viewer" on "parent_fleet";
                    "modify" if "admin" on "parent_fleet";
                    "create_child" if "admin" on "parent_fleet";
                }}
                has_relation(fleet: Fleet, "parent_fleet", child: {})
                    if child.fleet = fleet;
            "#,
            resource_name, resource_name,
        ),

        (PolarSnippet::InProject, "Project") => format!(
            r#"
                resource {} {{
                    permissions = [
                        "list_children",
                        "modify",
                        "read",
                        "create_child",
                    ];

                    relations = {{ containing_project: Project }};
                    "list_children" if "viewer" on "containing_project";
                    "read" if "viewer" on "containing_project";
                    "modify" if "collaborator" on "containing_project";
                    "create_child" if "collaborator" on "containing_project";
                }}

                has_relation(parent: Project, "containing_project", child: {})
                        if child.project = parent;
            "#,
            resource_name, resource_name,
        ),

        (PolarSnippet::InProject, _) => format!(
            r#"
                resource {} {{
                    permissions = [
                        "list_children",
                        "modify",
                        "read",
                        "create_child",
                    ];

                    relations = {{
                        containing_project: Project,
                        parent: {}
                    }};
                    "list_children" if "viewer" on "containing_project";
                    "read" if "viewer" on "containing_project";
                    "modify" if "collaborator" on "containing_project";
                    "create_child" if "collaborator" on "containing_project";
                }}

                has_relation(project: Project, "containing_project", child: {})
                    if has_relation(project, "containing_project", child.{});

                has_relation(parent: {}, "parent", child: {})
                    if child.{} = parent;
            "#,
            resource_name,
            parent_resource_name,
            resource_name,
            parent_as_snake,
            parent_resource_name,
            resource_name,
            parent_as_snake,
        ),
    };

    Ok(quote! {
        #[derive(Clone, Debug)]
        pub struct #resource_name {
            parent: #parent_resource_name,
            key: #primary_key_type,
            lookup_type: LookupType,
        }

        impl #resource_name {
            pub fn new(
                parent: #parent_resource_name,
                key: #primary_key_type,
                lookup_type: LookupType,
            ) -> #resource_name {
                #resource_name {
                    parent,
                    key,
                    lookup_type,
                }
            }

            pub fn id(&self) -> #primary_key_type {
                self.key.clone()
            }

            pub(super) fn init() -> Init {
                use oso::PolarClass;
                Init {
                    polar_snippet: #polar_snippet,
                    polar_class: #resource_name::get_polar_class(),
                }
            }
        }

        impl Eq for #resource_name {}
        impl PartialEq for #resource_name {
            fn eq(&self, other: &Self) -> bool {
                self.key == other.key
            }
        }

        impl oso::PolarClass for #resource_name {
            fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
                oso::Class::builder()
                    .with_equality_check()
                    .add_method(
                        "has_role",
                        |
                            r: &#resource_name,
                            actor: AuthenticatedActor,
                            role: String
                        | { #has_role_body },
                    )
                    .add_attribute_getter(
                        #parent_as_snake,
                        |r: &#resource_name| r.parent.clone()
                    )
            }
        }

        impl ApiResource for #resource_name {
            fn db_resource(&self) -> Option<(ResourceType, Uuid)> {
                #db_resource_body
            }

            fn parent(&self) -> Option<&dyn AuthorizedResource> {
                Some(&self.parent)
            }
        }

        impl ApiResourceError for #resource_name {
            fn not_found(&self) -> Error {
                self.lookup_type
                    .clone()
                    .into_not_found(ResourceType::#resource_name)
            }
        }
    })
}

// See the test for lookup_resource.
#[cfg(test)]
#[test]
fn test_authz_dump() {
    let output = authz_resource(
        quote! {
            name = "Organization",
            parent = "Fleet",
            primary_key = Uuid,
            roles_allowed = false,
            polar_snippet = Custom,
        }
        .into(),
    )
    .unwrap();
    println!("{}", output);
}
