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
}

/// Implementation of [`authz_resource!`]
pub fn authz_resource(
    raw_input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input = serde_tokenstream::from_tokenstream::<Input>(&raw_input)?;
    let resource_name = format_ident!("{}", input.name);
    let parent_resource_name = format_ident!("{}", input.parent);
    let parent_as_snake = heck::AsSnakeCase(input.parent).to_string();
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

    let polar_snippet_name = format_ident!(
        "{}_POLAR",
        heck::AsShoutySnakeCase(input.name).to_string()
    );
    // XXX-dap concrete proposed steps:
    //
    // - let's not try to change how this works with this change.  most
    //   importantly: it'll still be the case that things inside a Project
    //   "roll up" to the Project-level roles, rather than having their own
    // - to make this possible, let's say that the macro requires another input,
    //   which is a PolarSnippet enum, which is one of "Custom" and
    //   "ProjectResource".  It uses this to generate one of three Polar
    //   snippets for this resource:
    //   - Custom: generates nothing -- assume the real content is in the
    //     hardcoded omicron.polar file.  We'll use this for everything that's
    //     not inside a Project.
    //   - ProjectResource, when parent = "Project"
    //   - ProjectResource, when parent != "Project": define a relation between
    //     this thing and containing_project that relies on the _parent's_
    //     relation to the containing project.  Permissions are defined in terms
    //     of this relationship.  (there's still a separate relationship to the
    //     parent.)
    // - to make initialization less nightmarish, we could define a trait that's
    //   a subtrait of "oso::PolarClass" that has one thing: a const String
    //   Polar snippet (or maybe an Option).  Then during initialization, we
    //   have a Vec of these, and for each one, we register the class _and_
    //   append its snippet.
    //
    // XXX-dap There are some problems with the Polar snippet here:
    // - This is currently intended for things inside a project, though
    //   we also use it for Racks, Users, and Roles.  We *don't* use it for
    //   Sleds -- see the comment in omicron.polar about why
    // - One difference from what was before this change: for all the resources
    //   underneath Projects, we're defining roles on them.  If we didn't do
    //   this, then things two levels beneath Projects wouldn't work -- the
    //   rules for VpcSubnets will reference various roles on the parent, so Vpc
    //   needs to have those roles.  An alternative would be: for things inside
    //   a Project, we define a relationship between the thing and its
    //   containing Project.  We could define that as:
    //   - at the level immediately beneath Project: resource.project = project
    //   - at the levels below that: has_relation(project, "containing_project",
    //     resource.$parent).
    //   This seems truer to what we're really intending.
    //
    // All of this, plus the note in omicron.polar, suggest that this Polar
    // snippet needs to be a little more configurable in some way.  We have a
    // few different cases:
    //
    // - [Silo, ]Fleet, Organization, Project: probably want this to be fully
    //   custom
    // - things immediately inside the Project: fully generated based on Project
    //   roles
    // - things further below the Project: fully generated based on Project
    //   roles, but different from the previous category (maybe just in the
    //   implementation of `has_relation(_, "containing_project", _)`.
    // - things outside of the main hierarchy (roles, users, sleds, racks)
    //   - Should these be fully custom too?  It's not yet clear what the
    //     patterns are going to be for these.
    //
    // That means we somehow have to know this with the input to this macro.
    //
    // Also worth asking at this point: is the special-casing of Project roles
    // actually helping us at this point?  _Would_ it be better to say in this
    // policy file that roles can exist for every resource and roles are
    // inherited as below?  If we go _this_ route, we should definitely think
    // about whether the composition makes sense the way we've defined it (i.e.,
    // is it _always_ true that you should get "modify" if you have "modify" on
    // the parent?)
    let polar_snippet = format!(
        r#"
        resource {} {{
        	permissions = [
        		"list_children",
        		"modify",
        		"read",
        		"create_child",
        	];

                roles = [ "admin", "collaborator", "viewer" ];
                "admin" if "admin" on "parent";
                "collaborator" if "collaborator" on "parent";
                "viewer" if "viewer" on "parent";

        	relations = {{ parent: {} }};
        	"list_children" if "viewer" on "parent";
        	"read" if "viewer" on "parent";
        
        	"modify" if "collaborator" on "parent";
        	"create_child" if "collaborator" on "parent";
        }}

        has_relation(parent: {}, "parent", child: {})
        	if child.{} = parent;
    "#,
        resource_name,
        parent_resource_name,
        parent_resource_name,
        resource_name,
        parent_as_snake,
    );

    Ok(quote! {
        pub const #polar_snippet_name: &str = #polar_snippet;

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
            roles_allowed = false
        }
        .into(),
    )
    .unwrap();
    println!("{}", output);
}
