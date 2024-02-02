// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authz-related macro implementations

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_tokenstream::ParseWrapper;

/// Defines a structure and helpers for describing an API resource for authz
///
/// For context, see the module-level documentation for `omicron-nexus::authz`.
///
/// See [`Input`] for arguments to this macro.
///
/// # Examples
///
/// ## Resource that users can directly share with other users
///
/// This example generates `authz::Organization`:
///
/// ```ignore
/// authz_resource! {
///     name = "Organization",
///     parent = "Fleet",
///     primary_key = Uuid,
///     roles_allowed = true,
///     polar_snippet = Custom,
/// }
/// ```
///
/// This is a pretty high-level resource and users will be allowed to assign
/// roles directly to it.  The Polar snippet for it is totally custom and
/// contained in the "omicron.polar" base file.
///
/// ## Resource within a Project
///
/// We do not yet support assigning roles to resources below the Project level.
/// For these, we use an auto-generated "in-project" Polar snippet:
///
/// ```ignore
/// authz_resource! {
///     name = "Instance",
///     parent = "Project",
///     primary_key = Uuid,
///     roles_allowed = false,
///     polar_snippet = InProject,
/// }
/// ```
///
/// It's the same for resources whose parent is not "Project", but something
/// else that itself is under a Project:
///
/// ```ignore
/// authz_resource! {
///     name = "VpcRouter",
///     parent = "Vpc",
///     primary_key = Uuid,
///     roles_allowed = false,
///     polar_snippet = InProject,
/// }
/// ```
///
/// ## Resources outside the Organization / Project hierarchy
///
/// Many resources today are not part of the main Organization / Project
/// hierarchy.  In some cases it's still TBD how we intend to structure the
/// roles for these resources.  They generally live directly under the `Fleet`
/// and require "fleet.admin" to do anything with them.  Here's an example:
///
/// ```ignore
/// authz_resource! {
///     name = "Rack",
///     parent = "Fleet",
///     primary_key = Uuid,
///     roles_allowed = false,
///     polar_snippet = FleetChild,
/// }
/// ```
///
/// ## Resources with non-id primary keys
///
/// Most API resources use "id" (a Uuid) as an immutable, unique identifier.
/// Some don't, though, and that's supported too:
///
/// ```ignore
/// authz_resource! {
///     name = "Role",
///     parent = "Fleet",
///     primary_key = (String, String),
///     roles_allowed = false,
///     polar_snippet = FleetChild,
/// }
/// ```
///
/// In some cases, it may be more convenient to identify a composite key with a
/// struct rather than relying on tuples. This is supported too:
///
/// ```ignore
/// struct SomeCompositeId {
///     foo: String,
///     bar: String,
/// }
///
/// // There needs to be a `From` impl from the composite ID to the primary key.
/// impl From<SomeCompositeId> for (String, String) {
///     fn from(id: SomeCompositeId) -> Self {
///         (id.foo, id.bar)
///     }
/// }
///
/// authz_resource! {
///     name = "MyResource",
///     parent = "Fleet",
///     primary_key = (String, String),
///     input_key = SomeCompositeId,
///     roles_allowed = false,
///     polar_snippet = FleetChild,
/// }
/// ```

// Allow private intra-doc links.  This is useful because the `Input` struct
// cannot be exported (since we're a proc macro crate, and we can't expose
// a struct), but its documentation is very useful.
#[allow(rustdoc::private_intra_doc_links)]
#[proc_macro]
pub fn authz_resource(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match do_authz_resource(input.into()) {
        Ok(output) => output.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Arguments for [`authz_resource!`]
#[derive(serde::Deserialize)]
struct Input {
    /// Name of the resource
    ///
    /// This much match a corresponding variant of the `ResourceType` enum.
    /// It's usually PascalCase.
    name: String,
    /// Name of the parent `authz` resource
    parent: String,
    /// Rust type for the primary key for this resource
    primary_key: ParseWrapper<syn::Type>,
    /// Rust type for the input key for this resource (the key users specify
    /// for this resource, convertible to `primary_key`).
    ///
    /// This is the same as primary_key if not specified.
    #[serde(default)]
    input_key: Option<ParseWrapper<syn::Type>>,
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

    /// Generate it as resource nested under the Silo
    InSilo,

    /// Generate it as a resource nested within a Project (either directly or
    /// indirectly)
    InProject,
}

/// Implementation of [`authz_resource!`]
fn do_authz_resource(
    raw_input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input = serde_tokenstream::from_tokenstream::<Input>(&raw_input)?;
    let resource_name = format_ident!("{}", input.name);
    let parent_resource_name = format_ident!("{}", input.parent);
    let parent_as_snake = heck::AsSnakeCase(&input.parent).to_string();
    let primary_key_type = &*input.primary_key;
    let input_key_type =
        &**input.input_key.as_ref().unwrap_or(&input.primary_key);

    let (has_role_body, as_roles_body, api_resource_roles_trait) =
        if input.roles_allowed {
            (
                quote! {
                    actor.has_role_resource(
                        ResourceType::#resource_name,
                        r.key,
                        &role
                    )
                },
                quote! { Some(self) },
                quote! {
                    impl ApiResourceWithRoles for #resource_name {
                        fn resource_id(&self) -> Uuid {
                            self.key
                        }

                        fn conferred_roles_by(
                            &self,
                            _authn: &authn::Context,
                        ) ->
                            Result<
                                Option<(
                                    ResourceType,
                                    Uuid,
                                )>,
                                Error,
                            >
                        {
                            Ok(None)
                        }

                    }
                },
            )
        } else {
            (quote! { false }, quote! { None }, quote! {})
        };

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

        // If this resource is directly inside a Silo, we only need to define
        // permissions that are contingent on having roles on that Silo.
        (PolarSnippet::InSilo, _) => format!(
            r#"
                resource {} {{
                    permissions = [
                        "list_children",
                        "modify",
                        "read",
                        "create_child",
                    ];

                    relations = {{ containing_silo: Silo }};
                    "list_children" if "viewer" on "containing_silo";
                    "read" if "viewer" on "containing_silo";
                    "modify" if "collaborator" on "containing_silo";
                    "create_child" if "collaborator" on "containing_silo";
                }}

                has_relation(parent: Silo, "containing_silo", child: {})
                    if child.silo = parent;
            "#,
            resource_name, resource_name,
        ),

        // If this resource is directly inside a Project, we only need to define
        // permissions that are contingent on having roles on that Project.
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

        // If this resource is nested under something else within the Project,
        // we need to define both the "parent" relationship and the (indirect)
        // relationship to the containing Project.  Permissions are still
        // contingent on having roles on the Project, but to get to the Project,
        // we have to go through the parent resource.
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

    let doc_struct = format!(
        "`authz` type for a resource of type {}\
        \
        Used to uniquely identify a resource of type {} across renames, moves, \
        etc., and to do authorization checks (see  \
        [`crate::context::OpContext::authorize()`]).  See [`crate::authz`] \
        module-level documentation for more information.",
        resource_name, resource_name,
    );

    Ok(quote! {
        #[doc = #doc_struct]
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct #resource_name {
            parent: #parent_resource_name,
            key: #primary_key_type,
            lookup_type: LookupType,
        }

        impl #resource_name {
            /// Makes a new `authz` struct for this resource with the given
            /// `parent`, unique key `key`, looked up as described by
            /// `lookup_type`
            pub fn new(
                parent: #parent_resource_name,
                key: #input_key_type,
                lookup_type: LookupType,
            ) -> #resource_name {
                #resource_name {
                    parent,
                    key: key.into(),
                    lookup_type,
                }
            }

            /// A version of `new` that takes the primary key type directly.
            /// This is only different from [`Self::new`] if this resource
            /// uses a different input key type.
            pub fn with_primary_key(
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
                self.key.clone().into()
            }

            /// Describes how to register this type with Oso
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
            fn parent(&self) -> Option<&dyn AuthorizedResource> {
                Some(&self.parent)
            }

            fn resource_type(&self) -> ResourceType {
                ResourceType::#resource_name
            }

            fn lookup_type(&self) -> &LookupType {
                &self.lookup_type
            }

            fn as_resource_with_roles(
                &self,
            ) -> Option<&dyn ApiResourceWithRoles> {
                #as_roles_body
            }
        }

        #api_resource_roles_trait
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use expectorate::assert_contents;

    /// Ensures that generated code is as expected.
    ///
    /// For more information, see `test_lookup_snapshots` in
    /// nexus/db-macros/src/lookup.rs.
    #[test]
    fn test_authz_snapshots() {
        let output = do_authz_resource(quote! {
            name = "Organization",
            parent = "Fleet",
            primary_key = Uuid,
            roles_allowed = false,
            polar_snippet = Custom,
        })
        .unwrap();
        assert_contents("outputs/organization.txt", &pretty_format(output));

        let output = do_authz_resource(quote! {
            name = "Instance",
            parent = "Project",
            primary_key = (String, String),
            // The SomeCompositeId type doesn't exist, but that's okay because
            // this code is never compiled, just printed out.
            input_key = SomeCompositeId,
            roles_allowed = false,
            polar_snippet = InProject,
        })
        .unwrap();
        assert_contents("outputs/instance.txt", &pretty_format(output));
    }

    fn pretty_format(input: TokenStream) -> String {
        let parsed = syn::parse2(input).unwrap();
        prettyplease::unparse(&parsed)
    }
}
