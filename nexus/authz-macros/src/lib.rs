// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Authz-related macro implementations

extern crate proc_macro;

use nexus_macros_common::PrimaryKeyType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_tokenstream::ParseWrapper;
use syn::parse_quote;

// Allow private intra-doc links.  This is useful because the `Input` struct
// cannot be exported (since we're a proc macro crate, and we can't expose
// a struct), but its documentation is very useful.
#[allow(rustdoc::private_intra_doc_links)]
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
/// ## Resources with typed UUID primary keys
///
/// Some resources use a [newtype_uuid](https://crates.io/crates/newtype_uuid)
/// `TypedUuid` as their primary key (and resources should generally move over
/// to that model).
///
/// This can be specified with `primary_key = { uuid_kind = MyKind }`:
///
/// ```ignore
/// authz_resource! {
///     name = "LoopbackAddress",
///     parent = "Fleet",
///     primary_key = { uuid_kind = LoopbackAddressKind },
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
#[derive(serde::Deserialize, Debug)]
struct Input {
    /// Name of the resource
    ///
    /// This much match a corresponding variant of the `ResourceType` enum.
    /// It's usually PascalCase.
    name: String,
    /// Name of the parent `authz` resource
    parent: String,
    /// Rust type for the primary key for this resource.
    primary_key: InputPrimaryKeyType,
    /// The `TypedUuidKind` for this resource. Must be exclusive
    ///
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

#[derive(Debug)]
struct InputPrimaryKeyType(PrimaryKeyType);

impl<'de> serde::Deserialize<'de> for InputPrimaryKeyType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Attempt to parse as either a string or a map.
        struct PrimaryKeyVisitor;

        impl<'de2> serde::de::Visitor<'de2> for PrimaryKeyVisitor {
            type Value = PrimaryKeyType;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter.write_str(
                    "a Rust type, or a map with a single key `uuid_kind`",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                syn::parse_str(value)
                    .map(PrimaryKeyType::Standard)
                    .map_err(|e| E::custom(e.to_string()))
            }

            // seq represents a tuple type
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de2>,
            {
                let mut elements = vec![];
                while let Some(element) =
                    seq.next_element::<ParseWrapper<syn::Type>>()?
                {
                    elements.push(element.into_inner());
                }
                Ok(PrimaryKeyType::Standard(parse_quote!((#(#elements,)*))))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de2>,
            {
                let key: String = map.next_key()?.ok_or_else(|| {
                    serde::de::Error::custom("expected a single key")
                })?;
                if key == "uuid_kind" {
                    // uuid kinds must be plain identifiers
                    let value: ParseWrapper<syn::Ident> = map.next_value()?;
                    Ok(PrimaryKeyType::new_typed_uuid(&value))
                } else {
                    Err(serde::de::Error::custom(
                        "expected a single key `uuid_kind`",
                    ))
                }
            }
        }

        deserializer.deserialize_any(PrimaryKeyVisitor).map(InputPrimaryKeyType)
    }
}

/// How to generate the Polar snippet for this resource
#[derive(serde::Deserialize, Debug)]
enum PolarSnippet {
    /// Don't generate it at all -- it's generated elsewhere
    Custom,

    /// Generate it as a global resource, manipulable only to administrators
    FleetChild,

    /// Generate it as resource nested under the Silo
    InSilo,

    /// Generate it as a resource nested within a Project (either directly or
    /// indirectly). Grants modify/create permissions to `limited-collaborator`
    /// and above.
    InProjectLimited,

    /// Generate it as a resource nested within a Project.
    /// Requires the full `collaborator` role (not `limited-collaborator`)
    /// to modify or create these resources.
    InProjectFull,
}

/// Implementation of [`authz_resource!`]
fn do_authz_resource(
    raw_input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input = serde_tokenstream::from_tokenstream::<Input>(&raw_input)?;
    let resource_name = format_ident!("{}", input.name);
    let parent_resource_name = format_ident!("{}", input.parent);
    let parent_as_snake = heck::AsSnakeCase(&input.parent).to_string();
    let primary_key_type = input.primary_key.0.external();
    let input_key_type = input.input_key.as_deref().unwrap_or(primary_key_type);

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

        // The FleetChild case is similar to the InProject* cases, but we require
        // a different role (admin) and the parent is the Fleet instead of Project
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
        (PolarSnippet::InProjectLimited, "Project") => format!(
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
                    "modify" if "limited-collaborator" on "containing_project";
                    "create_child" if "limited-collaborator" on "containing_project";
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
        (PolarSnippet::InProjectLimited, _) => format!(
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
                    "modify" if "limited-collaborator" on "containing_project";
                    "create_child" if "limited-collaborator" on "containing_project";
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

        // InProjectFull: Like InProjectLimited, but modifying these things
        // requires "collaborator".
        (PolarSnippet::InProjectFull, "Project") => format!(
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
        (PolarSnippet::InProjectFull, _) => format!(
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
            polar_snippet = InProjectLimited,
        })
        .unwrap();
        assert_contents("outputs/instance.txt", &pretty_format(output));

        let output = do_authz_resource(quote! {
            name = "Rack",
            parent = "Fleet",
            primary_key = { uuid_kind = RackKind },
            roles_allowed = false,
            polar_snippet = FleetChild,
        })
        .unwrap();
        assert_contents("outputs/rack.txt", &pretty_format(output));
    }

    fn pretty_format(input: TokenStream) -> String {
        let parsed = syn::parse2(input).unwrap();
        prettyplease::unparse(&parsed)
    }
}
