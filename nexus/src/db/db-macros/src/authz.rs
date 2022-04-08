// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for generating authz structures and related impls
//!
//! See nexus/src/authz/api_resources.rs

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Arguments for [`authz_resource!`]
// NOTE: this is only "pub" for the `cargo doc` link on [`authz_resource!`].
#[derive(serde::Deserialize)]
pub struct Input {
    /// Name of the resource
    name: String,
    /// Name of the parent resource
    parent: String,
    /// Whether roles are allowed to be attached to this resource
    roles_allowed: bool,
    /// Type of the primary key for this resource
    primary_key_type: String,
}

/// Implementation of [`authz_resource!`]
pub fn authz_resource(
    raw_input: TokenStream,
) -> Result<TokenStream, syn::Error> {
    let input = serde_tokenstream::from_tokenstream::<Input>(&raw_input)?;
    let resource_name = format_ident!("{}", input.name);
    let parent_resource_name = format_ident!("{}", input.parent);
    let primary_key_type = format_ident!("{}", input.primary_key_type);
    let parent_as_snake = heck::AsSnakeCase(input.parent).to_string();

    Ok(quote! {
        struct #resource_name {
            parent: #parent_resource_name,
            key: #primary_key_type,
            lookup_type: LookupType,
        }

        impl #resource_name {
            fn new(
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
        }

        impl ApiResourceNew for #resource_name {
            const RESOURCE_TYPE: ResourceType = ResourceType::#resource_name;
            type PrimaryKey = #primary_key_type;
            type Parent = #paent_resource_name;

            fn key(&self) -> &Self::PrimaryKey { &self.key }
            fn parent(&self) -> &Self::Parent { &self.parent }
            fn lookup_type(&self) -> &LookupType { &self.lookup_type }
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
                        | {
                            actor.has_role_resource(
                                ResourceType::#resource_name,
                                r.key(),
                                &role,
                            )
                        },
                    ).
                    .add_attribute_getter(
                        #parent_as_snake,
                        |r: &#resource_name| r.parent.clone()
                    )
            }
        }
    })
}
