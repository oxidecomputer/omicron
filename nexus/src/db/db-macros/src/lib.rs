// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database-related macro implementations.
//!
//! Diesel provides facilities for mapping structures to SQL tables, but these
//! often require additional layers of structures to be usable. This crate
//! provides support for auto-generating structures that are common among many
//! tables.

// Copyright 2021 Oxide Computer Company

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DataStruct, DeriveInput, Error, Fields, Ident, Lit, Meta};

mod lookup;

/// Looks for a Meta-style attribute with a particular identifier.
///
/// As an example, for an attribute like `#[foo = "bar"]`, we can find this
/// attribute by calling `get_meta_attr(&item.attrs, "foo")`.
fn get_meta_attr(attrs: &[syn::Attribute], name: &str) -> Option<Meta> {
    attrs
        .iter()
        .filter_map(|attr| attr.parse_meta().ok())
        .find(|meta| meta.path().is_ident(name))
}

/// Accesses the "value" part of a name-value Meta attribute.
fn get_attribute_value(meta: &Meta) -> Option<&Lit> {
    if let Meta::NameValue(ref nv) = meta {
        Some(&nv.lit)
    } else {
        None
    }
}

/// Looks up a named field within a struct.
fn get_field_with_name<'a>(
    data: &'a DataStruct,
    name: &str,
) -> Option<&'a syn::Field> {
    if let Fields::Named(ref data_fields) = data.fields {
        data_fields.named.iter().find(|field| {
            if let Some(ident) = &field.ident {
                ident == name
            } else {
                false
            }
        })
    } else {
        None
    }
}

// Describes which derive macro is being used; allows sharing common code.
enum IdentityVariant {
    Asset,
    Resource,
}

/// Implements the "Resource" trait, and generates a bespoke Identity struct.
///
/// Many tables within our database make use of common fields,
/// including:
/// - ID
/// - Name
/// - Description
/// - Time Created
/// - Time Modified
/// - Time Deleted.
///
/// Although these fields can be refactored into a common structure (to be used
/// within the context of Diesel) they must be uniquely identified for a single
/// table.
#[proc_macro_derive(Resource)]
pub fn resource_target(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    derive_impl(input.into(), IdentityVariant::Resource)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Identical to [`macro@Resource`], but generates fewer fields.
///
/// Contains:
/// - ID
/// - Time Created
/// - Time Modified
#[proc_macro_derive(Asset)]
pub fn asset_target(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_impl(input.into(), IdentityVariant::Asset)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

// Implementation of `#[derive(Resource)]` and `#[derive(Asset)]`.
fn derive_impl(
    tokens: TokenStream,
    flavor: IdentityVariant,
) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    let name = &item.ident;

    // Ensure that the "table_name" attribute exists, and get it.
    let table_meta =
        get_meta_attr(&item.attrs, "table_name").ok_or_else(|| {
            Error::new(
                item.span(),
                format!(
                    "Resource needs 'table_name' attribute.\n\
                     Try adding #[table_name = \"your_table_name\"] to {}.",
                    name
                ),
            )
        })?;
    let table_name = get_attribute_value(&table_meta)
        .ok_or_else(|| {
            Error::new(
                item.span(),
                "'table_name' needs to be a name-value pair, like #[table_name = foo]"
            )
        })?;

    // Ensure that a field named "identity" exists within this struct.
    if let Data::Struct(ref data) = item.data {
        // We extract type of "identity" and enforce it is the expected type
        // using injected traits.
        let field = get_field_with_name(data, "identity")
            .ok_or_else(|| {
                Error::new(
                    item.span(),
                    format!(
                        "{name}Identity must be embedded within {name} as a field named `identity`.\n\
                         This proc macro will try to add accessor methods to {name}; this can only be\n\
                         accomplished if we know where to access them.",
                        name=name,
                    )
                )
            })?;

        return Ok(build(name, table_name, &field.ty, flavor));
    }

    Err(Error::new(item.span(), "Resource can only be derived for structs"))
}

// Emits generated structures, depending on the requested flavor of identity.
fn build(
    struct_name: &Ident,
    table_name: &Lit,
    observed_identity_ty: &syn::Type,
    flavor: IdentityVariant,
) -> TokenStream {
    let (identity_struct, resource_impl) = {
        match flavor {
            IdentityVariant::Resource => (
                build_resource_identity(struct_name, table_name),
                build_resource_impl(struct_name, observed_identity_ty),
            ),
            IdentityVariant::Asset => (
                build_asset_identity(struct_name, table_name),
                build_asset_impl(struct_name, observed_identity_ty),
            ),
        }
    };
    quote! {
        #identity_struct
        #resource_impl
    }
}

// Builds an "Identity" structure for a resource.
fn build_resource_identity(
    struct_name: &Ident,
    table_name: &Lit,
) -> TokenStream {
    let identity_doc = format!(
        "Auto-generated identity for [`{}`] from deriving [`macro@Resource`].",
        struct_name,
    );
    let identity_name = format_ident!("{}Identity", struct_name);
    quote! {
        #[doc = #identity_doc]
        #[derive(Clone, Debug, PartialEq, Selectable, Queryable, Insertable, serde::Serialize, serde::Deserialize)]
        #[table_name = #table_name ]
        pub struct #identity_name {
            pub id: ::uuid::Uuid,
            pub name: crate::db::model::Name,
            pub description: ::std::string::String,
            pub time_created: ::chrono::DateTime<::chrono::Utc>,
            pub time_modified: ::chrono::DateTime<::chrono::Utc>,
            pub time_deleted: ::std::option::Option<chrono::DateTime<chrono::Utc>>,
        }

        impl #identity_name {
            pub fn new(
                id: ::uuid::Uuid,
                params: ::omicron_common::api::external::IdentityMetadataCreateParams
            ) -> Self {
                let now = ::chrono::Utc::now();
                Self {
                    id,
                    name: params.name.into(),
                    description: params.description,
                    time_created: now,
                    time_modified: now,
                    time_deleted: None,
                }
            }
        }
    }
}

// Builds an "Identity" structure for an asset.
fn build_asset_identity(struct_name: &Ident, table_name: &Lit) -> TokenStream {
    let identity_doc = format!(
        "Auto-generated identity for [`{}`] from deriving [`macro@Asset`].",
        struct_name,
    );
    let identity_name = format_ident!("{}Identity", struct_name);
    quote! {
        #[doc = #identity_doc]
        #[derive(Clone, Debug, PartialEq, Selectable, Queryable, Insertable, serde::Serialize, serde::Deserialize)]
        #[table_name = #table_name ]
        pub struct #identity_name {
            pub id: ::uuid::Uuid,
            pub time_created: ::chrono::DateTime<::chrono::Utc>,
            pub time_modified: ::chrono::DateTime<::chrono::Utc>,
        }

        impl #identity_name {
            pub fn new(
                id: ::uuid::Uuid,
            ) -> Self {
                let now = ::chrono::Utc::now();
                Self {
                    id,
                    time_created: now,
                    time_modified: now,
                }
            }
        }
    }
}

// Implements "Resource" for the requested structure.
fn build_resource_impl(
    struct_name: &Ident,
    observed_identity_type: &syn::Type,
) -> TokenStream {
    let identity_trait = format_ident!("__{}IdentityMarker", struct_name);
    let identity_name = format_ident!("{}Identity", struct_name);
    quote! {
        // Verify that the field named "identity" is actually the generated
        // type within the struct deriving Resource.
        trait #identity_trait {}
        impl #identity_trait for #identity_name {}
        const _: () = {
            fn assert_identity<T: #identity_trait>() {}
            fn assert_all() {
                assert_identity::<#observed_identity_type>();
            }
        };

        impl crate::db::identity::Resource for #struct_name {
            fn id(&self) -> ::uuid::Uuid {
                self.identity.id
            }

            fn name(&self) -> &crate::db::model::Name {
                &self.identity.name
            }

            fn description(&self) -> &str {
                &self.identity.description
            }

            fn time_created(&self) -> ::chrono::DateTime<::chrono::Utc> {
                self.identity.time_created
            }

            fn time_modified(&self) -> ::chrono::DateTime<::chrono::Utc> {
                self.identity.time_modified
            }

            fn time_deleted(&self) -> ::std::option::Option<::chrono::DateTime<::chrono::Utc>> {
                self.identity.time_deleted
            }
        }
    }
}

// Implements "Asset" for the requested structure.
fn build_asset_impl(
    struct_name: &Ident,
    observed_identity_type: &syn::Type,
) -> TokenStream {
    let identity_trait = format_ident!("__{}IdentityMarker", struct_name);
    let identity_name = format_ident!("{}Identity", struct_name);
    quote! {
        // Verify that the field named "identity" is actually the generated
        // type within the struct deriving Asset.
        trait #identity_trait {}
        impl #identity_trait for #identity_name {}
        const _: () = {
            fn assert_identity<T: #identity_trait>() {}
            fn assert_all() {
                assert_identity::<#observed_identity_type>();
            }
        };

        impl crate::db::identity::Asset for #struct_name {
            fn id(&self) -> ::uuid::Uuid {
                self.identity.id
            }

            fn time_created(&self) -> ::chrono::DateTime<::chrono::Utc> {
                self.identity.time_created
            }

            fn time_modified(&self) -> ::chrono::DateTime<::chrono::Utc> {
                self.identity.time_modified
            }
        }
    }
}

/// Identical to [`macro@Resource`], but generates fewer fields.
///
/// Contains:
/// - ID
/// - Time Created
/// - Time Modified
#[proc_macro_attribute]
pub fn lookup_resource(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    lookup::lookup_resource(attr, input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_metadata_identity_fails_without_table_name() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
            IdentityVariant::Resource,
        );
        assert!(out.is_err());
        assert_eq!(
            "Resource needs 'table_name' attribute.\n\
             Try adding #[table_name = \"your_table_name\"] to MyTarget.",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_with_wrong_table_name_type() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[table_name]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
            IdentityVariant::Resource,
        );
        assert!(out.is_err());
        assert_eq!(
            "'table_name' needs to be a name-value pair, like #[table_name = foo]",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_for_enums() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[table_name = "foo"]
                enum MyTarget {
                    Foo,
                    Bar,
                }
            }
            .into(),
            IdentityVariant::Resource,
        );
        assert!(out.is_err());
        assert_eq!(
            "Resource can only be derived for structs",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_without_embedded_identity() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[table_name = "my_target"]
                struct MyTarget {
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
            IdentityVariant::Resource,
        );
        assert!(out.is_err());
        assert_eq!(
            "MyTargetIdentity must be embedded within MyTarget as a field named `identity`.\n\
             This proc macro will try to add accessor methods to MyTarget; this can only be\n\
             accomplished if we know where to access them.",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_minimal_example_compiles() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[table_name = "my_target"]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
            IdentityVariant::Resource,
        );
        assert!(out.is_ok());
    }
}
