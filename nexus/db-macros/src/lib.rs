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

use nexus_macros_common::PrimaryKeyType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde_tokenstream::ParseWrapper;
use syn::spanned::Spanned;
use syn::{parse_quote, Data, DataStruct, DeriveInput, Error, Fields, Ident};

mod lookup;
#[cfg(test)]
mod test_helpers;

/// Defines a structure and helper functions for looking up resources
///
/// # Examples
///
/// ```ignore
/// lookup_resource! {
///     name = "Organization",
///     ancestors = [],
///     children = [ "Project" ],
///     lookup_by_name = true,
///     soft_deletes = true,
///     primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
/// }
/// ```
///
/// See [`lookup::Input`] for documentation on the named arguments.
///
/// This defines a struct `Organization<'a>` with functions `fetch()`,
/// `fetch_for(authz::Action)`, and `lookup_for(authz::Action)` for looking up
/// an Organization in the database.  These functions are all protected by
/// access controls.
///
/// Building on that, we have:
///
/// ```ignore
/// lookup_resource! {
///     name = "Organization",
///     ancestors = [],
///     children = [ "Project" ],
///     lookup_by_name = true,
///     soft_deletes = true,
///     primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
/// }
///
/// lookup_resource! {
///     name = "Instance",
///     ancestors = [ "Organization", "Project" ],
///     children = [],
///     lookup_by_name = true,
///     soft_deletes = true,
///     primary_key_columns = [ { column_name = "id", rust_type = Uuid } ]
/// }
/// ```
///
/// These define `Project<'a>` and `Instance<'a>`.
///
/// It is also possible to use a `TypedUuid` as a key column, by specifying
/// `uuid_kind` rather than `rust_type`. For example:
///
/// ```ignore
/// lookup_resource! {
///     name = "Sled",
///     ancestors = [ "Organization", "Project" ],
///     children = [],
///     lookup_by_name = true,
///     soft_deletes = true,
///     primary_key_columns = [ { column_name = "id", uuid_kind = SledType } ]
/// }
/// ```
///
/// For more on these structs and how they're used, see
/// nexus/db-queries/src/lookup.rs.
// Allow private intra-doc links.  This is useful because the `Input` struct
// cannot be exported (since we're a proc macro crate, and we can't expose
// a struct), but its documentation is very useful.
#[allow(rustdoc::private_intra_doc_links)]
#[proc_macro]
pub fn lookup_resource(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match lookup::lookup_resource(input.into()) {
        Ok(output) => output.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

/// Looks for a Diesel Meta-style attribute with a particular identifier.
///
/// As an example, for an attribute like `#[diesel(foo = bar)]`, we can find this
/// attribute by calling `get_nv_attr(&item.attrs, "foo")`.
fn get_diesel_nv_attr(
    attrs: &[syn::Attribute],
    name: &str,
) -> Option<NameValue> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("diesel"))
        .filter_map(|attr| attr.parse_args::<NameValue>().ok())
        .find(|nv| nv.name.is_ident(name))
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
#[derive(Clone, Copy, Debug)]
enum IdentityVariant {
    Asset,
    Resource,
}

impl IdentityVariant {
    fn attr_name(&self) -> &'static str {
        match self {
            IdentityVariant::Asset => "asset",
            IdentityVariant::Resource => "resource",
        }
    }
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
#[proc_macro_derive(Resource, attributes(resource))]
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
#[proc_macro_derive(Asset, attributes(asset))]
pub fn asset_target(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_impl(input.into(), IdentityVariant::Asset)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[derive(Debug)]
pub(crate) struct NameValue {
    name: syn::Path,
    _eq_token: syn::token::Eq,
    value: syn::Path,
}

impl syn::parse::Parse for NameValue {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self {
            name: input.parse()?,
            _eq_token: input.parse()?,
            value: input.parse()?,
        })
    }
}

// Implementation of `#[derive(Resource)]` and `#[derive(Asset)]`.
fn derive_impl(
    tokens: TokenStream,
    flavor: IdentityVariant,
) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    let name = &item.ident;

    // Ensure that the "table_name" attribute exists, and get it.
    let table_nv =
        get_diesel_nv_attr(&item.attrs, "table_name").ok_or_else(|| {
            Error::new(
                item.span(),
                format!(
                    "Resource needs 'table_name' attribute.\n\
                     Try adding #[diesel(table_name = your_table_name)] to {}.",
                    name
                ),
            )
        })?;
    let table_name = table_nv.value;

    let input = MacroAttributes::parse_from_attrs(&item.attrs, flavor)?;
    let uuid_ty = input.uuid_ty();

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

        return Ok(build(name, &table_name, &field.ty, &uuid_ty, flavor));
    }

    Err(Error::new(item.span(), "Resource can only be derived for structs"))
}

/// Attributes specific to the `Resource` and `Asset` derive macros.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MacroAttributes {
    /// The `TypedUuid` type parameter.
    #[serde(default)]
    uuid_kind: Option<ParseWrapper<syn::Ident>>,
}

impl MacroAttributes {
    fn parse_from_attrs(
        attrs: &[syn::Attribute],
        flavor: IdentityVariant,
    ) -> syn::Result<Self> {
        let inner_attrs = attrs
            .iter()
            .filter_map(|attr| {
                attr.path().is_ident(flavor.attr_name()).then(|| {
                    let meta_list = attr.meta.require_list()?;
                    Ok::<_, syn::Error>(&meta_list.tokens)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let tokens = quote! { #(#inner_attrs,)* };
        serde_tokenstream::from_tokenstream(&tokens)
    }

    fn uuid_ty(&self) -> PrimaryKeyType {
        self.uuid_kind.as_ref().map_or_else(
            || PrimaryKeyType::Standard(parse_quote!(::uuid::Uuid)),
            |v| PrimaryKeyType::new_typed_uuid(v),
        )
    }
}

// Emits generated structures, depending on the requested flavor of identity.
fn build(
    struct_name: &Ident,
    table_name: &syn::Path,
    observed_identity_ty: &syn::Type,
    uuid_ty: &PrimaryKeyType,
    flavor: IdentityVariant,
) -> TokenStream {
    let (identity_struct, resource_impl) = {
        match flavor {
            IdentityVariant::Resource => (
                build_resource_identity(struct_name, table_name, uuid_ty),
                build_resource_impl(struct_name, observed_identity_ty, uuid_ty),
            ),
            IdentityVariant::Asset => (
                build_asset_identity(struct_name, table_name, uuid_ty),
                build_asset_impl(struct_name, observed_identity_ty, uuid_ty),
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
    table_name: &syn::Path,
    uuid_ty: &PrimaryKeyType,
) -> TokenStream {
    let identity_doc = format!(
        "Auto-generated identity for [`{}`] from deriving [`macro@Resource`].",
        struct_name,
    );
    let identity_name = format_ident!("{}Identity", struct_name);

    let external_uuid_ty = uuid_ty.external();
    let db_uuid_ty = uuid_ty.db();
    let convert_external_to_db =
        uuid_ty.external_to_db_nexus_db_model(quote! { id });

    quote! {
        #[doc = #identity_doc]
        #[derive(Clone, Debug, PartialEq, Eq, Selectable, Queryable, Insertable, serde::Serialize, serde::Deserialize)]
        #[diesel(table_name = #table_name) ]
        pub struct #identity_name {
            pub id: #db_uuid_ty,
            pub name: crate::db::model::Name,
            pub description: ::std::string::String,
            pub time_created: ::chrono::DateTime<::chrono::Utc>,
            pub time_modified: ::chrono::DateTime<::chrono::Utc>,
            pub time_deleted: ::std::option::Option<chrono::DateTime<chrono::Utc>>,
        }

        impl #identity_name {
            pub fn new(
                id: #external_uuid_ty,
                params: ::omicron_common::api::external::IdentityMetadataCreateParams
            ) -> Self {
                let now = ::chrono::Utc::now();
                Self {
                    id: #convert_external_to_db,
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
fn build_asset_identity(
    struct_name: &Ident,
    table_name: &syn::Path,
    uuid_ty: &PrimaryKeyType,
) -> TokenStream {
    let identity_doc = format!(
        "Auto-generated identity for [`{}`] from deriving [`macro@Asset`].",
        struct_name,
    );
    let identity_name = format_ident!("{}Identity", struct_name);

    let external_uuid_ty = uuid_ty.external();
    let db_uuid_ty = uuid_ty.db();
    let convert_external_to_db =
        uuid_ty.external_to_db_nexus_db_model(quote! { id });

    quote! {
        #[doc = #identity_doc]
        #[derive(Clone, Debug, PartialEq, Selectable, Queryable, Insertable, serde::Serialize, serde::Deserialize)]
        #[diesel(table_name = #table_name) ]
        pub struct #identity_name {
            pub id: #db_uuid_ty,
            pub time_created: ::chrono::DateTime<::chrono::Utc>,
            pub time_modified: ::chrono::DateTime<::chrono::Utc>,
        }

        impl #identity_name {
            pub fn new(
                id: #external_uuid_ty,
            ) -> Self {
                let now = ::chrono::Utc::now();
                Self {
                    id: #convert_external_to_db,
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
    uuid_ty: &PrimaryKeyType,
) -> TokenStream {
    let identity_trait = format_ident!("__{}IdentityMarker", struct_name);
    let identity_name = format_ident!("{}Identity", struct_name);

    let external_uuid_ty = uuid_ty.external();
    let convert_db_to_external =
        uuid_ty.db_to_external(quote! { self.identity.id });

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

        impl ::nexus_types::identity::Resource for #struct_name {
            type IdType = #external_uuid_ty;

            fn id(&self) -> #external_uuid_ty {
                #convert_db_to_external
            }

            fn name(&self) -> &::omicron_common::api::external::Name {
                &self.identity.name.0
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
    uuid_ty: &PrimaryKeyType,
) -> TokenStream {
    let identity_trait = format_ident!("__{}IdentityMarker", struct_name);
    let identity_name = format_ident!("{}Identity", struct_name);

    let external_uuid_ty = uuid_ty.external();
    let convert_db_to_external =
        uuid_ty.db_to_external(quote! { self.identity.id });

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

        impl ::nexus_types::identity::Asset for #struct_name {
            type IdType = #external_uuid_ty;

            fn id(&self) -> #external_uuid_ty {
                #convert_db_to_external
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_helpers::pretty_format;

    use expectorate::assert_contents;

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
            },
            IdentityVariant::Resource,
        );
        assert!(out.is_err());
        assert_eq!(
            "Resource needs 'table_name' attribute.\n\
             Try adding #[diesel(table_name = your_table_name)] to MyTarget.",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_with_wrong_table_name_type() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[diesel(table_name)]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            },
            IdentityVariant::Resource,
        );
        assert!(out.is_err());
        assert_eq!(
            "Resource needs 'table_name' attribute.\n\
             Try adding #[diesel(table_name = your_table_name)] to MyTarget.",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_for_enums() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[diesel(table_name = foo)]
                enum MyTarget {
                    Foo,
                    Bar,
                }
            },
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
                #[diesel(table_name = my_target)]
                struct MyTarget {
                    name: String,
                    is_cool: bool,
                }
            },
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
                #[diesel(table_name = my_target)]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            },
            IdentityVariant::Resource,
        );
        assert!(out.is_ok());
    }

    #[test]
    fn test_derive_with_unknown_field() {
        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[diesel(table_name = my_target)]
                #[resource(foo = bar)]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            },
            IdentityVariant::Resource,
        );
        let error = out.expect_err("input has unknown parameter for resource");
        assert!(error.to_string().contains("unknown field `foo`"));
    }

    #[test]
    fn test_derive_snapshots() {
        let out = derive_impl(
            quote! {
                #[derive(Asset)]
                #[diesel(table_name = my_target)]
                #[asset(uuid_kind = CustomKind)]
                struct AssetWithUuidKind {
                    identity: AssetWithUuidKindIdentity,
                    name: String,
                    is_cool: bool,
                }
            },
            IdentityVariant::Asset,
        )
        .unwrap();
        assert_contents(
            "outputs/asset_with_uuid_kind.txt",
            &pretty_format(out),
        );

        let out = derive_impl(
            quote! {
                #[derive(Resource)]
                #[diesel(table_name = my_target)]
                #[resource(uuid_kind = CustomKind)]
                struct ResourceWithUuidKind {
                    identity: ResourceWithUuidKindIdentity,
                    name: String,
                    is_cool: bool,
                }
            },
            IdentityVariant::Resource,
        )
        .unwrap();
        assert_contents(
            "outputs/resource_with_uuid_kind.txt",
            &pretty_format(out),
        );
    }
}
