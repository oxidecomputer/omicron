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

/// Generates a "StructNameIdentity" structure for the associated struct, along
/// with helper accessor functions.
///
/// Many tables within our database make use of common fields,
/// including:
/// - ID
/// - Name
/// - Description
/// - Time Created, modified, and deleted.
///
/// Although these fields can be refactored into a common structure, to be used
/// within the context of Diesel, they must be uniquely identified for a single
/// table.
#[proc_macro_derive(IdentityMetadata)]
pub fn target(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    identity_impl(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

// Looks for a Meta-style attribute with a particular identifier.
//
// As an example, for an attribute like `#[foo = "bar"]`, we can find this
// attribute by calling `get_meta_attr(&item.attrs, "foo")`.
fn get_meta_attr(attrs: &[syn::Attribute], name: &str) -> Option<Meta> {
    attrs
        .iter()
        .filter_map(|attr| attr.parse_meta().ok())
        .find(|meta| meta.path().is_ident(name))
}

// Accesses the "value" part of a name-value Meta attribute.
fn get_attribute_value(meta: &Meta) -> Option<&Lit> {
    if let Meta::NameValue(ref nv) = meta {
        Some(&nv.lit)
    } else {
        None
    }
}

// Looks up a named field within a struct.
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

// Implementation of `#[derive(IdentityMetadata)]`
fn identity_impl(tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    let name = &item.ident;

    // Ensure that the "table_name" attribute exists, and get it.
    let table_meta =
        get_meta_attr(&item.attrs, "table_name").ok_or_else(|| {
            Error::new(
                item.span(),
                format!(
                    "IdentityMetadata needs 'table_name' attribute.\n\
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
        // TODO: Admittedly, we aren't checking the type of the field named
        // 'identity' at all.
        get_field_with_name(data, "identity")
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

        return Ok(build_struct(name, table_name));
    }

    Err(Error::new(
        item.span(),
        "IdentityMetadata can only be derived for structs",
    ))
}

fn build_struct(struct_name: &Ident, table_name: &Lit) -> TokenStream {
    let identity_doc = format!(
        "Auto-generated identity for [`{}`] from deriving [macro@IdentityMetadata].",
        struct_name,
    );
    let identity_name = format_ident!("{}Identity", struct_name);
    quote! {
        #[doc = #identity_doc]
        #[derive(Clone, Debug, Selectable, Queryable, Insertable)]
        #[table_name = #table_name ]
        pub struct #identity_name {
            pub id: uuid::Uuid,
            pub name: omicron_common::api::external::Name,
            pub description: String,
            pub time_created: chrono::DateTime<chrono::Utc>,
            pub time_modified: chrono::DateTime<chrono::Utc>,
            pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
        }

        impl #identity_name {
            pub fn new(
                id: uuid::Uuid,
                params: omicron_common::api::external::IdentityMetadataCreateParams
            ) -> Self {
                let now = chrono::Utc::now();
                Self {
                    id,
                    name: params.name,
                    description: params.description,
                    time_created: now,
                    time_modified: now,
                    time_deleted: None,
                }
            }
        }

        impl Into<omicron_common::api::external::IdentityMetadata> for #identity_name {
            fn into(self) -> omicron_common::api::external::IdentityMetadata {
                omicron_common::api::external::IdentityMetadata {
                    id: self.id,
                    name: self.name,
                    description: self.description,
                    time_created: self.time_created,
                    time_modified: self.time_modified,
                }
            }
        }

        impl From<omicron_common::api::external::IdentityMetadata> for #identity_name {
            fn from(metadata: omicron_common::api::external::IdentityMetadata) -> Self {
                Self {
                    id: metadata.id,
                    name: metadata.name,
                    description: metadata.description,
                    time_created: metadata.time_created,
                    time_modified: metadata.time_modified,
                    time_deleted: None,
                }
            }
        }

        impl #struct_name {
            pub fn id(&self) -> uuid::Uuid {
                self.identity.id
            }

            pub fn name(&self) -> &omicron_common::api::external::Name {
                &self.identity.name
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_metadata_identity_fails_without_table_name() {
        let out = identity_impl(
            quote! {
                #[derive(IdentityMetadata)]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
        );
        assert!(out.is_err());
        assert_eq!(
            "IdentityMetadata needs 'table_name' attribute.\n\
             Try adding #[table_name = \"your_table_name\"] to MyTarget.",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_with_wrong_table_name_type() {
        let out = identity_impl(
            quote! {
                #[derive(IdentityMetadata)]
                #[table_name]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
        );
        assert!(out.is_err());
        assert_eq!(
            "'table_name' needs to be a name-value pair, like #[table_name = foo]",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_for_enums() {
        let out = identity_impl(
            quote! {
                #[derive(IdentityMetadata)]
                #[table_name = "foo"]
                enum MyTarget {
                    Foo,
                    Bar,
                }
            }
            .into(),
        );
        assert!(out.is_err());
        assert_eq!(
            "IdentityMetadata can only be derived for structs",
            out.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_derive_metadata_identity_fails_without_embedded_identity() {
        let out = identity_impl(
            quote! {
                #[derive(IdentityMetadata)]
                #[table_name = "my_target"]
                struct MyTarget {
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
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
        let out = identity_impl(
            quote! {
                #[derive(IdentityMetadata)]
                #[table_name = "my_target"]
                struct MyTarget {
                    identity: MyTargetIdentity,
                    name: String,
                    is_cool: bool,
                }
            }
            .into(),
        );
        assert!(out.is_ok());
    }
}
