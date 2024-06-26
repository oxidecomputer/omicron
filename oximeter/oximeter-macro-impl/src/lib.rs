// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of procedural macros for deriving oximeter's traits.
//!
//! This crate provides the implementation of the `Target` derive macro and the `oximeter::metric`
//! attribute macro. These allow users of the main `oximeter` crate to easily derive the methods to
//! retrieve the names, types, and values of their struct fields, and to associate a supported
//! datum type with their metric struct.

// Copyright 2021 Oxide Computer Company

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{
    Data, DeriveInput, Error, Field, Fields, FieldsNamed, Ident, ItemStruct,
};

/// Derive the `Target` trait for a type.
///
/// The `Target` trait can be attached to structs, where those structs describe the named fields
/// (and their types) for a target.
///
/// See the [`oximeter::Target`](../oximeter/traits/trait.Target.html) trait for details.
#[proc_macro_derive(Target)]
pub fn target(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    target_impl(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

/// Create a `Metric` trait implementation for a struct that defines the metric's schema
///
/// The `Metric` macro can be attached to structs, where those structs describe the named fields
/// (and their types) for a metric. The struct must also have a field named `datum`, or one
/// annotated with the `#[datum]` helper attribute (but named whatever you wish). This field
/// describes the datum of the metric, the type of underlying data that the metric tracks.
///
/// See the [`oximeter::Metric`](../oximeter/traits/trait.Metric.html) trait for details.
#[proc_macro_derive(Metric, attributes(datum))]
pub fn metric(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    metric_impl(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

// Implementation of `#[derive(Target)]`
fn target_impl(tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    if let Data::Struct(ref data) = item.data {
        let name = &item.ident;
        let fields = if let Fields::Named(ref data_fields) = data.fields {
            extract_struct_fields(&data_fields, None)
        } else if matches!(data.fields, Fields::Unit) {
            vec![]
        } else {
            return Err(Error::new(
                    item.span(),
                    "Can only be derived for structs with named fields or unit structs",
                ));
        };
        return Ok(build_target_trait_impl(&name, &fields[..]));
    }
    Err(Error::new(
        item.span(),
        "Can only be derived for structs with named fields or unit structs",
    ))
}

// Implementation of the `#[derive(Metric)]` derive macro.
fn metric_impl(item: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<ItemStruct>(item)?;
    let datum_field = extract_datum_type(&item)?;
    let name = &item.ident;
    if let Fields::Named(ref data_fields) = item.fields {
        let ignore = datum_field.ident.as_ref().unwrap().to_string();
        let fields = extract_struct_fields(&data_fields, Some(&ignore));
        let metric_impl =
            build_metric_trait_impl(name, &fields[..], &datum_field);
        Ok(quote! {
            #metric_impl
        })
    } else {
        Err(Error::new(
            item.span(),
            "Attribute may only be applied to structs with named fields",
        ))
    }
}

// Find a field named `datum` or annotated with the `#[datum]` attribute helper. Return its name,
// the type of the field, and the tokens representing the `oximeter::DatumType` enum variant
// corresponding to the field's type.
fn extract_datum_type(item: &ItemStruct) -> syn::Result<&syn::Field> {
    if let Fields::Named(ref fields) = item.fields {
        find_datum_field(fields)
            .ok_or_else(|| Error::new(item.span(), "Metric structs must have exactly one field named `datum` or a field annotated with the `#[datum]` attribute helper"))
    } else {
        Err(Error::new(item.span(), "Struct must contain named fields"))
    }
}

fn find_datum_field(fields: &FieldsNamed) -> Option<&syn::Field> {
    // Find fields annotated with the `#[datum]` helper.
    let annotated_fields = fields
        .named
        .iter()
        .filter(|field| {
            field.attrs.iter().any(|attr| {
                attr.path()
                    .get_ident()
                    .map(|ident| *ident == "datum")
                    .unwrap_or(false)
            })
        })
        .collect::<Vec<_>>();

    // Find fields named `datum`
    let named_fields = fields
        .named
        .iter()
        .filter(|field| *field.ident.as_ref().unwrap() == "datum")
        .collect::<Vec<_>>();

    match (annotated_fields.len(), named_fields.len()) {
        (1, 0) => annotated_fields.first(),
        (0, 1) => named_fields.first(),
        (1, 1) => {
            if named_fields.first() == annotated_fields.first() {
                named_fields.first()
            } else {
                None
            }
        }
        _ => None,
    }
    .map(|field| &**field)
}

fn build_shared_methods(item_name: &Ident, fields: &[&Field]) -> TokenStream {
    let field_idents = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect::<Vec<_>>();
    let names = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap().to_string())
        .collect::<Vec<_>>();
    let name = to_snake_case(&format!("{}", item_name));

    quote! {
        fn name(&self) -> &'static str {
            #name
        }

        fn version(&self) -> ::std::num::NonZeroU8 {
            unsafe { ::std::num::NonZeroU8::new_unchecked(1) }
        }

        fn field_names(&self) -> &'static [&'static str] {
            &[#(#names),*]
        }

        fn field_types(&self) -> Vec<::oximeter::FieldType> {
            vec![#(::oximeter::FieldType::from(&self.#field_idents),)*]
        }

        fn field_values(&self) -> Vec<::oximeter::FieldValue> {
            vec![#(::oximeter::FieldValue::from(&self.#field_idents),)*]
        }
    }
}

// Build the derived implementation for the Target trait
fn build_target_trait_impl(
    item_name: &Ident,
    fields: &[&Field],
) -> TokenStream {
    let shared_methods = build_shared_methods(item_name, fields);
    quote! {
        impl ::oximeter::Target for #item_name {
            #shared_methods
        }
    }
}

// Build the derived implementation for the Metric trait
fn build_metric_trait_impl(
    item_name: &Ident,
    fields: &[&Field],
    datum_field: &syn::Field,
) -> TokenStream {
    let shared_methods = build_shared_methods(item_name, fields);
    let datum_field_ident = datum_field.ident.as_ref().unwrap();
    let dat_type = &datum_field.ty;
    quote! {
        impl ::oximeter::Metric for #item_name {
            type Datum = #dat_type;

            #shared_methods

            fn datum_type(&self) -> ::oximeter::DatumType {
                <Self::Datum as ::oximeter::traits::Datum>::datum_type(&self.#datum_field_ident)
            }

            fn datum(&self) -> &#dat_type {
                &self.#datum_field_ident
            }

            fn datum_mut(&mut self) -> &mut #dat_type {
                &mut self.#datum_field_ident
            }

            fn measure(&self, timestamp: ::chrono::DateTime<chrono::Utc>) -> ::oximeter::Measurement {
                ::oximeter::Measurement::new(
                    timestamp,
                    ::oximeter::Datum::from(&self.#datum_field_ident)
                )
            }

            fn start_time(&self) -> Option<::chrono::DateTime<::chrono::Utc>> {
                <Self::Datum as ::oximeter::traits::Datum>::start_time(&self.#datum_field_ident)
            }
        }
    }
}

fn extract_struct_fields<'a>(
    fields: &'a FieldsNamed,
    ignore: Option<&'a str>,
) -> Vec<&'a Field> {
    if let Some(ignore) = ignore {
        fields
            .named
            .iter()
            .filter(|field| *field.ident.as_ref().unwrap() != ignore)
            .collect()
    } else {
        fields.named.iter().collect()
    }
}

// Convert the CapitalCase struct name in to snake_case.
fn to_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_uppercase() {
            if !out.is_empty() {
                out.push('_');
            }
        }
        out.push(ch.to_lowercase().next().unwrap());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::Type;

    #[test]
    fn test_target() {
        let out = target_impl(quote! {
            #[derive(Target)]
            struct MyTarget {
                name: String,
                is_cool: bool,
                addr: std::net::IpAddr,
            }
        });
        assert!(out.is_ok());
    }

    #[test]
    fn test_target_unit_struct() {
        let out = target_impl(quote! {
            #[derive(Target)]
            struct MyTarget;
        });
        assert!(out.is_ok());
    }

    #[test]
    fn test_target_empty_struct() {
        let out = target_impl(quote! {
            #[derive(Target)]
            struct MyTarget {}
        });
        assert!(out.is_ok());
    }

    #[test]
    fn test_target_enum() {
        let out = target_impl(quote! {
            #[derive(Target)]
            enum MyTarget {
                Bad,
                NoGood,
            };
        });
        assert!(out.is_err());
    }

    #[test]
    fn test_metric_datum_field() {
        let valid_types = &[
            "bool",
            "i64",
            "f64",
            "String",
            "Bytes",
            "Cumulative<i64>",
            "Cumulative<f64>",
            "Histogram<i64>",
            "Histogram<f64>",
        ];
        for type_ in valid_types.iter() {
            let ident = syn::parse_str::<Type>(type_).unwrap();
            metric_impl(quote! {
                struct MyMetric {
                    field: String,
                    datum: #ident,
                }
            })
            .unwrap();
        }
    }

    #[test]
    fn test_metric_annotated_field() {
        let valid_types = &[
            "bool",
            "i64",
            "f64",
            "String",
            "Bytes",
            "Cumulative<i64>",
            "Cumulative<f64>",
            "Histogram<i64>",
            "Histogram<f64>",
        ];
        for type_ in valid_types.iter() {
            let ident = syn::parse_str::<Type>(type_).unwrap();
            metric_impl(quote! {
                struct MyMetric {
                    field: String,
                    #[datum]
                    something: #ident,
                }
            })
            .unwrap();
        }
    }

    #[test]
    fn test_metric_enum() {
        let out = metric_impl(quote! {
            enum MyMetric {
                Variant
            }
        });
        assert!(out.is_err());
    }

    #[test]
    fn test_metric_without_datum_field() {
        let out = metric_impl(quote! {
            struct MyMetric {
                field: String,
            }
        });
        assert!(out.is_err());
    }

    #[test]
    fn test_target_with_datum_field() {
        let out = target_impl(quote! {
            struct MyTarget {
                datum: String,
            }
        });
        assert!(out.is_ok());
    }

    #[test]
    fn test_extract_datum_type_by_field_name() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            struct MyMetric {
                not_datum: String,
                datum: i64,
            }
        })
        .unwrap();
        let field = extract_datum_type(&item).unwrap();
        assert_eq!(field.ident.as_ref().unwrap(), "datum");
        if let syn::Type::Path(ref p) = field.ty {
            assert_eq!(
                p.path.segments.last().unwrap().ident.to_string(),
                "i64"
            );
        } else {
            panic!("Expected the extracted datum type");
        }
    }

    #[test]
    fn test_extract_datum_type_by_annotatd_field() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            struct MyMetric {
                not_datum: String,
                #[datum]
                also_not_datum: i64,
            }
        })
        .unwrap();
        let field = extract_datum_type(&item).unwrap();
        assert_eq!(field.ident.as_ref().unwrap(), "also_not_datum");
        if let syn::Type::Path(ref p) = field.ty {
            assert_eq!(
                p.path.segments.last().unwrap().ident.to_string(),
                "i64"
            );
        } else {
            panic!("Expected the extracted datum type");
        }
    }

    #[test]
    fn test_extract_datum_type_named_and_annotated() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            struct MyMetric {
                not_datum: String,
                #[datum]
                also_not_datum: i64,
                datum: i64,
            }
        })
        .unwrap();
        assert!(extract_datum_type(&item).is_err());
    }

    #[test]
    fn test_extract_datum_type_multiple_annotated_fields() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            struct MyMetric {
                not_datum: String,
                #[datum]
                also_not_datum: i64,
                #[datum]
                also_also: i64,
            }
        })
        .unwrap();
        assert!(extract_datum_type(&item).is_err());
    }

    #[test]
    fn test_extract_datum_type_named_and_annotated_same_field() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            struct MyMetric {
                not_datum: String,
                #[datum]
                datum: i64,
            }
        })
        .unwrap();
        let field = extract_datum_type(&item).unwrap();
        assert_eq!(field.ident.as_ref().unwrap(), "datum");
        if let syn::Type::Path(ref p) = field.ty {
            assert_eq!(
                p.path.segments.last().unwrap().ident.to_string(),
                "i64"
            );
        } else {
            panic!("Expected the extracted datum type");
        }
    }
}
