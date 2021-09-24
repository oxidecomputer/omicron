//! Implementation of procedural macros for deriving oximeter's traits.
//!
//! This crate provides the implementation of the `Target` derive macro and the `oximeter::metric`
//! attribute macro. These allow users of the main `oximeter` crate to easily derive the methods to
//! retrieve the names, types, and values of their struct fields, and to associate a supported
//! datum type with their metric struct.

// Copyright 2021 Oxide Computer Company

extern crate proc_macro;

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    Data, DeriveInput, Error, Fields, FieldsNamed, Ident, ItemStruct, Type,
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
        let (names, types, values) = if let Fields::Named(ref data_fields) =
            data.fields
        {
            let (names, types, values, _) =
                extract_struct_fields(&data_fields, None)?;
            (names, types, values)
        } else if matches!(data.fields, Fields::Unit) {
            (vec![], vec![], vec![])
        } else {
            return Err(Error::new(
                    item.span(),
                    "Can only be derived for structs with named fields or unit structs",
                ));
        };
        return Ok(build_target_trait_impl(&name, &names, &types, &values));
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
        let (field_names, field_types, field_values, _) =
            extract_struct_fields(
                &data_fields,
                Some(&datum_field.ident.as_ref().unwrap().to_string()),
            )?;
        let metric_impl = build_metric_trait_impl(
            name,
            &field_names,
            &field_types,
            &field_values,
            &datum_field,
        );
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
                attr.path
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

// Build the derived implementation for the Target trait
fn build_target_trait_impl(
    item_name: &Ident,
    names: &[String],
    types: &[TokenStream],
    values: &[TokenStream],
) -> TokenStream {
    let refs = names.iter().map(|name| format_ident!("{}", name));
    let name = to_snake_case(&format!("{}", item_name));

    // key format: "field0_value:field1_value:..."
    let fmt = vec!["{}"; values.len()].join(":");
    let key_formatter = quote! { format!(#fmt, #(self.#refs),*) };
    quote! {
        impl ::oximeter::Target for #item_name {
            fn name(&self) -> &'static str {
                #name
            }

            fn field_names(&self) -> &'static [&'static str] {
                &[#(#names),*]
            }

            fn field_types(&self) -> &'static [::oximeter::FieldType] {
                &[#(#types,)*]
            }

            fn field_values(&self) -> Vec<::oximeter::FieldValue> {
                vec![#(#values,)*]
            }

            fn key(&self) -> String {
                #key_formatter
            }
        }
    }
}

// Build the derived implementation for the Metric trait
fn build_metric_trait_impl(
    item_name: &Ident,
    names: &[String],
    types: &[TokenStream],
    values: &[TokenStream],
    datum_field: &syn::Field,
) -> TokenStream {
    let refs =
        names.iter().map(|name| format_ident!("{}", name)).collect::<Vec<_>>();
    let name = to_snake_case(&format!("{}", item_name));

    // key format: "field0_value:field1_value:..."
    let fmt = vec!["{}"; values.len()].join(":");
    let key_formatter = quote! { format!(#fmt, #(self.#refs),*) };
    let datum_field_ident = datum_field.ident.as_ref().unwrap();
    let dat_type = &datum_field.ty;
    quote! {
        impl ::oximeter::Metric for #item_name {
            type Datum = #dat_type;

            fn name(&self) -> &'static str {
                #name
            }

            fn field_names(&self) -> &'static [&'static str] {
                &[#(#names),*]
            }

            fn field_types(&self) -> &'static [::oximeter::FieldType] {
                &[#(#types,)*]
            }

            fn field_values(&self) -> Vec<::oximeter::FieldValue> {
                vec![#(#values,)*]
            }

            fn key(&self) -> String {
                #key_formatter
            }

            fn datum_type(&self) -> ::oximeter::DatumType {
                <Self::Datum as ::oximeter::traits::Datum>::datum_type(&self.#datum_field_ident)
            }

            fn datum(&self) -> &#dat_type {
                &self.#datum_field_ident
            }

            fn datum_mut(&mut self) -> &mut #dat_type {
                &mut self.#datum_field_ident
            }

            fn measure(&self) -> ::oximeter::Measurement {
                ::oximeter::Measurement::new(::oximeter::Datum::from(self.#datum_field_ident.clone()))
            }

            fn start_time(&self) -> Option<::chrono::DateTime<::chrono::Utc>> {
                <Self::Datum as ::oximeter::traits::Datum>::start_time(&self.#datum_field_ident)
            }
        }
    }
}

#[allow(clippy::type_complexity)]
fn extract_struct_fields(
    fields: &FieldsNamed,
    ignore: Option<&str>,
) -> syn::Result<(Vec<String>, Vec<TokenStream>, Vec<TokenStream>, Vec<Type>)> {
    let n_fields = fields.named.len();
    let mut names = Vec::with_capacity(n_fields);
    let mut types = Vec::with_capacity(n_fields);
    let mut values = Vec::with_capacity(n_fields);
    let mut arg_types = Vec::with_capacity(n_fields);
    for field in fields.named.iter() {
        if let Type::Path(ref ty) = field.ty {
            let field_name = format!(
                "{}",
                field.ident.as_ref().expect("Field must have a name")
            );
            if ignore.is_some() && ignore == Some(field_name.as_str()) {
                continue;
            }
            let field_type = format!(
                "{}",
                ty.path
                    .segments
                    .iter()
                    .last()
                    .ok_or_else(|| Error::new(
                        ty.path.span(),
                        "Expected a field with a path type"
                    ))?
                    .ident
            );

            let (type_variant, value_variant) =
                extract_variants(&field_type, &field_name, &field.span())?;
            names.push(field_name);
            types.push(type_variant);
            values.push(value_variant);
            arg_types.push(field.ty.clone());
        } else {
            return Err(Error::new(
                field.span(),
                "Expected a field with a path type",
            ));
        }
    }
    Ok((names, types, values, arg_types))
}

// Extract the field type and field value variants for the given field of a target struct
fn extract_variants(
    type_name: &str,
    field_name: &str,
    span: &Span,
) -> syn::Result<(TokenStream, TokenStream)> {
    let (fragment, maybe_clone) = match type_name {
        "String" => (type_name, ".clone()"),
        "IpAddr" | "Uuid" => (type_name, ""),
        "i64" => ("I64", ""),
        "bool" => ("Bool", ""),
        _ => {
            return Err(Error::new(
                *span,
                "Fields must be one of type: String, IpAddr, i64, bool, Uuid",
            ));
        }
    };
    let type_variant = syn::parse_str::<syn::Expr>(&format!(
        "::oximeter::FieldType::{}",
        fragment
    ))
    .unwrap();
    let value_variant = syn::parse_str::<syn::Expr>(&format!(
        "::oximeter::FieldValue::{}(self.{}{})",
        fragment, field_name, maybe_clone
    ))
    .unwrap();
    Ok((quote! { #type_variant }, quote! { #value_variant }))
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

    #[test]
    fn test_target() {
        let out = target_impl(
            quote! {
                #[derive(Target)]
                struct MyTarget {
                    name: String,
                    is_cool: bool,
                    addr: std::net::IpAddr,
                }
            }
            .into(),
        );
        assert!(out.is_ok());
    }

    #[test]
    fn test_target_unsupported_type() {
        let out = target_impl(
            quote! {
                #[derive(Target)]
                struct MyTarget {
                    no_no: f64,
                }
            }
            .into(),
        );
        assert!(out.is_err());
    }

    #[test]
    fn test_target_unit_struct() {
        let out = target_impl(
            quote! {
                #[derive(Target)]
                struct MyTarget;
            }
            .into(),
        );
        assert!(out.is_ok());
    }

    #[test]
    fn test_target_empty_struct() {
        let out = target_impl(
            quote! {
                #[derive(Target)]
                struct MyTarget {}
            }
            .into(),
        );
        assert!(out.is_ok());
    }

    #[test]
    fn test_target_enum() {
        let out = target_impl(
            quote! {
                #[derive(Target)]
                enum MyTarget {
                    Bad,
                    NoGood,
                };
            }
            .into(),
        );
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
