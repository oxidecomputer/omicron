//! Implementation of procedural macros for deriving oximeter's traits.
//!
//! This crate provides the implementation of the `Target` derive macro and the `oximeter::metric`
//! attribute macro. These allow users of the main `oximeter` crate to easily derive the methods to
//! retrieve the names, types, and values of their struct fields, and to associate a supported
//! measurement type with their metric struct.

// Copyright 2021 Oxide Computer Company

extern crate proc_macro;

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Error, Fields, FieldsNamed, Ident, Type};

/// Derive the `Target` trait for a type.
#[proc_macro_derive(Target)]
pub fn target(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    target_impl(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

/// Derive the `Metric` trait for a struct.
#[proc_macro_derive(Metric)]
pub fn metric(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    metric_impl(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

// Implementation of `#[derive(Target)]`
fn target_impl(tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    if let Data::Struct(ref data) = item.data {
        let name = &item.ident;
        let (names, types, values) =
            if let Fields::Named(ref data_fields) = data.fields {
                let (names, types, values, _) =
                    extract_struct_fields(data_fields.named.iter(), false)?;
                (names, types, values)
            } else if matches!(data.fields, Fields::Unit) {
                (vec![], vec![], vec![])
            } else {
                return Err(Error::new(
                    item.span(),
                    "Can only be derived for structs with named fields",
                ));
            };
        return Ok(build_target_trait_impl(&name, &names, &types, &values));
    }
    Err(Error::new(
        item.span(),
        "Can only be derived for structs with named fields",
    ))
}

// Implementation of the `[oximeter::metric]` procedural macro attribute.
fn metric_impl(item: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<syn::ItemStruct>(item)?;
    let name = &item.ident;
    if let Fields::Named(ref data_fields) = item.fields {
        let (meas_type, measurement_type) =
            extract_measurement_type(data_fields)?;
        let (names, types, values, _) =
            extract_struct_fields(data_fields.named.iter(), true)?;
        let metric_impl = build_metric_trait_impl(
            &name,
            &names,
            &types,
            &values,
            &measurement_type,
            &meas_type,
        );
        return Ok(quote! { #metric_impl });
    }
    Err(Error::new(
        item.span(),
        "Can only be derived for structs with at least one field, named `value`",
    ))
}

fn extract_measurement_type(
    fields: &FieldsNamed,
) -> syn::Result<(&syn::Type, TokenStream)> {
    let field = fields
        .named
        .iter()
        .find(|field| field.ident.as_ref().unwrap() == "value")
        .ok_or_else(|| {
            Error::new(fields.span(), "Must contain a field named \"value\"")
        })?;
    let err = |span| {
        Error::new(
            span,
            "Must be one of the supported data types: \
            bool, i64, f64, String, Bytes, Cumulative<i64>, Cumulative<f64>, \
            Histogram<i64>, or Histogram<f64>",
        )
    };
    if let syn::Type::Path(ref p) = field.ty {
        let path = p.path.segments.last().ok_or_else(|| err(field.span()))?;
        let as_segment =
            |s: &str| syn::parse_str::<syn::PathSegment>(s).unwrap();

        let meas_type = if path == &as_segment("bool") {
            quote! { ::oximeter::MeasurementType::Bool }
        } else if path == &as_segment("i64") {
            quote! { ::oximeter::MeasurementType::I64 }
        } else if path == &as_segment("f64") {
            quote! { ::oximeter::MeasurementType::F64 }
        } else if path == &as_segment("String") {
            quote! { ::oximeter::MeasurementType::String }
        } else if path == &as_segment("Bytes") {
            quote! { ::oximeter::MeasurementType::Bytes }
        } else if path == &as_segment("Cumulative<i64>") {
            quote! { ::oximeter::MeasurementType::CumulativeI64 }
        } else if path == &as_segment("Cumulative<f64>") {
            quote! { ::oximeter::MeasurementType::CumulativeF64 }
        } else if path == &as_segment("Histogram<i64>") {
            quote! { ::oximeter::MeasurementType::HistogramI64 }
        } else if path == &as_segment("Histogram<f64>") {
            quote! { ::oximeter::MeasurementType::HistogramF64 }
        } else {
            return Err(err(field.span()));
        };
        Ok((&field.ty, meas_type))
    } else {
        Err(err(fields.span()))
    }
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
    measurement_type: &TokenStream,
    meas_type: &syn::Type,
) -> TokenStream {
    let refs =
        names.iter().map(|name| format_ident!("{}", name)).collect::<Vec<_>>();
    let name = to_snake_case(&format!("{}", item_name));

    // key format: "field0_value:field1_value:..."
    let fmt = vec!["{}"; values.len()].join(":");
    let key_formatter = quote! { format!(#fmt, #(self.#refs),*) };
    let measurement_type =
        syn::parse_str::<syn::Expr>(&format!("{}", measurement_type)).unwrap();
    quote! {
        impl ::oximeter::Metric for #item_name {
            type Measurement = #meas_type;

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

            fn measurement_type(&self) -> ::oximeter::MeasurementType {
                #measurement_type
            }

            fn value(&self) -> &#meas_type {
                &self.value
            }

            fn measure(&self) -> ::oximeter::Measurement {
                ::oximeter::Measurement::from(&self.value)
            }
        }
    }
}

#[allow(clippy::type_complexity)]
fn extract_struct_fields<
    'a,
    F: std::iter::ExactSizeIterator<Item = &'a syn::Field>,
>(
    fields: F,
    is_metric: bool,
) -> syn::Result<(
    Vec<String>,
    Vec<TokenStream>,
    Vec<TokenStream>,
    Vec<syn::Type>,
)> {
    let n_fields = fields.len();
    let mut names = Vec::with_capacity(n_fields);
    let mut types = Vec::with_capacity(n_fields);
    let mut values = Vec::with_capacity(n_fields);
    let mut arg_types = Vec::with_capacity(n_fields);
    for field in fields {
        if let Type::Path(ref ty) = field.ty {
            let field_name = format!(
                "{}",
                field.ident.as_ref().expect("Field must have a name")
            );
            if field_name == "value" && is_metric {
                // Skip the value field, which has already been verified to exist and have a valid
                // type.
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
    fn test_metric() {
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
            let out = metric_impl(quote! {
                struct MyMetric {
                    field: String,
                    value: #ident,
                }
            });
            assert!(out.is_ok());
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
    fn test_metric_unsupported_type() {
        let out = metric_impl(quote! {
            struct MyMetric {
                field: String,
                value: f32,
            }
        });
        assert!(out.is_err());
    }

    #[test]
    fn test_metric_without_value_field() {
        let out = metric_impl(quote! {
            struct MyMetric {
                field: String,
            }
        });
        assert!(out.is_err());
    }

    #[test]
    fn test_target_with_value_field() {
        let out = target_impl(quote! {
            struct MyTarget {
                value: String,
            }
        });
        assert!(out.is_ok());
    }
}
