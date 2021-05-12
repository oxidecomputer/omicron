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
use syn::{Data, DeriveInput, Error, Fields, Ident, Type};

/// Derive the `Target` trait for a type.
#[proc_macro_derive(Target)]
pub fn target(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    target_impl(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

/// Derive the `Metric` trait for a struct.
#[proc_macro_attribute]
pub fn metric(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    metric_impl(attr.into(), input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

// Implementation of `#[derive(Target)]`
fn target_impl(tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    if let Data::Struct(ref data) = item.data {
        let name = &item.ident;
        if let Fields::Named(ref data_fields) = data.fields {
            let (names, types, values, _) =
                extract_struct_fields(data_fields.named.iter())?;
            if names.is_empty() {
                return Err(Error::new(
                    item.span(),
                    "Structs must have at least one field",
                ));
            }
            return Ok(build_target_trait_impl(&name, &names, &types, &values));
        }
        return Err(Error::new(
            item.span(),
            "Can only be derived for structs with named fields",
        ));
    }
    Err(Error::new(
        item.span(),
        "Can only be derived for structs with named fields",
    ))
}

// Implementation of the `[oximeter::metric]` procedural macro attribute.
fn metric_impl(
    attr: TokenStream,
    item: TokenStream,
) -> syn::Result<TokenStream> {
    let measurement_type = syn::parse2::<Ident>(attr)?;
    let meas_type = type_name_for_measurement_type(&measurement_type)?;
    let item = syn::parse2::<syn::ItemStruct>(item)?;
    let name = &item.ident;
    if let Fields::Named(ref data_fields) = item.fields {
        let (names, types, values, _) =
            extract_struct_fields(data_fields.named.iter())?;
        if names.is_empty() {
            return Err(Error::new(
                item.span(),
                "Structs must have at least one field",
            ));
        }
        let metric_impl = build_metric_trait_impl(
            &name,
            &names,
            &types,
            &values,
            &measurement_type,
            meas_type,
        );
        return Ok(quote! {
            #item
            #metric_impl
        });
    }
    Err(Error::new(
        item.span(),
        "Can only be derived for structs with named fields",
    ))
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

    // "target-name:field:field:..."
    let fmt = format!("{{}}{}", ":{}".repeat(values.len()));
    let key_formatter = quote! { format!(#fmt, #name, #(self.#refs),*) };
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

fn type_name_for_measurement_type(
    measurement_type: &Ident,
) -> syn::Result<TokenStream> {
    match format!("{}", measurement_type).as_str() {
        "Bool" => Ok(quote! { bool }),
        "I64" => Ok(quote! { i64 }),
        "F64" => Ok(quote! { f64 }),
        "String" => Ok(quote! { String }),
        "Bytes" => Ok(quote! { bytes::Bytes }),
        "CumulativeI64" => Ok(quote! { ::oximeter::types::Cumulative<i64> }),
        "CumulativeF64" => Ok(quote! { ::oximeter::types::Cumulative<f64> }),
        "HistogramI64" => Ok(quote! { ::oximeter::histogram::Histogram<i64> }),
        "HistogramF64" => Ok(quote! { ::oximeter::histogram::Histogram<f64> }),
        _ => Err(Error::new(measurement_type.span(),
                "Invalid measurement type, must be a variant of oximeter::MeasurementType: \
                \"Bool\", \"I64\", \"F64\", \"String\", \"Bytes\", \
                \"CumulativeI64\" or \"CumulativeF64\", \
                \"HistogramI64\" or \"HistogramF64\".",
            ))
    }
}

// Build the derived implementation for the Metric trait
fn build_metric_trait_impl(
    item_name: &Ident,
    names: &[String],
    types: &[TokenStream],
    values: &[TokenStream],
    measurement_type: &Ident,
    meas_type: TokenStream,
) -> TokenStream {
    let refs = names.iter().map(|name| format_ident!("{}", name));
    let name = to_snake_case(&format!("{}", item_name));
    let fmt = format!("{}{{}}", "{}:".repeat(values.len()));
    let key_formatter = quote! { format!(#fmt, #(self.#refs),*, #name) };
    let measurement_type = syn::parse_str::<syn::Expr>(&format!(
        "::oximeter::MeasurementType::{}",
        measurement_type
    ))
    .unwrap();
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
        }
    }
}

#[allow(clippy::type_complexity)]
fn extract_struct_fields<
    'a,
    F: std::iter::ExactSizeIterator<Item = &'a syn::Field>,
>(
    fields: F,
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
                extract_variants(&field_type, &field_name)?;
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
) -> syn::Result<(TokenStream, TokenStream)> {
    let (fragment, maybe_clone) = match type_name {
        "String" => (type_name, ".clone()"),
        "IpAddr" | "Uuid" => (type_name, ""),
        "i64" => ("I64", ""),
        "bool" => ("Bool", ""),
        _ => {
            return Err(Error::new(
                Span::call_site(),
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
            out.push(ch.to_lowercase().next().unwrap());
        } else {
            out.push(ch.to_lowercase().next().unwrap());
        }
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
        assert!(out.is_err());
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
            "Bool",
            "I64",
            "F64",
            "String",
            "Bytes",
            "CumulativeI64",
            "CumulativeF64",
            "HistogramI64",
            "HistogramF64",
        ];
        for type_ in valid_types.iter() {
            let ident = syn::parse_str::<Type>(type_).unwrap();
            let out = metric_impl(
                quote! { #ident },
                quote! {
                    struct MyMetric {
                        field: String,
                    }
                },
            );
            assert!(out.is_ok());
        }
    }

    #[test]
    fn test_metric_enum() {
        let out = metric_impl(
            quote! { bool },
            quote! {
                enum MyMetric {
                    Variant
                }
            },
        );
        assert!(out.is_err());
    }

    #[test]
    fn test_metric_unsupported_type() {
        let out = metric_impl(
            quote! { f32 },
            quote! {
                struct MyMetric {
                    field: String,
                }
            },
        );
        assert!(out.is_err());
    }
}
