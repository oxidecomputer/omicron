// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of procedural macros for deriving oximeter's traits.
//!
//! This crate provides the implementation of the `Target` derive macro and the `oximeter::metric`
//! attribute macro. These allow users of the main `oximeter` crate to easily derive the methods to
//! retrieve the names, types, and values of their struct fields, and to associate a supported
//! datum type with their metric struct.

// Copyright 2023 Oxide Computer Company

extern crate proc_macro;

use heck::AsSnakeCase;
use proc_macro2::TokenStream;
use quote::quote;
use std::num::NonZeroU8;
use syn::spanned::Spanned;
use syn::Error;
use syn::Expr;
use syn::ExprLit;
use syn::Field;
use syn::Fields;
use syn::FieldsNamed;
use syn::Ident;
use syn::ItemStruct;
use syn::Lit;
use syn::LitInt;
use syn::LitStr;
use syn::Meta;

/// Derive the `Target` trait for a type.
///
/// The `Target` trait can be attached to structs, where those structs describe the named fields
/// (and their types) for a target.
///
/// See the [`oximeter::Target`](../oximeter/traits/trait.Target.html) trait for details.
#[proc_macro_derive(Target, attributes(oximeter))]
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
#[proc_macro_derive(Metric, attributes(oximeter, datum))]
pub fn metric(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    metric_impl(input.into()).unwrap_or_else(|e| e.to_compile_error()).into()
}

#[derive(Clone, Debug)]
struct OximeterArgs<'a> {
    name: String,
    description: String,
    version: NonZeroU8,
    datum_field: Option<&'a Field>,
}

// Extract the oximeter attribute helper arguments.
//
// This supports the following:
//
// - name = "foo": A renaming of the target or metric object.
// - version = x: An explicit version. This defaults to 1.
// - description: The `doc` attribute is exctracted if one exists, or may be
// overridden with an explicit attribute.
fn oximeter_attribute_args(
    item: &ItemStruct,
    is_metric: bool,
) -> syn::Result<OximeterArgs> {
    let item_name = format!("{}", item.ident);
    let mut args = OximeterArgs {
        name: AsSnakeCase(item_name).to_string(),
        description: String::new(),
        version: NonZeroU8::new(1).unwrap(),
        datum_field: None,
    };
    // Parse the outer attributes on the struct itself.
    for attr in item.attrs.iter() {
        match &attr.meta {
            Meta::NameValue(nv) => {
                if attr.path().is_ident("doc") {
                    let Expr::Lit(ExprLit { lit: Lit::Str(desc), .. }) = &nv.value else {
                        return Err(Error::new(nv.span(), "expected a literal string"));
                    };
                    args.description = desc.value().clone();
                }
            }
            Meta::List(list) => {
                if attr.path().is_ident("oximeter") {
                    if list.tokens.is_empty() {
                        return Err(Error::new(
                            attr.span(),
                            "`oximeter` attribute may not be empty",
                        ));
                    }
                    attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("name") {
                            let _: syn::token::Eq = meta.input.parse()?;
                            let lit: LitStr = meta.input.parse()?;
                            let value = lit.value();
                            // If the user is overriding the name, we require
                            // that it be snake_case to avoid confusion and
                            // ambiguity.
                            let new_name = AsSnakeCase(&value).to_string();
                            if value != new_name {
                                return Err(Error::new(
                                    lit.span(),
                                    "Explicitly overridden names must be in snake_case",
                                ));
                            }
                            args.name = new_name;
                            return Ok(());
                        }
                        if meta.path.is_ident("description") {
                            let _: syn::token::Eq = meta.input.parse()?;
                            let lit: LitStr = meta.input.parse()?;
                            args.description = lit.value();
                            return Ok(());
                        }
                        if meta.path.is_ident("version") {
                            let _: syn::token::Eq = meta.input.parse()?;
                            let lit: LitInt = meta.input.parse()?;
                            args.version = NonZeroU8::new(
                                lit.base10_parse()?
                            ).ok_or_else(|| Error::new(lit.span(), "version must be > 0"))?;
                            return Ok(());
                        }
                        Err(meta.error(format!(
                            "unrecognized oximeter attribute: '{}'",
                            meta.path
                                .get_ident()
                                .map(ToString::to_string)
                                .unwrap_or_default()
                        )))
                    })?;
                }
            }
            Meta::Path(p) if p.is_ident("oximeter") => {
                return Err(Error::new(
                    p.span(),
                    "`oximeter` attribute requires arguments",
                ));
            }
            _ => {}
        }
    }

    // Parse the outer attributes on the struct fields, if any.
    //
    // We're looking for a field named `datum` or a field marked with `datum`
    if is_metric {
        let Some(f) = find_datum_field(&item)? else {
            return Err(Error::new(
                    item.span(),
                    "datum field must be specified for metrics",
                ));
        };
        args.datum_field.replace(f);
    }
    Ok(args)
}

// Implementation of `#[derive(Target)]`
fn target_impl(tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<ItemStruct>(tokens)?;
    let args = oximeter_attribute_args(&item, false)?;
    let item_name = &item.ident;
    let fields = if let Fields::Named(ref data_fields) = item.fields {
        extract_struct_fields(&data_fields, None::<String>)
    } else if matches!(item.fields, Fields::Unit) {
        vec![]
    } else {
        return Err(Error::new(
            item.span(),
            "Can only be derived for structs with named fields or unit structs",
        ));
    };
    return Ok(build_target_trait_impl(&item_name, &fields[..], &args));
}

// Implementation of the `#[derive(Metric)]` derive macro.
fn metric_impl(item: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<ItemStruct>(item)?;
    let args = oximeter_attribute_args(&item, true)?;
    let item_name = &item.ident;
    let fields = match &item.fields {
        Fields::Named(ref data_fields) => {
            let ignore =
                args.datum_field.unwrap().ident.as_ref().unwrap().to_string();
            extract_struct_fields(&data_fields, Some(&ignore))
        }
        Fields::Unnamed(_) => {
            vec![]
        }
        _ => {
            return Err(Error::new(
                item.span(),
                "Attribute may only be applied to structs with named fields",
            ));
        }
    };
    let metric_impl = build_metric_trait_impl(
        item_name,
        &fields,
        &args.datum_field.unwrap(),
        &args,
    );
    Ok(quote! {
        #metric_impl
    })
}

// Find a field named `datum` or annotated with the `#[datum]` attribute helper. Return its name,
// the type of the field, and the tokens representing the `oximeter::DatumType` enum variant
// corresponding to the field's type.
/*
fn extract_datum_type(item: &ItemStruct) -> syn::Result<&syn::Field> {
    const ERR: &str = "Metric structs must have exactly one field named \
        `datum`; a field annotated with the `#[datum]` \
        attribute helper; or be a tuple struct with \
        one field.";
    match &item.fields {
        Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
            if unnamed.len() != 1 {
                return Err(Error::new(item.span(), ERR));
            }
            Ok(&unnamed[0])
        }
        Fields::Named(fields) => {
            find_datum_field(fields).ok_or_else(|| Error::new(item.span(),
            ERR))?;
            todo!();
            Ok(None);
        }
        _ => Err(Error::new(item.span(), "Struct must contain named fields")),
    }
}
*/

// Find the datum field from the fields of a struct.
//
// For a tuple struct, this returns the first field, of which there must be
// exactly 1.
//
// For a struct with named fields, a field named `datum` or decorated with the
// attribute `#[datum]` or `#[oximeter(datum)]` is allowed, of which there must
// be at most one.
//
// Unit structs are not allowed.
fn find_datum_field(item: &ItemStruct) -> syn::Result<Option<&syn::Field>> {
    match &item.fields {
        Fields::Unit => Err(Error::new(
            item.span(),
            "Struct must be a single-element tuple struct \
            or contain named fields",
        )),
        Fields::Named(named) => {
            let mut datum_field = None;
            for field in named.named.iter() {
                if field.ident.as_ref().unwrap() == "datum" {
                    if datum_field.is_some() {
                        return Err(Error::new(
                            field.span(),
                            "there must be a single datum field",
                        ));
                    }
                    datum_field.replace(field);
                    continue;
                }
                for attr in field.attrs.iter() {
                    match &attr.meta {
                        Meta::Path(p) => {
                            if p.is_ident("datum") {
                                if datum_field.is_some() {
                                    return Err(Error::new(
                                        field.span(),
                                        "there must be a single datum field",
                                    ));
                                }
                                datum_field.replace(field);
                            }
                        }
                        Meta::List(list) => {
                            if attr.path().is_ident("oximeter") {
                                if list.tokens.is_empty() {
                                    return Err(Error::new(
                                        attr.span(),
                                        "`oximeter` attribute may not be empty",
                                    ));
                                }
                                attr.parse_nested_meta(|meta| {
                                    if meta.path.is_ident("datum") {
                                        if !meta.input.is_empty() {
                                            return Err(Error::new(
                                                meta.path.span(),
                                                "datum may not have arguments"
                                            ));
                                        }
                                        if datum_field.is_some() {
                                            if datum_field.is_some() {
                                                return Err(Error::new(
                                                    field.span(),
                                                    "there must be a single datum field"
                                                ));
                                            }
                                        }
                                        datum_field.replace(field);
                                        return Ok(());
                                    }
                                    Err(Error::new(meta.path.span(), "Unrecognized oximeter attribute"))
                                })?;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(datum_field)
        }
        Fields::Unnamed(fields) => {
            if fields.unnamed.len() != 1 {
                return Err(Error::new(
                    fields.span(),
                    "Only a single field is supported",
                ));
            }
            Ok(Some(&fields.unnamed[0]))
        }
    }
}

fn build_shared_methods(fields: &[&Field], args: &OximeterArgs) -> TokenStream {
    let field_idents = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect::<Vec<_>>();
    let field_names = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap().to_string())
        .collect::<Vec<_>>();
    let name = &args.name;
    let description = &args.description;
    let version = args.version.get();

    quote! {
        fn name(&self) -> &'static str {
            #name
        }

        fn description(&self) -> &'static str {
            #description
        }

        fn version(&self) -> ::std::num::NonZeroU8 {
            // Safety: This is checked at the time the macro is run.
            unsafe { ::std::num::NonZeroU8::new_unchecked(#version) }
        }

        fn field_names(&self) -> &'static [&'static str] {
            &[#(#field_names),*]
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
    args: &OximeterArgs,
) -> TokenStream {
    let shared_methods = build_shared_methods(fields, args);
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
    args: &OximeterArgs,
) -> TokenStream {
    let shared_methods = build_shared_methods(fields, args);
    let datum_field_ident = match datum_field.ident.as_ref() {
        Some(id) => quote! { #id },
        None => quote! { .0 },
    };
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

fn extract_struct_fields(
    fields: &FieldsNamed,
    ignore: Option<impl AsRef<str>>,
) -> Vec<&Field> {
    if let Some(ignore) = ignore {
        fields
            .named
            .iter()
            .filter(|field| *field.ident.as_ref().unwrap() != ignore.as_ref())
            .collect()
    } else {
        fields.named.iter().collect()
    }
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
        out.unwrap();
        //assert!(out.is_ok());
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
    fn test_metric_single_tuple_field() {
        let out = metric_impl(quote! { struct MyMetric(pub i64); });
        out.unwrap();
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
    fn test_oximeter_args_metric_datum_by_field_name() {
        let q = quote! {
            #[derive(Metric)]
            struct MyMetric {
                not_datum: String,
                datum: i64,
            }
        };
        let item: ItemStruct = syn::parse2(q).unwrap();
        let args = oximeter_attribute_args(&item, true).unwrap();
        assert!(
            args.datum_field
                .expect("Should have extracted datum field")
                .ident
                .as_ref()
                .expect("Ident must be named")
                == "datum"
        );
    }

    #[test]
    fn test_oximeter_args_metric_datum_by_annotated_field() {
        let q = quote! {
            #[derive(Metric)]
            struct MyMetric {
                not_datum: String,
                #[datum]
                also_not_datum: i64,
            }
        };
        let item: ItemStruct = syn::parse2(q).unwrap();
        let args = oximeter_attribute_args(&item, true).unwrap();
        assert!(
            args.datum_field
                .expect("Should have extracted datum field")
                .ident
                .as_ref()
                .expect("Ident must be named")
                == "also_not_datum"
        );

        let q = quote! {
            #[derive(Metric)]
            struct MyMetric {
                not_datum: String,
                #[oximeter(datum)]
                also_not_datum: i64,
            }
        };
        let item: ItemStruct = syn::parse2(q).unwrap();
        let args = oximeter_attribute_args(&item, true).unwrap();
        assert!(
            args.datum_field
                .expect("Should have extracted datum field")
                .ident
                .as_ref()
                .expect("Ident must be named")
                == "also_not_datum"
        );
    }

    #[test]
    fn test_oximeter_args_metric_datum_named_and_annotated_fails() {
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Metric)]
            struct MyMetric {
                not_datum: String,
                #[datum]
                also_not_datum: i64,
                datum: i64,
            }
        })
        .unwrap();
        assert!(oximeter_attribute_args(&item, true).is_err());
    }

    #[test]
    fn test_oximeter_args_metric_multiple_annotated_fields() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            #[derive(Metric)]
            struct MyMetric {
                not_datum: String,
                #[datum]
                also_not_datum: i64,
                #[oximeter(datum)]
                also_also: i64,
            }
        })
        .unwrap();
        assert!(oximeter_attribute_args(&item, true).is_err());
    }

    #[test]
    fn test_oximeter_args_metric_named_and_annotated_same_field() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            #[derive(Metric)]
            struct MyMetric {
                not_datum: String,
                #[datum]
                datum: i64,
            }
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, true).unwrap();
        let field = args
            .datum_field
            .as_ref()
            .expect("Should have extracted a datum field");
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
    fn test_oximeter_args_metric_tuple_struct_one_field() {
        let item = syn::parse2::<syn::ItemStruct>(quote! {
            #[derive(Debug)]
            struct Metric(pub f64);
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, true).unwrap();
        let field = args
            .datum_field
            .as_ref()
            .expect("Should have extracted datum field");
        assert!(field.ident.is_none());
        let syn::Type::Path(ref p) = field.ty else {
            panic!("Expected the extracted datum type");
        };
        assert_eq!(p.path.segments.last().unwrap().ident.to_string(), "f64",);
    }

    #[test]
    fn test_oximeter_args_extract_description() {
        let desc = "Some description";
        let item = syn::parse2::<ItemStruct>(quote! {
            #[doc = #desc]
            #[derive(Metric)]
            struct Met(pub f64);
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, true).unwrap();
        assert_eq!(args.description, desc, "Failed to extract description");
    }

    #[test]
    fn test_oximeter_args_extract_overridden_description() {
        let desc = "Some description";
        let new_desc = "New description";
        let item = syn::parse2::<ItemStruct>(quote! {
            #[doc = #desc]
            #[derive(Metric)]
            #[oximeter(description = #new_desc)]
            struct Met(pub f64);
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, true).unwrap();
        assert_eq!(
            args.description, new_desc,
            "Failed to extract overridden description"
        );
    }

    #[test]
    fn test_oximeter_args_extract_name() {
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Target)]
            struct MyTarget;
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, false).unwrap();
        assert_eq!(args.name, "my_target", "Failed to extract target name");
    }

    #[test]
    fn test_oximeter_args_extract_overridden_name() {
        let new_name = "new_name";
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Target)]
            #[oximeter(name = #new_name)]
            struct MyTarget;
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, false).unwrap();
        assert_eq!(args.name, new_name, "Failed to extract target name");
    }

    #[test]
    fn test_oximeter_args_extract_overridden_name_non_snake_case() {
        let new_name = "CamelCase";
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Target)]
            #[oximeter(name = #new_name)]
            struct MyTarget;
        })
        .unwrap();
        oximeter_attribute_args(&item, false).expect_err(
            "Should fail when an overridden name is not snake_case",
        );
    }

    #[test]
    fn test_oximeter_args_extract_version() {
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Target)]
            struct MyTarget;
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, false).unwrap();
        assert_eq!(args.version.get(), 1, "Failed to extract target version");
    }

    #[test]
    fn test_oximeter_args_extract_overridden_version() {
        let version = 2;
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Target)]
            #[oximeter(version = #version)]
            struct MyTarget;
        })
        .unwrap();
        let args = oximeter_attribute_args(&item, false).unwrap();
        assert_eq!(
            args.version.get(),
            version,
            "Failed to extract overridden target version"
        );
    }

    #[test]
    fn test_oximeter_args_fail_on_non_integer_version() {
        let version = "1";
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Target)]
            #[oximeter(version = #version)]
            struct MyTarget;
        })
        .unwrap();
        oximeter_attribute_args(&item, false).expect_err(
            "Should fail to extract a version which is not an integer",
        );
    }

    #[test]
    fn test_oximeter_args_fail_out_of_range_version() {
        let version = 10000;
        let item = syn::parse2::<ItemStruct>(quote! {
            #[derive(Target)]
            #[oximeter(version = #version)]
            struct MyTarget;
        })
        .unwrap();
        oximeter_attribute_args(&item, false)
            .expect_err("Should fail to extract a version which is not a u8");
    }
}
