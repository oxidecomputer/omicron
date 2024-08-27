// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Generate Rust types and code from oximeter schema definitions.

use crate::ir::find_schema_version;
use crate::ir::load_schema;
use chrono::prelude::DateTime;
use chrono::prelude::Utc;
use oximeter_types::AuthzScope;
use oximeter_types::DatumType;
use oximeter_types::FieldSchema;
use oximeter_types::FieldSource;
use oximeter_types::FieldType;
use oximeter_types::MetricsError;
use oximeter_types::TimeseriesSchema;
use oximeter_types::Units;
use proc_macro2::TokenStream;
use quote::quote;

/// Emit types for using one timeseries definition.
///
/// Provided with a TOML-formatted schema definition, this emits Rust types for
/// the target and metric from the latest version; and a function that returns
/// the `TimeseriesSchema` for _all_ versions of the timeseries.
///
/// Both of these items are emitted in a module with the same name as the
/// target.
pub fn use_timeseries(contents: &str) -> Result<TokenStream, MetricsError> {
    let schema = load_schema(contents)?;
    let latest = find_schema_version(schema.iter().cloned(), None);
    let mod_name = quote::format_ident!("{}", latest[0].target_name());
    let types = emit_schema_types(latest);
    let func = emit_schema_function(schema.iter());
    Ok(quote! {
        pub mod #mod_name {
            #types
            #func
        }
    })
}

fn emit_schema_function<'a>(
    list: impl Iterator<Item = &'a TimeseriesSchema>,
) -> TokenStream {
    let list = list.map(quote_timeseries_schema);
    quote! {
        pub fn timeseries_schema() -> Vec<::oximeter::schema::TimeseriesSchema> {
            vec![
                #(#list),*
            ]
        }
    }
}

fn emit_schema_types(list: Vec<TimeseriesSchema>) -> TokenStream {
    let first_schema = list.first().expect("load_schema ensures non-empty");
    let target_def = emit_target(first_schema);
    let metric_defs = emit_metrics(&list);
    quote! {
        #target_def
        #metric_defs
    }
}

fn emit_metrics(schema: &[TimeseriesSchema]) -> TokenStream {
    let items = schema.iter().map(|s| emit_one(FieldSource::Metric, s));
    quote! { #(#items)* }
}

fn emit_target(schema: &TimeseriesSchema) -> TokenStream {
    emit_one(FieldSource::Target, schema)
}

/// Return true if all the fields in the schema are `Copy`.
fn fields_are_copyable<'a>(
    mut fields: impl Iterator<Item = &'a FieldSchema>,
) -> bool {
    // Do a positive match, to ensure new variants don't actually derive copy
    // inappropriately. Better we clone, in that case.
    fields.all(FieldSchema::is_copyable)
}

/// Return true if the datum type is copyable.
fn datum_type_is_copyable(datum_type: DatumType) -> bool {
    match datum_type {
        DatumType::Bool
        | DatumType::I8
        | DatumType::U8
        | DatumType::I16
        | DatumType::U16
        | DatumType::I32
        | DatumType::U32
        | DatumType::I64
        | DatumType::U64
        | DatumType::CumulativeI64
        | DatumType::CumulativeU64
        | DatumType::CumulativeF32
        | DatumType::CumulativeF64
        | DatumType::F32
        | DatumType::F64 => true,
        DatumType::String
        | DatumType::Bytes
        | DatumType::HistogramI8
        | DatumType::HistogramU8
        | DatumType::HistogramI16
        | DatumType::HistogramU16
        | DatumType::HistogramI32
        | DatumType::HistogramU32
        | DatumType::HistogramI64
        | DatumType::HistogramU64
        | DatumType::HistogramF32
        | DatumType::HistogramF64 => false,
    }
}

/// Return `true` if values of this datum are partially ordered (can derive
/// `PartialOrd`.)
fn datum_type_is_partially_ordered(datum_type: DatumType) -> bool {
    match datum_type {
        DatumType::Bool
        | DatumType::I8
        | DatumType::U8
        | DatumType::I16
        | DatumType::U16
        | DatumType::I32
        | DatumType::U32
        | DatumType::I64
        | DatumType::U64
        | DatumType::String
        | DatumType::Bytes
        | DatumType::CumulativeI64
        | DatumType::CumulativeU64
        | DatumType::CumulativeF32
        | DatumType::CumulativeF64
        | DatumType::F32
        | DatumType::F64 => true,
        DatumType::HistogramI8
        | DatumType::HistogramU8
        | DatumType::HistogramI16
        | DatumType::HistogramU16
        | DatumType::HistogramI32
        | DatumType::HistogramU32
        | DatumType::HistogramI64
        | DatumType::HistogramU64
        | DatumType::HistogramF32
        | DatumType::HistogramF64 => false,
    }
}

/// Return `true` if values of this datum are totally ordered (can derive
/// `Ord`.)
fn datum_type_is_totally_ordered(datum_type: DatumType) -> bool {
    match datum_type {
        DatumType::Bool
        | DatumType::I8
        | DatumType::U8
        | DatumType::I16
        | DatumType::U16
        | DatumType::I32
        | DatumType::U32
        | DatumType::I64
        | DatumType::U64
        | DatumType::String
        | DatumType::Bytes
        | DatumType::CumulativeI64
        | DatumType::CumulativeU64
        | DatumType::CumulativeF32
        | DatumType::CumulativeF64 => true,
        DatumType::F32
        | DatumType::F64
        | DatumType::HistogramI8
        | DatumType::HistogramU8
        | DatumType::HistogramI16
        | DatumType::HistogramU16
        | DatumType::HistogramI32
        | DatumType::HistogramU32
        | DatumType::HistogramI64
        | DatumType::HistogramU64
        | DatumType::HistogramF32
        | DatumType::HistogramF64 => false,
    }
}

/// Return `true` if values of this datum are hashable (can derive `Hash`).
fn datum_type_is_hashable(datum_type: DatumType) -> bool {
    match datum_type {
        DatumType::Bool
        | DatumType::I8
        | DatumType::U8
        | DatumType::I16
        | DatumType::U16
        | DatumType::I32
        | DatumType::U32
        | DatumType::I64
        | DatumType::U64
        | DatumType::String
        | DatumType::Bytes
        | DatumType::CumulativeI64
        | DatumType::CumulativeU64
        | DatumType::CumulativeF32
        | DatumType::CumulativeF64 => true,
        DatumType::F32
        | DatumType::F64
        | DatumType::HistogramI8
        | DatumType::HistogramU8
        | DatumType::HistogramI16
        | DatumType::HistogramU16
        | DatumType::HistogramI32
        | DatumType::HistogramU32
        | DatumType::HistogramI64
        | DatumType::HistogramU64
        | DatumType::HistogramF32
        | DatumType::HistogramF64 => false,
    }
}

fn compute_extra_derives(
    source: FieldSource,
    schema: &TimeseriesSchema,
) -> TokenStream {
    match source {
        FieldSource::Target => {
            if fields_are_copyable(schema.target_fields()) {
                quote! { #[derive(Copy, Eq, Hash, Ord, PartialOrd)] }
            } else {
                quote! { #[derive(Eq, Hash, Ord, PartialOrd)] }
            }
        }
        FieldSource::Metric => {
            let mut derives = Vec::new();
            if fields_are_copyable(schema.metric_fields())
                && datum_type_is_copyable(schema.datum_type)
            {
                derives.push(quote! { Copy });
            }
            if datum_type_is_partially_ordered(schema.datum_type) {
                derives.push(quote! { PartialOrd });
                if datum_type_is_totally_ordered(schema.datum_type) {
                    derives.push(quote! { Eq, Ord });
                }
            }
            if datum_type_is_hashable(schema.datum_type) {
                derives.push(quote! { Hash })
            }
            if derives.is_empty() {
                quote! {}
            } else {
                quote! { #[derive(#(#derives),*)] }
            }
        }
    }
}

fn emit_one(source: FieldSource, schema: &TimeseriesSchema) -> TokenStream {
    let name = match source {
        FieldSource::Target => schema.target_name(),
        FieldSource::Metric => schema.metric_name(),
    };
    let struct_name =
        quote::format_ident!("{}", format!("{}", heck::AsPascalCase(name)));
    let field_defs: Vec<_> = schema
        .field_schema
        .iter()
        .filter_map(|s| {
            if s.source == source {
                let name = quote::format_ident!("{}", s.name);
                let type_ = emit_rust_type_for_field(s.field_type);
                let docstring = s.description.as_str();
                Some(quote! {
                    #[doc = #docstring]
                    pub #name: #type_
                })
            } else {
                None
            }
        })
        .collect();
    let extra_derives = compute_extra_derives(source, schema);
    let (oximeter_trait, maybe_datum, type_docstring) = match source {
        FieldSource::Target => (
            quote! {::oximeter::Target },
            quote! {},
            schema.description.target.as_str(),
        ),
        FieldSource::Metric => {
            let datum_type = emit_rust_type_for_datum_type(schema.datum_type);
            (
                quote! { ::oximeter::Metric },
                quote! { pub datum: #datum_type, },
                schema.description.metric.as_str(),
            )
        }
    };
    quote! {
        #[doc = #type_docstring]
        #[derive(Clone, Debug, PartialEq, #oximeter_trait)]
        #extra_derives
        pub struct #struct_name {
            #( #field_defs, )*
            #maybe_datum
        }
    }
}

// Implement ToTokens for the components of a `TimeseriesSchema`.
//
// This is used so that we can emit a function that will return the same data as
// we parse from the TOML file with the timeseries definition, as a way to
// export the definitions without needing that actual file at runtime.
fn quote_datum_type(datum_type: DatumType) -> TokenStream {
    match datum_type {
        DatumType::Bool => quote! { ::oximeter::DatumType::Bool },
        DatumType::I8 => quote! { ::oximeter::DatumType::I8 },
        DatumType::U8 => quote! { ::oximeter::DatumType::U8 },
        DatumType::I16 => quote! { ::oximeter::DatumType::I16 },
        DatumType::U16 => quote! { ::oximeter::DatumType::U16 },
        DatumType::I32 => quote! { ::oximeter::DatumType::I32 },
        DatumType::U32 => quote! { ::oximeter::DatumType::U32 },
        DatumType::I64 => quote! { ::oximeter::DatumType::I64 },
        DatumType::U64 => quote! { ::oximeter::DatumType::U64 },
        DatumType::F32 => quote! { ::oximeter::DatumType::F32 },
        DatumType::F64 => quote! { ::oximeter::DatumType::F64 },
        DatumType::String => quote! { ::oximeter::DatumType::String },
        DatumType::Bytes => quote! { ::oximeter::DatumType::Bytes },
        DatumType::CumulativeI64 => {
            quote! { ::oximeter::DatumType::CumulativeI64 }
        }
        DatumType::CumulativeU64 => {
            quote! { ::oximeter::DatumType::CumulativeU64 }
        }
        DatumType::CumulativeF32 => {
            quote! { ::oximeter::DatumType::CumulativeF32 }
        }
        DatumType::CumulativeF64 => {
            quote! { ::oximeter::DatumType::CumulativeF64 }
        }
        DatumType::HistogramI8 => {
            quote! { ::oximeter::DatumType::HistogramI8 }
        }
        DatumType::HistogramU8 => {
            quote! { ::oximeter::DatumType::HistogramU8 }
        }
        DatumType::HistogramI16 => {
            quote! { ::oximeter::DatumType::HistogramI16 }
        }
        DatumType::HistogramU16 => {
            quote! { ::oximeter::DatumType::HistogramU16 }
        }
        DatumType::HistogramI32 => {
            quote! { ::oximeter::DatumType::HistogramI32 }
        }
        DatumType::HistogramU32 => {
            quote! { ::oximeter::DatumType::HistogramU32 }
        }
        DatumType::HistogramI64 => {
            quote! { ::oximeter::DatumType::HistogramI64 }
        }
        DatumType::HistogramU64 => {
            quote! { ::oximeter::DatumType::HistogramU64 }
        }
        DatumType::HistogramF32 => {
            quote! { ::oximeter::DatumType::HistogramF32 }
        }
        DatumType::HistogramF64 => {
            quote! { ::oximeter::DatumType::HistogramF64 }
        }
    }
}

// Emit tokens representing the Rust path matching the provided datum type.
fn emit_rust_type_for_datum_type(datum_type: DatumType) -> TokenStream {
    match datum_type {
        DatumType::Bool => quote! { bool },
        DatumType::I8 => quote! { i8 },
        DatumType::U8 => quote! { u8 },
        DatumType::I16 => quote! { i16 },
        DatumType::U16 => quote! { u16 },
        DatumType::I32 => quote! { i32 },
        DatumType::U32 => quote! { u32 },
        DatumType::I64 => quote! { i64 },
        DatumType::U64 => quote! { u64 },
        DatumType::F32 => quote! { f32 },
        DatumType::F64 => quote! { f64 },
        DatumType::String => quote! { String },
        DatumType::Bytes => quote! { ::bytes::Bytes },
        DatumType::CumulativeI64 => {
            quote! { ::oximeter::types::Cumulative<i64> }
        }
        DatumType::CumulativeU64 => {
            quote! { ::oximeter::types::Cumulative<u64> }
        }
        DatumType::CumulativeF32 => {
            quote! { ::oximeter::types::Cumulative<f32> }
        }
        DatumType::CumulativeF64 => {
            quote! { ::oximeter::types::Cumulative<f64> }
        }
        DatumType::HistogramI8 => {
            quote! { ::oximeter::histogram::Histogram<i8> }
        }
        DatumType::HistogramU8 => {
            quote! { ::oximeter::histogram::Histogram<u8> }
        }
        DatumType::HistogramI16 => {
            quote! { ::oximeter::histogram::Histogram<i16> }
        }
        DatumType::HistogramU16 => {
            quote! { ::oximeter::histogram::Histogram<u16> }
        }
        DatumType::HistogramI32 => {
            quote! { ::oximeter::histogram::Histogram<i32> }
        }
        DatumType::HistogramU32 => {
            quote! { ::oximeter::histogram::Histogram<u32> }
        }
        DatumType::HistogramI64 => {
            quote! { ::oximeter::histogram::Histogram<i64> }
        }
        DatumType::HistogramU64 => {
            quote! { ::oximeter::histogram::Histogram<u64> }
        }
        DatumType::HistogramF32 => {
            quote! { ::oximeter::histogram::Histogram<f32> }
        }
        DatumType::HistogramF64 => {
            quote! { ::oximeter::histogram::Histogram<f64> }
        }
    }
}

// Generate the quoted path to the Rust type matching the given field type.
fn emit_rust_type_for_field(field_type: FieldType) -> TokenStream {
    match field_type {
        FieldType::String => quote! { ::std::borrow::Cow<'static, str> },
        FieldType::I8 => quote! { i8 },
        FieldType::U8 => quote! { u8 },
        FieldType::I16 => quote! { i16 },
        FieldType::U16 => quote! { u16 },
        FieldType::I32 => quote! { i32 },
        FieldType::U32 => quote! { u32 },
        FieldType::I64 => quote! { i64 },
        FieldType::U64 => quote! { u64 },
        FieldType::IpAddr => quote! { ::core::net::IpAddr },
        FieldType::Uuid => quote! { ::uuid::Uuid },
        FieldType::Bool => quote! { bool },
    }
}

fn quote_field_source(source: FieldSource) -> TokenStream {
    match source {
        FieldSource::Target => {
            quote! { ::oximeter::schema::FieldSource::Target }
        }
        FieldSource::Metric => {
            quote! { ::oximeter::schema::FieldSource::Metric }
        }
    }
}

fn quote_field_type(field_type: FieldType) -> TokenStream {
    match field_type {
        FieldType::String => quote! { ::oximeter::FieldType::String },
        FieldType::I8 => quote! { ::oximeter::FieldType::I8 },
        FieldType::U8 => quote! { ::oximeter::FieldType::U8 },
        FieldType::I16 => quote! { ::oximeter::FieldType::I16 },
        FieldType::U16 => quote! { ::oximeter::FieldType::U16 },
        FieldType::I32 => quote! { ::oximeter::FieldType::I32 },
        FieldType::U32 => quote! { ::oximeter::FieldType::U32 },
        FieldType::I64 => quote! { ::oximeter::FieldType::I64 },
        FieldType::U64 => quote! { ::oximeter::FieldType::U64 },
        FieldType::IpAddr => quote! { ::oximeter::FieldType::IpAddr },
        FieldType::Uuid => quote! { ::oximeter::FieldType::Uuid },
        FieldType::Bool => quote! { ::oximeter::FieldType::Bool },
    }
}

fn quote_authz_scope(authz_scope: AuthzScope) -> TokenStream {
    match authz_scope {
        AuthzScope::Fleet => {
            quote! { ::oximeter::schema::AuthzScope::Fleet }
        }
        AuthzScope::Silo => quote! { ::oximeter::schema::AuthzScope::Silo },
        AuthzScope::Project => {
            quote! { ::oximeter::schema::AuthzScope::Project }
        }
        AuthzScope::ViewableToAll => {
            quote! { ::oximeter::schema::AuthzScope::ViewableToAll }
        }
    }
}

fn quote_creation_time(created: DateTime<Utc>) -> TokenStream {
    let secs = created.timestamp();
    let nsecs = created.timestamp_subsec_nanos();
    quote! {
        ::chrono::DateTime::from_timestamp(#secs, #nsecs).unwrap()
    }
}

fn quote_units(units: Units) -> TokenStream {
    match units {
        Units::None => quote! { ::oximeter::schema::Units::None },
        Units::Count => quote! { ::oximeter::schema::Units::Count },
        Units::Bytes => quote! { ::oximeter::schema::Units::Bytes },
        Units::Seconds => quote! { ::oximeter::schema::Units::Seconds },
        Units::Nanoseconds => {
            quote! { ::oximeter::schema::Units::Nanoseconds }
        }
        Units::Amps => quote! { ::oximeter::schema::Units::Amps },
        Units::Volts => quote! { ::oximeter::schema::Units::Volts },
        Units::DegreesCelcius => {
            quote! { ::oximeter::schema::Units::DegreesCelcius }
        }
        Units::Rpm => quote! { ::oximeter::schema::Units::Rpm },
    }
}

fn quote_field_schema(field_schema: &FieldSchema) -> TokenStream {
    let name = field_schema.name.as_str();
    let field_type = quote_field_type(field_schema.field_type);
    let source = quote_field_source(field_schema.source);
    let description = field_schema.description.as_str();
    quote! {
        ::oximeter::FieldSchema {
            name: String::from(#name),
            field_type: #field_type,
            source: #source,
            description: String::from(#description),
        }
    }
}

fn quote_timeseries_schema(
    timeseries_schema: &TimeseriesSchema,
) -> TokenStream {
    let field_schema =
        timeseries_schema.field_schema.iter().map(quote_field_schema);
    let timeseries_name = timeseries_schema.timeseries_name.to_string();
    let target_description = timeseries_schema.description.target.as_str();
    let metric_description = timeseries_schema.description.metric.as_str();
    let authz_scope = quote_authz_scope(timeseries_schema.authz_scope);
    let units = quote_units(timeseries_schema.units);
    let datum_type = quote_datum_type(timeseries_schema.datum_type);
    let ver = timeseries_schema.version.get();
    let version = quote! { ::core::num::NonZeroU8::new(#ver).unwrap() };
    let created = quote_creation_time(timeseries_schema.created);
    quote! {
        ::oximeter::schema::TimeseriesSchema {
            timeseries_name:
                <::oximeter::TimeseriesName as ::std::convert::TryFrom<&str>>::try_from(
                    #timeseries_name
                ).unwrap(),
            description: ::oximeter::schema::TimeseriesDescription {
                target: String::from(#target_description),
                metric: String::from(#metric_description),
            },
            authz_scope: #authz_scope,
            units: #units,
            field_schema: ::std::collections::BTreeSet::from([
                #(#field_schema),*
            ]),
            datum_type: #datum_type,
            version: #version,
            created: #created,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oximeter_types::TimeseriesDescription;
    use std::{collections::BTreeSet, num::NonZeroU8};

    #[test]
    fn emit_schema_types_generates_expected_tokens() {
        let schema = TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([
                FieldSchema {
                    name: "f0".into(),
                    field_type: FieldType::String,
                    source: FieldSource::Target,
                    description: "target field".into(),
                },
                FieldSchema {
                    name: "f1".into(),
                    field_type: FieldType::Uuid,
                    source: FieldSource::Metric,
                    description: "metric field".into(),
                },
            ]),
            datum_type: DatumType::CumulativeU64,
            version: NonZeroU8::new(1).unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Bytes,
            created: Utc::now(),
        };

        let tokens = emit_schema_types(vec![schema.clone()]);

        let expected = quote! {
            #[doc = "a target"]
            #[derive(Clone, Debug, PartialEq, ::oximeter::Target)]
            #[derive(Eq, Hash, Ord, PartialOrd)]
            pub struct Foo {
                #[doc = "target field"]
                pub f0: ::std::borrow::Cow<'static, str>,
            }

            #[doc = "a metric"]
            #[derive(Clone, Debug, PartialEq, ::oximeter::Metric)]
            #[derive(Copy, PartialOrd, Eq, Ord, Hash)]
            pub struct Bar {
                #[doc = "metric field"]
                pub f1: ::uuid::Uuid,
                pub datum: ::oximeter::types::Cumulative<u64>,
            }
        };

        assert_eq!(tokens.to_string(), expected.to_string());
    }

    #[test]
    fn emit_schema_types_with_no_metric_fields_generates_expected_tokens() {
        let schema = TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::String,
                source: FieldSource::Target,
                description: "target field".into(),
            }]),
            datum_type: DatumType::CumulativeU64,
            version: NonZeroU8::new(1).unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Bytes,
            created: Utc::now(),
        };

        let tokens = emit_schema_types(vec![schema.clone()]);

        let expected = quote! {
            #[doc = "a target"]
            #[derive(Clone, Debug, PartialEq, ::oximeter::Target)]
            #[derive(Eq, Hash, Ord, PartialOrd)]
            pub struct Foo {
                #[doc = "target field"]
                pub f0: ::std::borrow::Cow<'static, str>,
            }

            #[doc = "a metric"]
            #[derive(Clone, Debug, PartialEq, ::oximeter::Metric)]
            #[derive(Copy, PartialOrd, Eq, Ord, Hash)]
            pub struct Bar {
                pub datum: ::oximeter::types::Cumulative<u64>,
            }
        };

        assert_eq!(tokens.to_string(), expected.to_string());
    }

    #[test]
    fn compute_extra_derives_respects_non_copy_fields() {
        // Metric fields are not copy, even though datum is.
        let schema = TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::String,
                source: FieldSource::Metric,
                description: "metric field".into(),
            }]),
            datum_type: DatumType::CumulativeU64,
            version: NonZeroU8::new(1).unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Bytes,
            created: Utc::now(),
        };
        let tokens = compute_extra_derives(FieldSource::Metric, &schema);
        assert_eq!(
            tokens.to_string(),
            quote! { #[derive(PartialOrd, Eq, Ord, Hash)] }.to_string(),
            "Copy should not be derived for a datum type that is copy, \
            when the fields themselves are not copy."
        );
    }

    #[test]
    fn compute_extra_derives_respects_non_copy_datum_types() {
        // Fields are copy, but datum is not.
        let schema = TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::Uuid,
                source: FieldSource::Metric,
                description: "metric field".into(),
            }]),
            datum_type: DatumType::String,
            version: NonZeroU8::new(1).unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Bytes,
            created: Utc::now(),
        };
        let tokens = compute_extra_derives(FieldSource::Metric, &schema);
        assert_eq!(
            tokens.to_string(),
            quote! { #[derive(PartialOrd, Eq, Ord, Hash)] }.to_string(),
            "Copy should not be derived for a datum type that is not copy, \
            when the fields themselves are copy."
        );
    }

    #[test]
    fn compute_extra_derives_respects_partially_ordered_datum_types() {
        // No fields, datum is partially- but not totally-ordered.
        let schema = TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::Uuid,
                source: FieldSource::Target,
                description: "target field".into(),
            }]),
            datum_type: DatumType::F64,
            version: NonZeroU8::new(1).unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Bytes,
            created: Utc::now(),
        };
        let tokens = compute_extra_derives(FieldSource::Metric, &schema);
        assert_eq!(
            tokens.to_string(),
            quote! { #[derive(Copy, PartialOrd)] }.to_string(),
            "Should derive only PartialOrd for a metric type that is \
            not totally-ordered."
        );
    }

    #[test]
    fn compute_extra_derives_respects_totally_ordered_datum_types() {
        // No fields, datum is also totally-ordered
        let schema = TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::Uuid,
                source: FieldSource::Target,
                description: "target field".into(),
            }]),
            datum_type: DatumType::U64,
            version: NonZeroU8::new(1).unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Bytes,
            created: Utc::now(),
        };
        let tokens = compute_extra_derives(FieldSource::Metric, &schema);
        assert_eq!(
            tokens.to_string(),
            quote! { #[derive(Copy, PartialOrd, Eq, Ord, Hash)] }.to_string(),
            "Should derive Ord for a metric type that is totally-ordered."
        );
    }

    #[test]
    fn compute_extra_derives_respects_datum_type_with_no_extra_derives() {
        // No metric fields, and histograms don't admit any other derives.
        let schema = TimeseriesSchema {
            timeseries_name: "foo:bar".parse().unwrap(),
            description: TimeseriesDescription {
                target: "a target".into(),
                metric: "a metric".into(),
            },
            field_schema: BTreeSet::from([FieldSchema {
                name: "f0".into(),
                field_type: FieldType::String,
                source: FieldSource::Target,
                description: "target field".into(),
            }]),
            datum_type: DatumType::HistogramF64,
            version: NonZeroU8::new(1).unwrap(),
            authz_scope: AuthzScope::Fleet,
            units: Units::Bytes,
            created: Utc::now(),
        };
        let tokens = compute_extra_derives(FieldSource::Metric, &schema);
        assert!(
            tokens.is_empty(),
            "A histogram has no extra derives, so a timeseries schema \
            with no metric fields should also have no extra derives."
        );
    }
}
