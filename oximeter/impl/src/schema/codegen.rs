// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Generate Rust types and code from oximeter schema definitions.

use crate::schema::ir::find_schema_version;
use crate::schema::ir::load_schema;
use crate::schema::AuthzScope;
use crate::schema::FieldSource;
use crate::schema::Units;
use crate::DatumType;
use crate::FieldSchema;
use crate::FieldType;
use crate::MetricsError;
use crate::TimeseriesSchema;
use chrono::prelude::DateTime;
use chrono::prelude::Utc;
use proc_macro2::TokenStream;
use quote::quote;

/// Emit types for using one timeseries definition.
///
/// Provided with a TOML-formatted schema definition, this emits Rust types for
/// the target and metric from the latest version.
///
/// All of these items are emitted in a module with the same name as the
/// target.
pub fn use_timeseries(contents: &str) -> Result<TokenStream, MetricsError> {
    let schema = load_schema(contents)?;
    let latest = find_schema_version(schema.iter().cloned(), None);
    let mod_name = quote::format_ident!("{}", latest[0].target_name());
    let types = emit_schema_types(latest);
    Ok(quote! {
        pub mod #mod_name {
            #types
        }
    })
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
impl quote::ToTokens for DatumType {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let toks = match self {
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
        };
        toks.to_tokens(tokens);
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

impl quote::ToTokens for FieldSource {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let toks = match self {
            FieldSource::Target => {
                quote! { ::oximeter::schema::FieldSource::Target }
            }
            FieldSource::Metric => {
                quote! { ::oximeter::schema::FieldSource::Metric }
            }
        };
        toks.to_tokens(tokens);
    }
}

impl quote::ToTokens for FieldType {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let toks = match self {
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
        };
        toks.to_tokens(tokens);
    }
}

impl quote::ToTokens for AuthzScope {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let toks = match self {
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
        };
        toks.to_tokens(tokens);
    }
}

fn quote_creation_time(created: DateTime<Utc>) -> TokenStream {
    let secs = created.timestamp();
    let nsecs = created.timestamp_subsec_nanos();
    quote! {
        ::chrono::DateTime::from_timestamp(#secs, #nsecs).unwrap()
    }
}

impl quote::ToTokens for Units {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let toks = match self {
            Units::Count => quote! { ::oximeter::schema::Units::Count },
            Units::Bytes => quote! { ::oximeter::schema::Units::Bytes },
        };
        toks.to_tokens(tokens);
    }
}

impl quote::ToTokens for FieldSchema {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let name = self.name.as_str();
        let field_type = self.field_type;
        let source = self.source;
        let description = self.description.as_str();
        let toks = quote! {
            ::oximeter::FieldSchema {
                name: String::from(#name),
                field_type: #field_type,
                source: #source,
                description: String::from(#description),
            }
        };
        toks.to_tokens(tokens);
    }
}

impl quote::ToTokens for TimeseriesSchema {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let field_schema = &self.field_schema;
        let timeseries_name = self.timeseries_name.to_string();
        let target_description = self.description.target.as_str();
        let metric_description = self.description.metric.as_str();
        let authz_scope = self.authz_scope;
        let units = self.units;
        let datum_type = self.datum_type;
        let ver = self.version.get();
        let version = quote! { ::core::num::NonZeroU8::new(#ver).unwrap() };
        let created = quote_creation_time(self.created);
        let toks = quote! {
            ::oximeter::schema::TimeseriesSchema {
                timeseries_name: ::oximeter::TimeseriesName::try_from(#timeseries_name).unwrap(),
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
        };
        toks.to_tokens(tokens);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TimeseriesDescription;
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
            pub struct Foo {
                #[doc = "target field"]
                pub f0: ::std::borrow::Cow<'static, str>,
            }

            #[doc = "a metric"]
            #[derive(Clone, Debug, PartialEq, ::oximeter::Metric)]
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
            pub struct Foo {
                #[doc = "target field"]
                pub f0: ::std::borrow::Cow<'static, str>,
            }

            #[doc = "a metric"]
            #[derive(Clone, Debug, PartialEq, ::oximeter::Metric)]
            pub struct Bar {
                pub datum: ::oximeter::types::Cumulative<u64>,
            }
        };

        assert_eq!(tokens.to_string(), expected.to_string());
    }
}
