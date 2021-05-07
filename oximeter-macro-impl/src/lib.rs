use std::str::FromStr;

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, ToTokens, TokenStreamExt};
use syn::spanned::Spanned;
use syn::{
    parse::Parser, punctuated::Punctuated, Data, DeriveInput, Error, Fields, Ident, LitStr, Token,
    Type,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The kind of the metric
#[derive(
    Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, JsonSchema, Serialize, Deserialize,
)]
pub enum MetricKind {
    /// A gauge reflects an instantaneous value of a metric. Must be scalar.
    Gauge,

    /// A cumulative metric is one that accumulates over time. May be scalar or a distribution.
    Cumulative,
}

impl FromStr for MetricKind {
    type Err = String;
    fn from_str(kind: &str) -> Result<Self, Self::Err> {
        match kind {
            "gauge" => Ok(MetricKind::Gauge),
            "cumulative" => Ok(MetricKind::Cumulative),
            _ => Err(
                String::from("Invalid metric kind, must be one of \"gauge\" or \"cumulative\"")
            ),
        }
    }
}

/// The data type of an individual measurement of a metric.
#[derive(
    Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq, Hash, JsonSchema, Serialize, Deserialize,
)]
pub enum MetricType {
    Bool,
    I64,
    F64,
    String,
    Bytes,
    DistributionI64,
    DistributionF64,
}

impl FromStr for MetricType {
    type Err = String;
    fn from_str(type_: &str) -> Result<Self, Self::Err> {
        match type_ {
            "bool" => Ok(MetricType::Bool),
            "i64" => Ok(MetricType::I64),
            "f64" => Ok(MetricType::F64),
            "String" => Ok(MetricType::String),
            "Bytes" => Ok(MetricType::Bytes),
            "DistributionI64" => Ok(MetricType::DistributionI64),
            "DistributionF64" => Ok(MetricType::DistributionF64),
            _ => Err(String::from(
                "Invalid metric type, must be one of
                \"bool\", \"i64\", \"f64\", \"String\", \"Bytes\"
                \"DistributionI64\" or \"DistributionF64\""
            )),
        }
    }
}

impl ToTokens for MetricKind {
    fn to_tokens(&self, stream: &mut TokenStream) {
        enum_to_tokens("Kind", &format!("{:?}", self), stream);
    }
}

impl ToTokens for MetricType {
    fn to_tokens(&self, stream: &mut TokenStream) {
        enum_to_tokens("Type", &format!("{:?}", self), stream);
    }
}

fn enum_to_tokens(suffix: &str, variant: &str, stream: &mut TokenStream) {
    let mod_name = proc_macro2::Ident::new("oximeter", Span::call_site());
    stream.append(mod_name);

    stream.append(proc_macro2::Punct::new(':', proc_macro2::Spacing::Joint));
    stream.append(proc_macro2::Punct::new(':', proc_macro2::Spacing::Alone));
    let enum_name = proc_macro2::Ident::new(&format!("Metric{}", suffix), Span::call_site());
    stream.append(enum_name);

    stream.append(proc_macro2::Punct::new(':', proc_macro2::Spacing::Joint));
    stream.append(proc_macro2::Punct::new(':', proc_macro2::Spacing::Alone));
    let ident = proc_macro2::Ident::new(variant, Span::call_site());
    stream.append(ident);
}

// Implementation of `#[derive(Target)]`
pub fn target(tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    if let Data::Struct(ref data) = item.data {
        let name = &item.ident;
        if let Fields::Named(ref data_fields) = data.fields {
            let (names, types, values, _) = extract_struct_fields(data_fields.named.iter())?;
            if names.is_empty() {
                return Err(Error::new(
                    item.span(),
                    "Structs must have at least one field",
                ));
            }
            return Ok(build_target_trait_impl(&name, &names, &types, &values).into());
        }
        return Err(Error::new(
            item.span(),
            "Can only be derived for structs with named fields",
        ));
    }
    return Err(Error::new(
        item.span(),
        "Can only be derived for structs with named fields",
    ));
}

// Implementation of the `[oximeter::metric]` procedural macro attribute.
pub fn metric(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let (metric_kind, metric_type) = parse_metric_attributes(attr)?;

    let item = syn::parse2::<syn::ItemStruct>(item)?;
    let name = &item.ident;
    if let Fields::Named(ref data_fields) = item.fields {
        let (names, types, values, _) = extract_struct_fields(data_fields.named.iter())?;
        if names.is_empty() {
            return Err(Error::new(
                item.span(),
                "Structs must have at least one field",
            ));
        }
        let metric_impl =
            build_metric_trait_impl(&name, &names, &types, &values, metric_kind, metric_type);
        return Ok(quote! {
            #item
            #metric_impl
        });
    }
    return Err(Error::new(
        item.span(),
        "Can only be derived for structs with named fields",
    ));
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
        impl oximeter::Target for #item_name {
            fn name(&self) -> &'static str {
                #name
            }

            fn field_names(&self) -> &'static [&'static str] {
                &[#(#names),*]
            }

            fn field_types(&self) -> &'static [oximeter::FieldType] {
                &[#(#types,)*]
            }

            fn field_values(&self) -> Vec<oximeter::FieldValue> {
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
    metric_kind: MetricKind,
    metric_type: MetricType,
) -> TokenStream {
    let refs = names.iter().map(|name| format_ident!("{}", name));
    let name = to_snake_case(&format!("{}", item_name));
    let fmt = format!("{}{{}}", "{}:".repeat(values.len()));
    let key_formatter = quote! { format!(#fmt, #(self.#refs),*, #name) };
    quote! {
        impl oximeter::Metric for #item_name {
            fn name(&self) -> &'static str {
                #name
            }

            fn field_names(&self) -> &'static [&'static str] {
                &[#(#names),*]
            }

            fn field_types(&self) -> &'static [oximeter::FieldType] {
                &[#(#types,)*]
            }

            fn field_values(&self) -> Vec<oximeter::FieldValue> {
                vec![#(#values,)*]
            }

            fn key(&self) -> String {
                #key_formatter
            }

            fn metric_kind(&self) -> oximeter::MetricKind {
                #metric_kind
            }

            fn metric_type(&self) -> oximeter::MetricType {
                #metric_type
            }
        }
    }
}

fn extract_struct_fields<'a, F: std::iter::ExactSizeIterator<Item = &'a syn::Field>>(
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
            let field_name = format!("{}", field.ident.as_ref().expect("Field must have a name"));
            let field_type = format!(
                "{}",
                ty.path
                    .segments
                    .iter()
                    .last()
                    .ok_or_else(|| Error::new(ty.path.span(), "Expected a field with a path type"))?
                    .ident
            );

            let (type_variant, value_variant) = extract_variants(&field_type, &field_name)?;
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
    let type_variant =
        syn::parse_str::<syn::Expr>(&format!("::oximeter::FieldType::{}", fragment)).unwrap();
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

// Pull out the metric type/kind from the attribute
fn parse_metric_attributes(attr: TokenStream) -> syn::Result<(MetricKind, MetricType)> {
    let tokens = attr.clone();
    let parser = Punctuated::<LitStr, Token![,]>::parse_separated_nonempty;
    let attrs = parser.parse2(tokens).unwrap();
    if attrs.len() != 2 {
        return Err(Error::new(
            Span::call_site(),
            "Must specify metric kind followed by type",
        ));
    }
    let mut iter = attrs.iter();
    let kind = iter.next().unwrap();
    let metric_kind = kind.value().parse().map_err(|e| Error::new(kind.span(), e))?;
    let type_ = iter.next().unwrap();
    let metric_type = type_.value().parse().map_err(|e| Error::new(type_.span(), e))?;
    if matches!(metric_kind, MetricKind::Gauge) {
        if matches!(metric_type, MetricType::DistributionI64 | MetricType::DistributionF64) {
            return Err(Error::new(type_.span(), "Distribution types are not valid for a gauge"));
        }
    } else {
        if matches!(metric_type, MetricType::Bool | MetricType::String | MetricType::Bytes) {
            return Err(Error::new(type_.span(), "Cumulative metrics must be i64, f64, or distributions"));
        }
    }
    Ok((metric_kind, metric_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_target() {
        let out = target(
        quote!{
            #[derive(Target)]
            struct MyTarget {
                name: String,
                is_cool: bool,
                addr: std::net::IpAddr,
            }
        }.into());
        assert!(out.is_ok());
    }

    #[test]
    fn test_target_unsupported_type() {
        let out = target(
        quote!{
            #[derive(Target)]
            struct MyTarget {
                no_no: f64,
            }
        }.into());
        assert!(out.is_err());
    }

    #[test]
    fn test_target_unit_struct() {
        let out = target(
        quote!{
            #[derive(Target)]
            struct MyTarget;
        }.into());
        assert!(out.is_err());
    }

    #[test]
    fn test_target_enum() {
        let out = target(
        quote!{
            #[derive(Target)]
            enum MyTarget {
                Bad,
                NoGood,
            };
        }.into());
        assert!(out.is_err());
    }

    #[test]
    fn test_gauge_metric() {
        let valid_types = &["bool", "i64", "f64", "String", "Bytes"];
        for type_ in valid_types.iter() {
            let out = metric(
                quote!{"gauge", #type_},
                quote!{
                    struct MyMetric {
                        field: String,
                    }
                }
            );
            assert!(out.is_ok());
        }
    }

    #[test]
    fn test_invalid_gauge_metric() {
        let invalid_types = &["DistributionI64", "DistributionF64"];
        for type_ in invalid_types.iter() {
            let out = metric(
                quote!{"gauge", #type_},
                quote!{
                    struct MyMetric {
                        field: String,
                    }
                }
            );
            assert!(out.is_err());
        }
    }

    #[test]
    fn test_cumulative_metric() {
        let valid_types = &["i64", "f64", "DistributionI64", "DistributionF64"];
        for type_ in valid_types.iter() {
            let out = metric(
                quote!{"cumulative", #type_},
                quote!{
                    struct MyMetric {
                        field: String,
                    }
                }
            );
            assert!(out.is_ok());
        }
    }

    #[test]
    fn test_invalid_cumulative_metric() {
        let invalid_types = &["bool", "String", "Bytes"];
        for type_ in invalid_types.iter() {
            let out = metric(
                quote!{"cumulative", #type_},
                quote!{
                    struct MyMetric {
                        field: String,
                    }
                }
            );
            assert!(out.is_err());
        }
    }
}
