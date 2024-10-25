// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

use anyhow::Context as _;
use nom::IResult;

/// Build script for generating native type representations from the
/// ground-truth SQL definitions.
fn main() -> anyhow::Result<()> {
    const INIT_FILE: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/schema/single-node/db-init.sql");
    let contents = std::fs::read_to_string(INIT_FILE)
        .with_context(|| format!("Failed to read SQL file: '{INIT_FILE}'"))?;
    let field_type_enum =
        find_enum(&contents, "type").context("failed to find column 'type'")?;
    let field_source_enum = find_enum(&contents, "source")
        .context("failed to find column 'source'")?;
    let datum_type_enum = find_enum(&contents, "datum_type")
        .context("failed to find column 'datum_type'")?;
    std::fs::write(
        format!("{}/enum_defs.rs", std::env::var("OUT_DIR")?),
        [field_type_enum, field_source_enum, datum_type_enum].join("\n"),
    )
    .context("writing output file")?;
    Ok(())
}

// Find an enum in the `timeseries_schema` table definition for the named
// column, and return the corresponding `DataType::Enum8()` definition for it.
fn find_enum(contents: &str, column: &str) -> Option<String> {
    let needle = format!("{column} Enum(\n");
    let start = contents.find(&needle)? + needle.len();
    let s = &contents[start..].trim();
    let (variants, names): (Vec<i8>, Vec<String>) =
        variant_list(s).ok()?.1.into_iter().unzip();
    let enum_map = quote::format_ident!("{}_ENUM_MAP", column.to_uppercase());
    let enum_rev_map =
        quote::format_ident!("{}_ENUM_REV_MAP", column.to_uppercase());
    let enum_type =
        quote::format_ident!("{}_ENUM_DATA_TYPE", column.to_uppercase());
    let parsed_type = if column == "type" {
        quote::quote! { ::oximeter::FieldType }
    } else if column == "source" {
        quote::quote! { ::oximeter::FieldSource }
    } else if column == "datum_type" {
        quote::quote! { ::oximeter::DatumType }
    } else {
        unreachable!();
    };
    Some(quote::quote! {
        /// Mapping from the variant index to the string form.
        #[allow(dead_code)]
        static #enum_map: ::std::sync::LazyLock<::indexmap::IndexMap<i8, String>> = ::std::sync::LazyLock::new(|| {
            ::indexmap::IndexMap::from([
                #((#variants, String::from(#names))),*
            ])
        });
        /// Reverse mapping, from the _parsed_ form to the variant index.
        #[allow(dead_code)]
        static #enum_rev_map: ::std::sync::LazyLock<::indexmap::IndexMap<#parsed_type, i8>> = ::std::sync::LazyLock::new(|| {
            ::indexmap::IndexMap::from([
                #((<#parsed_type as ::std::str::FromStr>::from_str(#names).unwrap(), #variants)),*
            ])
        });
        /// Actual DataType::Enum8(_) with the contained variant-to-name mapping.
        #[allow(dead_code)]
        static #enum_type: ::std::sync::LazyLock<crate::native::block::DataType> = ::std::sync::LazyLock::new(|| {
            crate::native::block::DataType::Enum8(
                ::indexmap::IndexMap::from([
                    #((#variants, String::from(#names))),*
                ])
            )
        });
    }.to_string())
}

fn variant_list(s: &str) -> IResult<&str, Vec<(i8, String)>> {
    nom::multi::separated_list1(
        nom::bytes::complete::is_a(" ,\n"),
        single_variant,
    )(s)
}

fn single_variant(s: &str) -> IResult<&str, (i8, String)> {
    nom::combinator::map(
        nom::sequence::separated_pair(
            nom::sequence::delimited(
                nom::bytes::complete::tag("'"),
                nom::character::complete::alphanumeric1,
                nom::bytes::complete::tag("'"),
            ),
            nom::bytes::complete::tag(" = "),
            nom::character::complete::i8,
        ),
        |(name, variant): (&str, i8)| (variant, name.to_string()),
    )(s)
}
