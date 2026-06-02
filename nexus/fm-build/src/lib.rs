// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A surprise tool that will help us later.

use camino::Utf8Path;
use iddqd::IdOrdMap;
use miette::{IntoDiagnostic, WrapErr};
use std::sync::Arc;

pub mod parser;
pub mod schema;
use schema::Schema;

pub struct CodeGenerator {
    #[allow(dead_code)]
    all_fact_tables: IdOrdMap<Arc<schema::FactTable>>,
    des: IdOrdMap<De>,
}

impl CodeGenerator {
    pub fn for_schema(
        schema_path: impl AsRef<Utf8Path>,
    ) -> miette::Result<Self> {
        let schema_path = schema_path.as_ref();
        println!("cargo::rerun-if-changed={schema_path}");
        let sql = std::fs::read_to_string(schema_path)
            .into_diagnostic()
            .wrap_err_with(|| format!("failed to read {schema_path}"))?;
        let Schema { all_fact_tables, fact_tables_by_de } =
            Schema::from_sql(schema_path, &sql)?;
        let des = fact_tables_by_de
            .into_iter()
            .map(|(de_name, fact_tables)| {
                let fact_enum_name = quote::format_ident!("{de_name}Fact");
                De { name: de_name, fact_enum_name, fact_tables }
            })
            .collect();

        Ok(Self { all_fact_tables, des })
    }

    pub fn generate_de_enums(&self) -> impl quote::ToTokens {
        let de_enums = self.des.iter().map(|de| {
            let enum_name = &de.fact_enum_name;
            let variants = de.fact_tables.iter().map(|table| {
                let variant_name = &table.fact_variant_name;
                quote::quote!(
                    #variant_name(#variant_name)
                )
            });
            quote::quote!(
                #[derive(Debug, Clone, PartialEq, Eq)]
                pub enum #enum_name {
                    #(#variants),*
                }
            )
        });
        quote::quote!(
            #(#de_enums)*
        )
    }
}

#[derive(Debug)]
struct De {
    name: Arc<str>,
    fact_enum_name: syn::Ident,
    fact_tables: IdOrdMap<Arc<schema::FactTable>>,
}

impl iddqd::IdOrdItem for De {
    type Key<'a> = &'a str;
    fn key(&self) -> Self::Key<'_> {
        &self.name
    }

    iddqd::id_upcast!();
}

impl schema::FactTable {}
