// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::IdOrdMap;
use miette::IntoDiagnostic;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::CreateTable;
use sqlparser::ast::DataType;
use sqlparser::ast::TimezoneInfo;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct Schema {
    #[allow(dead_code)]
    pub(crate) all_fact_tables: IdOrdMap<Arc<FactTable>>,
    pub(crate) fact_tables_by_de: BTreeMap<Arc<str>, IdOrdMap<Arc<FactTable>>>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct FactTable {
    pub(crate) create_stmt: CreateTable,
    pub(crate) table_name: String,
    // Rust ident for the diagnosis engine enum variant
    pub(crate) de_name: Arc<str>,
    // Rust ident for the fact variant enum variant
    pub(crate) fact_variant_name: String,
}

impl iddqd::IdOrdItem for FactTable {
    type Key<'a> = &'a str;

    fn key(&self) -> Self::Key<'_> {
        &self.table_name
    }

    iddqd::id_upcast!();
}

impl FactTable {
    pub(crate) fn gen_diesel_schema(
        &self,
        crdb_to_diesel_enum_type_names: &HashMap<&str, &str>,
    ) -> miette::Result<impl quote::ToTokens> {
        let table_name = quote::format_ident!("{}", self.table_name);
        let columns = self
            .create_stmt
            .columns
            .iter()
            .map(|col| {
                let colname = quote::format_ident!("{}", col.name.value);
                let coltype =
                    to_diesel_schema_type(col, crdb_to_diesel_enum_type_names)?;
                Ok(quote::quote!(#colname -> #coltype))
            })
            .collect::<miette::Result<Vec<_>>>()?;
        Ok(quote::quote! {
            table! {
                #table_name (id, sitrep_id) {
                    #(#columns),*
                }
            }
        })
    }
}

/// Generates the Diesel `sql_types` type for `col`, wrapping it in `Nullable<>`
/// unless the column is declared `NOT NULL`.
fn to_diesel_schema_type(
    col: &ColumnDef,
    crdb_to_diesel_enum_type_names: &HashMap<&str, &str>,
) -> miette::Result<impl quote::ToTokens> {
    let not_null = col
        .options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::NotNull));
    let inner =
        diesel_column_type(&col.data_type, crdb_to_diesel_enum_type_names)?;
    Ok(if not_null {
        quote::quote!(#inner)
    } else {
        quote::quote!(Nullable<#inner>)
    })
}

/// Maps a SQL column data type to the corresponding Diesel `sql_types` type,
/// wrapping array types' element type in `Array<>`.
fn diesel_column_type(
    data_type: &DataType,
    crdb_to_diesel_enum_type_names: &HashMap<&str, &str>,
) -> miette::Result<impl quote::ToTokens> {
    use sqlparser::ast::ArrayElemTypeDef;
    let tokens = match data_type {
        DataType::Array(elem) => {
            let element = match elem {
                ArrayElemTypeDef::SquareBracket(ty, _)
                | ArrayElemTypeDef::AngleBracket(ty)
                | ArrayElemTypeDef::Parenthesis(ty) => ty,
                ArrayElemTypeDef::None => {
                    miette::bail!("array column has no element type");
                }
            };
            let element =
                diesel_scalar_type(element, crdb_to_diesel_enum_type_names)?;
            quote::quote!(Array<#element>)
        }
        scalar => {
            let scalar =
                diesel_scalar_type(scalar, crdb_to_diesel_enum_type_names)?;
            quote::quote!(#scalar)
        }
    };
    Ok(tokens)
}

fn diesel_scalar_type(
    data_type: &DataType,
    crdb_to_diesel_enum_type_names: &HashMap<&str, &str>,
) -> miette::Result<impl quote::ToTokens> {
    let tokens = match data_type {
        DataType::Uuid => quote::quote!(Uuid),
        DataType::Bool | DataType::Boolean => quote::quote!(Bool),
        DataType::Text
        | DataType::String(_)
        | DataType::Varchar(_)
        | DataType::Nvarchar(_)
        | DataType::Char(_)
        | DataType::Character(_)
        | DataType::CharVarying(_)
        | DataType::CharacterVarying(_) => quote::quote!(Text),
        DataType::SmallInt(_) | DataType::Int2(_) => quote::quote!(Int2),
        DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) => {
            quote::quote!(Int4)
        }
        DataType::BigInt(_) | DataType::Int8(_) | DataType::Int64 => {
            quote::quote!(Int8)
        }
        DataType::Real | DataType::Float4 => quote::quote!(Float),
        DataType::Double(_) | DataType::DoublePrecision | DataType::Float8 => {
            quote::quote!(Double)
        }
        DataType::Numeric(_) | DataType::Decimal(_) => quote::quote!(Numeric),
        DataType::JSON | DataType::JSONB => quote::quote!(Jsonb),
        DataType::Date => quote::quote!(Date),
        DataType::Bytea
        | DataType::Bytes(_)
        | DataType::Blob(_)
        | DataType::Binary(_) => quote::quote!(Binary),
        DataType::Timestamp(
            _,
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone,
        ) => quote::quote!(Timestamptz),
        DataType::Timestamp(
            _,
            TimezoneInfo::None | TimezoneInfo::WithoutTimeZone,
        ) => quote::quote!(Timestamp),
        DataType::Custom(name, _) => {
            let crdb_name = name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.as_str())
                .ok_or_else(|| miette::miette!("column has an empty type"))?;
            if crdb_name.eq_ignore_ascii_case("inet") {
                // `INET` is a real CRDB scalar type that `sqlparser` doesn't
                // model as a built-in.
                quote::quote!(Inet)
            } else {
                // Anything else is assumed to be a database enum; look up the
                // Diesel type for its CRDB type name.
                let diesel_name = crdb_to_diesel_enum_type_names
                    .get(crdb_name)
                    .copied()
                    .ok_or_else(|| {
                        miette::miette!(
                            "no Diesel enum type is known for the database type `{crdb_name}`"
                        )
                    })?;
                let path = syn::parse_str::<syn::Type>(diesel_name)
                    .into_diagnostic()?;
                quote::quote!(#path)
            }
        }
        other => {
            miette::bail!("unsupported column type `{other:?}`");
        }
    };
    Ok(tokens)
}

#[cfg(test)]
mod test {
    use super::*;
    use quote::ToTokens;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn fact_table(sql: &str) -> FactTable {
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .expect("SQL should parse")
            .into_iter()
            .next()
            .expect("one statement");
        let Statement::CreateTable(create_stmt) = stmt else {
            panic!("expected a CREATE TABLE statement");
        };
        FactTable {
            create_stmt,
            table_name: "example".to_string(),
            de_name: Arc::from("ExampleEngine"),
            fact_variant_name: "ExampleFact".to_string(),
        }
    }

    fn enum_map() -> HashMap<&'static str, &'static str> {
        HashMap::from([(
            "diagnosis_engine",
            "nexus_db_schema::enums::DiagnosisEngineEnum",
        )])
    }

    #[test]
    fn generates_table_macro() {
        let table = fact_table(
            "CREATE TABLE example (
                id UUID NOT NULL,
                sitrep_id UUID NOT NULL,
                case_id UUID NOT NULL,
                created_sitrep_id UUID NOT NULL,
                de diagnosis_engine NOT NULL,
                fact_field_1 INT8,
                fact_field_2 TEXT NOT NULL,
                fact_field_3 INET[],

                PRIMARY KEY (id, sitrep_id)
            )",
        );
        let generated = table
            .gen_diesel_schema(&enum_map())
            .expect("should generate")
            .to_token_stream()
            .to_string();
        println!("generated:\n{}", generated);

        assert!(generated.contains("table ! { example (id , sitrep_id)"));
        // `NOT NULL` columns map to the bare Diesel type...
        assert!(generated.contains("id -> Uuid"));
        assert!(generated.contains("fact_field_2 -> Text"));
        // ...while nullable columns are wrapped in `Nullable<>`.
        assert!(generated.contains("fact_field_1 -> Nullable < Int8 >"));
        // Database enums are resolved to their Diesel enum type path.
        assert!(
            generated.contains(
                "de -> nexus_db_schema :: enums :: DiagnosisEngineEnum"
            )
        );
        // `INET` is a scalar, and `INET[]` becomes a (nullable) array of it.
        assert!(
            generated.contains("fact_field_3 -> Nullable < Array < Inet > >")
        );
    }

    #[test]
    fn unknown_enum_type_is_an_error() {
        let table = fact_table(
            "CREATE TABLE example (
                id UUID NOT NULL,
                whatsit some_unknown_enum NOT NULL,
                PRIMARY KEY (id, sitrep_id)
            )",
        );
        assert!(table.gen_diesel_schema(&enum_map()).is_err());
    }
}
