// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CockroachDB schema for Nexus.

pub mod enums;
pub mod schema;

#[cfg(test)]
mod crdb_alignment_test;

/// Metadata about a Diesel table, auto-registered by the `table!` macro
/// in [`schema`].
#[cfg(test)]
pub struct DieselTableInfo {
    pub name: &'static str,
    /// Returns (column_name, type_name) pairs via `std::any::type_name`.
    pub columns: fn() -> Vec<(&'static str, &'static str)>,
}

#[cfg(test)]
#[linkme::distributed_slice]
pub static DIESEL_TABLES: [DieselTableInfo];

/// Internal macro used by our `table!` shadow to register table metadata.
///
/// In test builds, this registers each table into the [`DIESEL_TABLES`]
/// distributed slice so that integration tests can enumerate all Diesel
/// tables. In non-test builds, this is a no-op.
#[macro_export]
macro_rules! __register_table {
    ($table_name:ident; $($col_name:ident),*) => {
        #[cfg(test)]
        const _: () = {
            #[linkme::distributed_slice($crate::DIESEL_TABLES)]
            static TABLE_INFO: $crate::DieselTableInfo = $crate::DieselTableInfo {
                name: stringify!($table_name),
                columns: {
                    fn cols() -> Vec<(&'static str, &'static str)> {
                        use diesel::Column;
                        use diesel::Expression;
                        vec![
                            $((
                                <$table_name::$col_name as Column>::NAME,
                                std::any::type_name::<
                                    <$table_name::$col_name as Expression>::SqlType
                                >(),
                            ),)*
                        ]
                    }
                    cols
                },
            };
        };
    };
}
