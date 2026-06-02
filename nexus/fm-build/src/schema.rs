// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use iddqd::IdOrdMap;
use sqlparser::ast::CreateTable;
use std::collections::BTreeMap;
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
    // Rust ident for the diagnosis engine enum variant
    pub(crate) de_name: Arc<str>,
    // Rust ident for the fact variant enum variant
    pub(crate) fact_variant_name: String,
}

impl iddqd::IdOrdItem for FactTable {
    type Key<'a> = &'a sqlparser::ast::ObjectName;

    fn key(&self) -> Self::Key<'_> {
        &self.create_stmt.name
    }

    iddqd::id_upcast!();
}
