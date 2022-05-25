// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A generic query for selecting a unique next item from a table.

use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::prelude::Column;
use diesel::prelude::Expression;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::serialize::ToSql;
use diesel::sql_types;
use diesel::sql_types::HasSqlType;
use std::marker::PhantomData;
use uuid::Uuid;

/// The `NextItem` query is used to generate common subqueries that select the
/// next available item from a database table.
///
/// There are a number of contexts in which we'd like to choose a value for a
/// particular model field, avoiding conflicts with existing records in a table.
/// A canonical example is selecting the next available IP address for a guest
/// network interface, from the VPC Subnet's IP subnet. See the other types in
/// this module for more examples.
///
/// Query structure
/// ---------------
///
/// In its most general form, this generates queries that look like:
///
/// ```sql
/// SELECT
///     <base> + offset AS <item_column>
/// FROM
///     generate_series(0, <max_offset>) AS offset
/// LEFT OUTER JOIN
///     <table>
/// ON
///     (<scope_column>, <item_column>, time_deleted IS NULL) =
///     (<scope_key>, <base> + offset, TRUE)
/// WHERE
///     <item_column> IS NULL
/// LIMIT 1
/// ```
///
/// In the example of a guest IP address, this would be instantiated as:
///
/// ```sql
/// SELECT
///     <first_address> + offset AS ip
/// FROM
///     generate_series(0, <max_offset>) AS offset
/// LEFT OUTER JOIN
///     network_interfacd
/// ON
///     (subnet_id, ip, time_deleted IS NULL) =
///     (<subnet_id>, <first_address> + offset, TRUE)
/// WHERE
///     ip IS NULL
/// LIMIT 1
/// ```
///
/// This query selects the lowest address in the IP subnet that's not already
/// allocated to a guest interface. Note that the query is linear in the number
/// of _allocated_ guest addresses. <first_address> and <max_offset> are chosen
/// based on the subnet and its size, and take into account reserved IP
/// addresses (such as the broadcast address).
///
/// Scanning for an item
/// --------------------
///
/// Much of the value of this query comes from the ability to specify the
/// starting point for a scan for the next item. In the case above, of an IP
/// address for a guest NIC, we always try to allocate the lowest available
/// address. This implies the search is linear in the number of allocated IP
/// addresses, but that runtime cost is acceptable for a few reasons. First, the
/// predictability of the addresses is nice. Second, the subnets can generally
/// be quite small, meaning both the actual runtime can be quite low, and it's
/// not clear that a more complex strategy may perform much better.
///
/// However, consumers can _choose_ the base from which to start the scan. This
/// allows consumers to follow a strategy similar to linear probing in
/// hashtables: pick a random item in the search space, and then take the first
/// available starting from there. This has the benefit of avoiding a retry-loop
/// in the application in the case that a randomly-chosen item conflicts with an
/// existing item. That "retry loop" is converted into a sequential scan in the
/// database for an item that does not conflict.
///
/// Scopes
/// ------
///
/// When we choose a next available item, we usually only want to avoid
/// conflicts within some scope. For example, consider MAC addresses for guest
/// network interfaces. These must be unique within a single VPC, but are
/// allowed to be duplicated in different VPCs. In this case, the scope is the
/// VPC.
///
/// The query can be constructed with two methods: [`NextItem::new_scoped`] or
/// [`NextItem::new_unscoped`]. In the former case, the method accepts a scope
/// key, and the column in the table where we can find that key. In the latter,
/// there is no scope, which means the items must be globally unique in the
/// entire table (among non-deleted items). The query is structured slightly
/// differently in these two cases.
///
/// Scan sizes
/// ----------
///
/// The scan size is limited by the `max_offset` field. The maximum value
/// depends heavily on the context. For a guest IP address, the subnets can be
/// small (though they don't need to be). More importantly, we definitely don't
/// want to fail a request if there _is_ an available address, even if that's
/// the very last address in a subnet.
///
/// For that reason, the
/// [`NextGuestIpv4Address`](`crate::db::queries::network_interface::NextGuestIpv4Address`)
/// query uses the full subnet as its maximum scan size. For other contexts, the
/// size may be much smaller.
#[derive(Debug, Clone, Copy)]
pub(super) struct NextItem<
    Table,
    Item,
    ItemColumn,
    ScopeKey = NoScopeKey,
    ScopeColumn = NoScopeColumn,
> {
    table: Table,
    _columns: PhantomData<(ItemColumn, ScopeColumn)>,
    scope_key: ScopeKey,
    base: Item,
    max_offset: i64,
}

impl<Table, Item, ItemColumn, ScopeKey, ScopeColumn>
    NextItem<Table, Item, ItemColumn, ScopeKey, ScopeColumn>
where
    // Table is a database table whose name can be used in a query fragment
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg>,

    // Item can be converted to the SQL type of the ItemColumn
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg>,

    // ItemColum is a column in the target table
    ItemColumn: Column<Table = Table>,

    // ScopeKey can be converted to the SQL type of the ScopeColumn
    ScopeKey: ScopeKeyType + ToSql<<ScopeColumn as Expression>::SqlType, Pg>,

    // ScopeColumn is a column on the target table
    ScopeColumn: ScopeColumnType + Column<Table = Table>,

    // The Postgres backend supports the SQL types of both columns
    Pg: HasSqlType<<ScopeColumn as Expression>::SqlType>
        + HasSqlType<<ItemColumn as Expression>::SqlType>,
{
    /// Create a new `NextItem` query, scoped to a particular key.
    pub(super) fn new_scoped(
        item: Item,
        scope_key: ScopeKey,
        max_offset: u32,
    ) -> Self {
        Self {
            table: Table::table(),
            _columns: PhantomData,
            scope_key,
            base: item,
            max_offset: i64::from(max_offset),
        }
    }
}

impl<Table, Item, ItemColumn>
    NextItem<Table, Item, ItemColumn, NoScopeKey, NoScopeColumn>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg>,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg>,
    ItemColumn: Column<Table = Table>,
    Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
{
    /// Create a new `NextItem` query, with a global scope.
    // This will be used in future to implement queries like creating VNIs or
    // VPC Subnet IPv6 prefixes.
    #[allow(dead_code)]
    pub(super) fn new_unscoped(item: Item, max_offset: u32) -> Self {
        Self {
            table: Table::table(),
            _columns: PhantomData,
            scope_key: NoScopeKey,
            base: item,
            max_offset: i64::from(max_offset),
        }
    }
}

impl<Table, Item, ItemColumn, ScopeKey, ScopeColumn> QueryFragment<Pg>
    for NextItem<Table, Item, ItemColumn, ScopeKey, ScopeColumn>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg>,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg>,
    ItemColumn: Column<Table = Table>,
    ScopeKey: ToSql<<ScopeColumn as Expression>::SqlType, Pg>,
    ScopeColumn: Column<Table = Table>,
    Pg: HasSqlType<<ScopeColumn as Expression>::SqlType>
        + HasSqlType<<ItemColumn as Expression>::SqlType>,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        push_next_item_select_clause::<Table, Item, ItemColumn>(
            &self.base,
            &self.table,
            &self.max_offset,
            out.reborrow(),
        )?;

        // This generates the JOIN conditions for the query, which look like:
        // ON
        //      (<scope_column>, <item_column>, time_deleted IS NULL) =
        //      (<scope_key>, <base> + offset, TRUE)
        out.push_sql(" ON (");
        out.push_identifier(ScopeColumn::NAME)?;
        out.push_sql(", ");
        out.push_identifier(ItemColumn::NAME)?;
        out.push_sql(", ");
        out.push_identifier("time_deleted")?;
        out.push_sql(" IS NULL) = (");
        out.push_bind_param::<<ScopeColumn as Expression>::SqlType, ScopeKey>(
            &self.scope_key,
        )?;
        out.push_sql(", ");
        out.push_bind_param::<<ItemColumn as Expression>::SqlType, Item>(
            &self.base,
        )?;
        out.push_sql(" + ");
        out.push_identifier("offset")?;
        out.push_sql(", TRUE) ");

        push_next_item_where_clause::<Table, ItemColumn>(out.reborrow())
    }
}

impl<Table, Item, ItemColumn> QueryFragment<Pg>
    for NextItem<Table, Item, ItemColumn, NoScopeKey, NoScopeColumn>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg>,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg>,
    ItemColumn: Column<Table = Table>,
    Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        push_next_item_select_clause::<Table, Item, ItemColumn>(
            &self.base,
            &self.table,
            &self.max_offset,
            out.reborrow(),
        )?;

        // This generates the JOIN conditions for the query, which look like:
        // ON
        //      (<item_column>, time_deleted IS NULL) =
        //      (<base> + offset, TRUE)
        //
        // Note that there is no scope here.
        out.push_sql(" ON (");
        out.push_identifier(ItemColumn::NAME)?;
        out.push_sql(", ");
        out.push_identifier("time_deleted")?;
        out.push_sql(" IS NULL) = (");
        out.push_bind_param::<<ItemColumn as Expression>::SqlType, Item>(
            &self.base,
        )?;
        out.push_sql(" + ");
        out.push_identifier("offset")?;
        out.push_sql(", TRUE) ");

        push_next_item_where_clause::<Table, ItemColumn>(out.reborrow())
    }
}

// Push the initial `SELECT` clause shared by the scoped and unscoped next item
// queries.
//
// ```sql
// SELECT
//      <base> + offset AS <item_column>
// FROM
//      generate_series(0, <max_offset>) AS offset
//  LEFT OUTER JOIN
//      <table>
// ```
fn push_next_item_select_clause<'a, Table, Item, ItemColumn>(
    base: &'a Item,
    table: &'a Table,
    max_offset: &'a i64,
    mut out: AstPass<'_, 'a, Pg>,
) -> diesel::QueryResult<()>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg>,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg>,
    ItemColumn: Column<Table = Table>,
    Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
{
    out.push_sql("SELECT ");
    out.push_bind_param::<<ItemColumn as Expression>::SqlType, Item>(base)?;
    out.push_sql(" + ");
    out.push_identifier("offset")?;
    out.push_sql(" AS ");
    out.push_identifier(ItemColumn::NAME)?;
    out.push_sql(" FROM generate_series(0, ");
    out.push_bind_param::<sql_types::BigInt, i64>(max_offset)?;
    out.push_sql(") AS ");
    out.push_identifier("offset")?;
    out.push_sql(" LEFT OUTER JOIN ");
    table.walk_ast(out.reborrow())
}

// Push the final where clause shared by scoped and unscoped next item queries.
//
// ```sql
// WHERE <item_column> IS NULL LIMIT 1
// ```
fn push_next_item_where_clause<Table, ItemColumn>(
    mut out: AstPass<Pg>,
) -> diesel::QueryResult<()>
where
    Table: diesel::Table
        + diesel::associations::HasTable<Table = Table>
        + QueryFragment<Pg>,
    ItemColumn: Column<Table = Table>,
{
    out.push_sql(" WHERE ");
    out.push_identifier(ItemColumn::NAME)?;
    out.push_sql(" IS NULL LIMIT 1");
    Ok(())
}

/// A macro used to delegate the implementation of [`QueryFragment`] to a field
/// of `self` called `inner`.
macro_rules! delegate_query_fragment_impl {
    ($parent:ty) => {
        impl QueryFragment<Pg> for $parent {
            fn walk_ast<'a>(
                &'a self,
                out: AstPass<'_, 'a, Pg>,
            ) -> diesel::QueryResult<()> {
                self.inner.walk_ast(out)
            }
        }
    };
}

/// A marker trait used to identify types that can be used as scope keys.
pub(crate) trait ScopeKeyType: Sized + std::fmt::Debug {}
impl ScopeKeyType for Uuid {}

/// A type indicating that a [`NextItem`] query has no scope key.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NoScopeKey;
impl ScopeKeyType for NoScopeKey {}

/// A marker trait used to identify types that identify the table column for a
/// scope key.
pub(crate) trait ScopeColumnType: Sized + std::fmt::Debug {}
impl<T> ScopeColumnType for T where T: Column + std::fmt::Debug {}

/// A type indicating that a [`NextItem`] query has no scope column.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NoScopeColumn;
impl ScopeColumnType for NoScopeColumn {}
