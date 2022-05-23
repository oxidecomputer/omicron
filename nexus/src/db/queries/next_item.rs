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
/// Example query
/// -------------
///
/// Let's look at an example of the query implementation, used when selecting an
/// IP address for a guest network interface:
///
/// ```sql
/// SELECT
///     <first_address> + shift AS ip
/// FROM
///     generate_series(0, <max_shift>) AS shift
/// LEFT OUTER JOIN
///     network_interface
/// ON
///     (subnet_id, ip, time_deleted IS NULL) =
///     (<subnet_id>, <first_address> + shift, TRUE)
/// WHERE
///     ip IS NULL
/// LIMIT 1
/// ```
///
/// This query selects the lowest address in the IP subnet that's not already
/// allocated to a guest interface. Note that the query is linear in the number
/// of _allocated_ guest addresses. <first_address> and <max_shift> are chosen
/// based on the subnet and its size, and take into account reserved IP
/// addresses (such as the broadcast address).
///
/// General query structure
/// -----------------------
///
/// Much of the value of this type comes from the ability to specify the
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
/// There are several other potential issues. One is that, we may wish to skip
/// certain reserved items. Second, and slightly more subtle, is that the size
/// of the search space is dependent on the starting point. If we randomly
/// select a base that's pretty close to the end of the search space, then the
/// search would be over a much smaller space than if the base is at the start
/// of the total range.
///
/// In its most general form, the `NextItem` query accounts for both of these,
/// and looks like:
///
/// ```sql
/// SELECT
///     <base> + shift AS <item_column>
/// FROM
///     (
///         SELECT
///             shift
///         FROM
///             generate_series(0, <max_shift>)
///         AS shift
///         UNION ALL
///         SELECT
///             shift
///         FROM
///             generate_series(<min_shift>, -1)
///         AS shift
/// LEFT OUTER JOIN
///     <table>
/// ON
///     (<scope_column>, <item_column>, time_deleted IS NULL) =
///     (<scope_key>, <base> + shift, TRUE)
/// WHERE
///     <item_column> IS NULL
/// LIMIT 1
/// ```
///
/// Scan sizes
/// ----------
///
/// The scan size is limited by the sum of the `max_shift` and the `min_shift`.
/// These determine the shift to the right and left of the base item provided.
/// So if `min_shift` is zero, only base items above the base item are
/// considered. Visually, base, min, and max shift create a search space as
/// shown below. Note the order in which we search.
///
///
/// ```text
///  <- min_shift ----|      |--> max_shift ----->
/// [-----------------|<base>|--------------------]
///
/// n + 1, ..., n + k] [1,    2, 3, ..., n - 1, n, -+
/// ^                                                |
/// +------------------------------------------------+
/// ```
///
/// Shift generators
/// ----------------
///
/// This concept of selecting a base and max/min shift is encapsulated in the
/// [`ShiftGenerator`] trait. The simplest implementation,
/// [`DefaultShiftGenerator`], follows the diagram above, search from `base`, to
/// `base + max_shift`, to `base - min-shift`, to `base - 1`.
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
#[derive(Debug, Clone, Copy)]
pub(super) struct NextItem<
    Table,
    Item,
    ItemColumn,
    ScopeKey = NoScopeKey,
    ScopeColumn = NoScopeColumn,
    Generator = DefaultShiftGenerator<Item>,
> {
    table: Table,
    _d: PhantomData<(Item, ItemColumn, ScopeColumn)>,
    scope_key: ScopeKey,
    shift_generator: Generator,
}

impl<Table, Item, ItemColumn, ScopeKey, ScopeColumn, Generator>
    NextItem<Table, Item, ItemColumn, ScopeKey, ScopeColumn, Generator>
where
    // Table is a database table whose name can be used in a query fragment
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg> + Copy,

    // Item can be converted to the SQL type of the ItemColumn
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg> + Copy,

    // ItemColum is a column in the target table
    ItemColumn: Column<Table = Table> + Copy,

    // ScopeKey can be converted to the SQL type of the ScopeColumn
    ScopeKey: ScopeKeyType + ToSql<<ScopeColumn as Expression>::SqlType, Pg>,

    // ScopeColumn is a column on the target table
    ScopeColumn: ScopeColumnType + Column<Table = Table>,

    // The Postgres backend supports the SQL types of both columns
    Pg: HasSqlType<<ScopeColumn as Expression>::SqlType>
        + HasSqlType<<ItemColumn as Expression>::SqlType>,

    // We need an implementation to create the shifts from the base
    Generator: ShiftGenerator<Item>,
{
    /// Create a new `NextItem` query, scoped to a particular key.
    pub(super) fn new_scoped(
        shift_generator: Generator,
        scope_key: ScopeKey,
    ) -> Self {
        Self {
            table: Table::table(),
            _d: PhantomData,
            scope_key,
            shift_generator,
        }
    }
}

impl<Table, Item, ItemColumn, Generator>
    NextItem<Table, Item, ItemColumn, NoScopeKey, NoScopeColumn, Generator>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg> + Copy,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg> + Copy,
    ItemColumn: Column<Table = Table> + Copy,
    Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
    Generator: ShiftGenerator<Item>,
{
    /// Create a new `NextItem` query, with a global scope.
    pub(super) fn new_unscoped(shift_generator: Generator) -> Self {
        Self {
            table: Table::table(),
            _d: PhantomData,
            scope_key: NoScopeKey,
            shift_generator,
        }
    }
}

impl<Table, Item, ItemColumn, ScopeKey, ScopeColumn, Generator>
    QueryFragment<Pg>
    for NextItem<Table, Item, ItemColumn, ScopeKey, ScopeColumn, Generator>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg> + Copy,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg> + Copy,
    ItemColumn: Column<Table = Table> + Copy,
    ScopeKey: ToSql<<ScopeColumn as Expression>::SqlType, Pg>,
    ScopeColumn: Column<Table = Table>,
    Pg: HasSqlType<<ScopeColumn as Expression>::SqlType>
        + HasSqlType<<ItemColumn as Expression>::SqlType>,
    Generator: ShiftGenerator<Item>,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        push_next_item_select_clause::<Table, Item, ItemColumn, Generator>(
            &self.table,
            &self.shift_generator,
            out.reborrow(),
        )?;

        // This generates the JOIN conditions for the query, which look like:
        // ON
        //      (<scope_column>, <item_column>, time_deleted IS NULL) =
        //      (<scope_key>, <base> + shift, TRUE)
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
            self.shift_generator.base(),
        )?;
        out.push_sql(" + ");
        out.push_identifier("shift")?;
        out.push_sql(", TRUE) ");

        push_next_item_where_clause::<Table, ItemColumn>(out.reborrow())
    }
}

impl<Table, Item, ItemColumn, Generator> QueryFragment<Pg>
    for NextItem<Table, Item, ItemColumn, NoScopeKey, NoScopeColumn, Generator>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg> + Copy,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg> + Copy,
    ItemColumn: Column<Table = Table> + Copy,
    Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
    Generator: ShiftGenerator<Item>,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        push_next_item_select_clause::<Table, Item, ItemColumn, Generator>(
            &self.table,
            &self.shift_generator,
            out.reborrow(),
        )?;

        // This generates the JOIN conditions for the query, which look like:
        // ON
        //      (<item_column>, time_deleted IS NULL) =
        //      (<base> + shift, TRUE)
        //
        // Note that there is no scope here.
        out.push_sql(" ON (");
        out.push_identifier(ItemColumn::NAME)?;
        out.push_sql(", ");
        out.push_identifier("time_deleted")?;
        out.push_sql(" IS NULL) = (");
        out.push_bind_param::<<ItemColumn as Expression>::SqlType, Item>(
            self.shift_generator.base(),
        )?;
        out.push_sql(" + ");
        out.push_identifier("shift")?;
        out.push_sql(", TRUE) ");

        push_next_item_where_clause::<Table, ItemColumn>(out.reborrow())
    }
}

// Push the initial `SELECT` clause shared by the scoped and unscoped next item
// queries.
//
// ```sql
// SELECT
//      <base> + shift AS <item_column>
// FROM
//      <shift_generator.walk_ast()>
// LEFT OUTER JOIN
//      <table>
// ```
fn push_next_item_select_clause<'a, Table, Item, ItemColumn, Generator>(
    table: &'a Table,
    shift_generator: &'a Generator,
    mut out: AstPass<'_, 'a, Pg>,
) -> diesel::QueryResult<()>
where
    Table: diesel::Table + HasTable<Table = Table> + QueryFragment<Pg> + Copy,
    Item: ToSql<<ItemColumn as Expression>::SqlType, Pg> + Copy + 'a,
    ItemColumn: Column<Table = Table> + Copy,
    Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
    Generator: ShiftGenerator<Item>,
{
    out.push_sql("SELECT ");
    out.push_bind_param::<<ItemColumn as Expression>::SqlType, Item>(
        shift_generator.base(),
    )?;
    out.push_sql(" + ");
    out.push_identifier("shift")?;
    out.push_sql(" AS ");
    out.push_identifier(ItemColumn::NAME)?;
    out.push_sql(" FROM (");
    shift_generator.walk_ast::<Table, ItemColumn>(out.reborrow())?;
    out.push_sql(") LEFT OUTER JOIN ");
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

/// Trait for generating the shift from the base for a [`NextItem`] query.
pub trait ShiftGenerator<Item> {
    /// Return the base item for the scan.
    fn base(&self) -> &Item;

    /// Return the maximum shift from the base item for the scan.
    fn max_shift(&self) -> &i64;

    /// Return the minimum shift from the base item for the scan.
    fn min_shift(&self) -> &i64;

    /// Insert the part of the query represented by the shift of items into the
    /// provided AstPass.
    ///
    /// The default implementation pushes:
    ///
    /// ```sql
    /// SELECT generate_series(0, <max_shift>) AS shift
    /// UNION ALL
    /// SELECT generate_series(<min_shift>, -1) AS shift
    /// ```
    fn walk_ast<'a, Table, ItemColumn>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()>
    where
        Table:
            diesel::Table + HasTable<Table = Table> + QueryFragment<Pg> + Copy,
        Item: ToSql<<ItemColumn as Expression>::SqlType, Pg> + Copy,
        ItemColumn: Column<Table = Table> + Copy,
        Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
    {
        out.push_sql("SELECT generate_series(0, ");
        out.push_bind_param::<sql_types::BigInt, i64>(self.max_shift())?;
        out.push_sql(") AS ");
        out.push_identifier("shift")?;
        out.push_sql(" UNION ALL SELECT generate_series(");
        out.push_bind_param::<sql_types::BigInt, i64>(self.min_shift())?;
        out.push_sql(", -1) AS ");
        out.push_identifier("shift")
    }
}

/// The basic shift generator, that just delegates to the default trait
/// implementation.
#[derive(Debug, Clone, Copy)]
pub struct DefaultShiftGenerator<Item> {
    pub base: Item,
    pub max_shift: i64,
    pub min_shift: i64,
}

impl<Item> ShiftGenerator<Item> for DefaultShiftGenerator<Item> {
    fn base(&self) -> &Item {
        &self.base
    }

    fn max_shift(&self) -> &i64 {
        &self.max_shift
    }

    fn min_shift(&self) -> &i64 {
        &self.min_shift
    }
}
