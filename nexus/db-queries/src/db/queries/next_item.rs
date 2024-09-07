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
///     generate_series(0, <max_shift>) AS shift,
/// LEFT OUTER JOIN
///     network_interface
/// ON
///     (subnet_id, ip, time_deleted IS NULL) =
///     (<subnet_id>, <first_address> + shift, TRUE)
/// WHERE
///     ip IS NULL
/// ORDER BY ip
/// LIMIT 1
/// ```
///
/// This query selects the next address after <first_address> in the IP subnet
/// that's not already allocated to a guest interface. Note that the query is
/// linear in the number of _allocated_ guest addresses. <first_address> and
/// <max_shift> are chosen based on the subnet and its size, and take into
/// account reserved IP addresses (such as the broadcast address).
///
/// General query structure
/// -----------------------
///
/// Much of the value of this type comes from the ability to specify the
/// starting point for a scan for the next item. In the case above, of an IP
/// address for a guest NIC, we always try to allocate the next available
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
/// In its most general form, the `NextItem` query accounts for the latter of
/// these, and looks like:
///
/// ```sql
/// SELECT
///     <base> + shift AS <item_column>
/// FROM
///     (
///         SELECT
///             "index", shift
///         FROM
///             generate_series(0, <n>) AS "index"
///             generate_series(0, <max_shift>) AS shift,
///         UNION ALL
///         SELECT
///             shift
///         FROM
///             generate_series(<n + 1>, <range_size>) AS "index"
///             generate_series(<min_shift>, -1) AS shift,
/// LEFT OUTER JOIN
///     <table>
/// ON
///     (<scope_column>, <item_column>, time_deleted IS NULL) =
///     (<scope_key>, <base> + shift, TRUE)
/// WHERE
///     <item_column> IS NULL
/// ORDER BY "index"
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
/// Ordering
/// --------
///
/// The subquery is designed to select the next address _after_ the provided
/// base. To preserve this behavior even in situations where we wrap around the
/// end of the range, we include the `index` column when generating the shifts,
/// and order the result by that column.
///
/// The CockroachDB docs on [ordering] specify that queries without an explicit
/// `ORDER BY` clause are returned "as the coordinating nodes receives them."
/// Without this clause, the order is non-deterministic.
///
/// Shift generators
/// ----------------
///
/// This concept of selecting a base and max/min shift is encapsulated in the
/// [`ShiftGenerator`] trait. The simplest implementation,
/// [`DefaultShiftGenerator`], follows the diagram above, search from `base`, to
/// `base + max_shift`, to `base - min-shift`, to `base - 1`. Other
/// `ShiftGenerator` implementations could provide more complex behavior, such
/// as a deny-list of precluded items, a non-contiguous search space, or other
/// behavior.
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
/// [ordering]: https://www.cockroachlabs.com/docs/stable/select-clause#sorting-and-limiting-query-results
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

const SHIFT_COLUMN_IDENT: &str = "shift";
const INDEX_COLUMN_IDENT: &str = "index";

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
        out.push_identifier(SHIFT_COLUMN_IDENT)?;
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
        out.push_identifier(SHIFT_COLUMN_IDENT)?;
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
    out.push_identifier(SHIFT_COLUMN_IDENT)?;
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
// WHERE <item_column> IS NULL ORDER BY "index" LIMIT 1
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
    out.push_sql(" IS NULL ORDER BY ");
    out.push_identifier(INDEX_COLUMN_IDENT)?;
    out.push_sql(" LIMIT 1");
    Ok(())
}

/// A macro used to delegate the implementation of [`QueryFragment`] to a field
/// of `self` called `inner`.
macro_rules! delegate_query_fragment_impl {
    ($parent:ty) => {
        impl ::diesel::query_builder::QueryFragment<::diesel::pg::Pg>
            for $parent
        {
            fn walk_ast<'a>(
                &'a self,
                out: ::diesel::query_builder::AstPass<'_, 'a, ::diesel::pg::Pg>,
            ) -> ::diesel::QueryResult<()> {
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

    /// Return the indices of the generated shifts for the scan.
    fn shift_indices(&self) -> &ShiftIndices;

    /// Insert the part of the query represented by the shift of items into the
    /// provided AstPass.
    ///
    /// The default implementation pushes:
    ///
    /// ```sql
    /// SELECT
    ///     generate_series(0, <n>) as "index"
    ///     generate_series(0, <max_shift>) AS shift,
    /// UNION ALL
    /// SELECT
    ///     generate_series(<n+1>, <range_size>) as "index"
    ///     generate_series(<min_shift>, -1) AS shift,
    /// ```
    fn walk_ast<'a, 'b, Table, ItemColumn>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()>
    where
        Table:
            diesel::Table + HasTable<Table = Table> + QueryFragment<Pg> + Copy,
        Item: ToSql<<ItemColumn as Expression>::SqlType, Pg> + Copy,
        ItemColumn: Column<Table = Table> + Copy,
        Pg: HasSqlType<<ItemColumn as Expression>::SqlType>,
        'a: 'b,
    {
        out.push_sql("SELECT generate_series(0, ");
        out.push_bind_param::<sql_types::BigInt, i64>(
            self.shift_indices().first_end(),
        )?;
        out.push_sql(") AS ");
        out.push_identifier(INDEX_COLUMN_IDENT)?;
        out.push_sql(", generate_series(0, ");
        out.push_bind_param::<sql_types::BigInt, i64>(self.max_shift())?;
        out.push_sql(") AS ");
        out.push_identifier(SHIFT_COLUMN_IDENT)?;
        out.push_sql(" UNION ALL SELECT generate_series(");
        out.push_bind_param::<sql_types::BigInt, i64>(
            self.shift_indices().second_start(),
        )?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::BigInt, i64>(
            self.shift_indices().second_end(),
        )?;
        out.push_sql(") AS ");
        out.push_identifier(INDEX_COLUMN_IDENT)?;
        out.push_sql(", generate_series(");
        out.push_bind_param::<sql_types::BigInt, i64>(self.min_shift())?;
        out.push_sql(", -1) AS ");
        out.push_identifier(SHIFT_COLUMN_IDENT)
    }
}

/// Helper to compute the range of the _index_ column, used to predictably sort
/// the generated items.
///
/// This type cannot be created directly, it's generated internally by creating
/// a `DefaultShiftGenerator`.
// NOTE: This type mostly exists to satisfy annoying lifetime constraints
// imposed by Diesel's `AstPass`. One can only push bind parameters that outlive
// the AST pass itself, so you cannot push owned values or even references to
// values generated anywhere within the `walk_ast()` implementation;
#[derive(Copy, Clone, Debug)]
pub struct ShiftIndices {
    // The end of the first range.
    first_end: i64,
    // The start of the second range.
    second_start: i64,
    // The end of the second range.
    //
    // This is equal to the number of items generated for the range.
    second_end: i64,
}

impl ShiftIndices {
    fn new(max_shift: i64, min_shift: i64) -> Self {
        assert!(max_shift >= 0);
        assert!(min_shift <= 0);

        // We're just generating the list of indices (0, n_items), but we need
        // to split it in the middle. Specifically, we'll split it at
        // `max_shift`, and then generate the remainder.
        let first_end = max_shift;
        let second_start = first_end + 1;
        let second_end = max_shift - min_shift;
        Self { first_end, second_start, second_end }
    }

    /// Return the end of the first set of indices.
    pub fn first_end(&self) -> &i64 {
        &self.first_end
    }

    /// Return the start of the second set of indices.
    pub fn second_start(&self) -> &i64 {
        &self.second_start
    }

    /// Return the end of the second set of indices.
    pub fn second_end(&self) -> &i64 {
        &self.second_end
    }
}

/// The basic shift generator, that just delegates to the default trait
/// implementation.
#[derive(Debug, Clone, Copy)]
pub struct DefaultShiftGenerator<Item> {
    base: Item,
    max_shift: i64,
    min_shift: i64,
    shift_indices: ShiftIndices,
}

impl<Item> DefaultShiftGenerator<Item> {
    /// Create a default generator, checking the provided ranges.
    ///
    /// Returns `None` if either the max_shift is less than 0, or the min_shift
    /// is greater than 0.
    pub fn new(base: Item, max_shift: i64, min_shift: i64) -> Option<Self> {
        if max_shift < 0 || min_shift > 0 {
            return None;
        }
        let shift_indices = ShiftIndices::new(max_shift, min_shift);
        Some(Self { base, max_shift, min_shift, shift_indices })
    }
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

    fn shift_indices(&self) -> &ShiftIndices {
        &self.shift_indices
    }
}

#[cfg(test)]
mod tests {

    use super::DefaultShiftGenerator;
    use super::NextItem;
    use super::ShiftIndices;
    use crate::db;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use chrono::DateTime;
    use chrono::Utc;
    use diesel::pg::Pg;
    use diesel::query_builder::AstPass;
    use diesel::query_builder::QueryFragment;
    use diesel::query_builder::QueryId;
    use diesel::Column;
    use diesel::Insertable;
    use diesel::SelectableHelper;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::sync::Arc;
    use uuid::Uuid;

    table! {
        test_schema.item (id) {
            id -> Uuid,
            value -> Int4,
            time_deleted -> Nullable<Timestamptz>,
        }
    }

    async fn setup_test_schema(pool: &db::Pool) {
        let connection = pool.claim().await.unwrap();
        (*connection)
            .batch_execute_async(
                "CREATE SCHEMA IF NOT EXISTS test_schema; \
                CREATE TABLE IF NOT EXISTS test_schema.item ( \
                    id UUID PRIMARY KEY, \
                    value INT4 NOT NULL, \
                    time_deleted TIMESTAMPTZ \
                ); \
                TRUNCATE test_schema.item; \
                CREATE UNIQUE INDEX ON test_schema.item (value) WHERE time_deleted IS NULL; \
            ")
            .await
            .unwrap()
    }

    // Describes an item to be allocated with a NextItem query
    #[derive(Queryable, Debug, Insertable, Selectable, Clone)]
    #[diesel(table_name = item)]
    struct Item {
        id: Uuid,
        value: i32,
        time_deleted: Option<DateTime<Utc>>,
    }

    #[derive(Debug, Clone, Copy)]
    struct NextItemQuery {
        inner: NextItem<item::dsl::item, i32, item::dsl::value>,
    }

    // These implementations are needed to actually allow inserting the results
    // of the `NextItemQuery` itself.
    impl NextItemQuery {
        fn new(generator: DefaultShiftGenerator<i32>) -> Self {
            Self { inner: NextItem::new_unscoped(generator) }
        }
    }

    delegate_query_fragment_impl!(NextItemQuery);

    impl QueryId for NextItemQuery {
        type QueryId = ();
        const HAS_STATIC_QUERY_ID: bool = false;
    }

    impl Insertable<item::table> for NextItemQuery {
        type Values = NextItemQueryValues;
        fn values(self) -> Self::Values {
            NextItemQueryValues(self)
        }
    }

    #[derive(Debug, Clone)]
    struct NextItemQueryValues(NextItemQuery);

    impl QueryId for NextItemQueryValues {
        type QueryId = ();
        const HAS_STATIC_QUERY_ID: bool = false;
    }

    impl diesel::insertable::CanInsertInSingleQuery<Pg> for NextItemQueryValues {
        fn rows_to_insert(&self) -> Option<usize> {
            Some(1)
        }
    }

    impl QueryFragment<Pg> for NextItemQueryValues {
        fn walk_ast<'a>(
            &'a self,
            mut out: AstPass<'_, 'a, Pg>,
        ) -> diesel::QueryResult<()> {
            out.push_sql("(");
            out.push_identifier(item::dsl::id::NAME)?;
            out.push_sql(", ");
            out.push_identifier(item::dsl::value::NAME)?;
            out.push_sql(", ");
            out.push_identifier(item::dsl::time_deleted::NAME)?;
            out.push_sql(") VALUES (gen_random_uuid(), (");
            self.0.walk_ast(out.reborrow())?;
            out.push_sql("), NULL)");
            Ok(())
        }
    }

    // Test that we correctly insert the next available item
    #[tokio::test]
    async fn test_wrapping_next_item_query() {
        // Setup the test database
        let logctx = dev::test_setup_log("test_wrapping_next_item_query");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool =
            Arc::new(crate::db::Pool::new_single_host(&logctx.log, &cfg));
        let conn = pool.claim().await.unwrap();

        // We're going to operate on a separate table, for simplicity.
        setup_test_schema(&pool).await;

        // We'll first insert an item at 0.
        //
        // This generator should start at 0, and then select over the range [0,
        // 10], wrapping back to 0.
        let generator = DefaultShiftGenerator::new(0, 10, 0).unwrap();
        let query = NextItemQuery::new(generator);
        let it = diesel::insert_into(item::dsl::item)
            .values(query)
            .returning(Item::as_returning())
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(it.value, 0);

        // Insert the same query again, which should give us 1 now.
        let it = diesel::insert_into(item::dsl::item)
            .values(query)
            .returning(Item::as_returning())
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(it.value, 1);

        // Insert 10, and guarantee that we get it back.
        let generator = DefaultShiftGenerator::new(10, 0, -10).unwrap();
        let query = NextItemQuery::new(generator);
        let it = diesel::insert_into(item::dsl::item)
            .values(query)
            .returning(Item::as_returning())
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(it.value, 10);

        // Now, insert the same query again. Since 0, 1, and 10 are all
        // allocated, we should wrap around and insert 2.
        let it = diesel::insert_into(item::dsl::item)
            .values(query)
            .returning(Item::as_returning())
            .get_result_async(&*conn)
            .await
            .unwrap();
        assert_eq!(it.value, 2);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_next_item_query_is_ordered_by_indices() {
        // Setup the test database
        let logctx =
            dev::test_setup_log("test_next_item_query_is_ordered_by_indices");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool =
            Arc::new(crate::db::Pool::new_single_host(&logctx.log, &cfg));
        let conn = pool.claim().await.unwrap();

        // We're going to operate on a separate table, for simplicity.
        setup_test_schema(&pool).await;

        // To test ordering behavior, we'll generate a range where the natural
        // order of the _items_ differs from their indices. I.e., we have some
        // non-zero base. We'll make sure we order everything by those indices,
        // not the items themselves.
        const MIN_SHIFT: i64 = -5;
        const MAX_SHIFT: i64 = 5;
        const BASE: i32 = 5;
        let generator =
            DefaultShiftGenerator::new(BASE, MAX_SHIFT, MIN_SHIFT).unwrap();
        let query = NextItemQuery::new(generator);

        // Insert all items until there are none left.
        let first_range = i64::from(BASE)..=i64::from(BASE) + MAX_SHIFT;
        let second_range = i64::from(BASE) + MIN_SHIFT..i64::from(BASE);
        let mut expected = first_range.chain(second_range);
        while let Some(expected_value) = expected.next() {
            let it = diesel::insert_into(item::dsl::item)
                .values(query)
                .returning(Item::as_returning())
                .get_result_async(&*conn)
                .await
                .unwrap();
            assert_eq!(i64::from(it.value), expected_value);
        }
        assert!(
            expected.next().is_none(),
            "Should have exhausted the expected values"
        );
        diesel::insert_into(item::dsl::item)
            .values(query)
            .returning(Item::as_returning())
            .get_result_async(&*conn)
            .await
            .expect_err(
                "The next item query should not have further items to generate",
            );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[test]
    fn test_shift_indices() {
        // In this case, we're generating a list of 11 items, all sequential. So
        // we want the first set of arguments to `generate_series()` to be 0,
        // 10, and the second set 11, 10. That means the first indices will be
        // 0..=10 and the second 11..=10, i.e., empty.
        let min_shift = 0;
        let max_shift = 10;
        let indices = ShiftIndices::new(max_shift, min_shift);
        assert_eq!(indices.first_end, 10);
        assert_eq!(indices.second_start, 11);
        assert_eq!(indices.second_end, 10);

        // Here, the list is split in half. We want to still result in a
        // sequence 0..=10, split at 5. So the arguments to generate_series()
        // should be (0, 5), and (6, 10).
        let min_shift = -5;
        let max_shift = 5;
        let indices = ShiftIndices::new(max_shift, min_shift);
        assert_eq!(indices.first_end, 5);
        assert_eq!(indices.second_start, 6);
        assert_eq!(indices.second_end, 10);

        // This case tests where most the range is _before_ the base, i.e., the
        // max shift is zero. Note that this technically still means we have one
        // item in the list of available, which is the base itself (at a shift
        // of 0).
        let min_shift = -10;
        let max_shift = 0;
        let indices = ShiftIndices::new(max_shift, min_shift);
        assert_eq!(indices.first_end, 0);
        assert_eq!(indices.second_start, 1);
        assert_eq!(indices.second_end, 10);
    }
}
