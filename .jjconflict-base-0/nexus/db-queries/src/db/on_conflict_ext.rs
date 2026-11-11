// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::dsl::sql;
use diesel::expression::SqlLiteral;
use diesel::prelude::*;
use diesel::sql_types::Bool;
use diesel::upsert::IncompleteOnConflict;

/// An extension trait for incomplete `INSERT INTO ... ON CONFLICT`
/// expressions.
///
/// In Diesel, these are represented by [`IncompleteOnConflict`]. This trait is
/// implemented for expressions of that type.
pub trait IncompleteOnConflictExt {
    /// The output of [`IncompleteOnConflictExt::as_partial_index`].
    type AsPartialIndexOutput;

    /// Adds a `WHERE false` clause to the `ON CONFLICT` expression, which
    /// informs the database that the set of columns is a partial index.
    ///
    /// This is a replacement for [`DecoratableTarget::filter_target`] which is
    /// easier to use, and significantly harder to misuse. In Omicron, we have
    /// implemented a general ban on `filter_target` because it's so easy to
    /// silently introduce bugs with it.
    ///
    /// # Background
    ///
    /// ## 1. Partial indexes
    ///
    /// Most users of databases are aware of *unique indexes*: a set of columns
    /// that uniquely identifies a row in a given table. For example, the
    /// primary key of a table is always a unique index.
    ///
    /// Databases like CockroachDB also support *[partial indexes]*: a unique
    /// index formed only on the set of columns that meet a given condition. In
    /// Omicron, we have several partial indexes of the form (as described in
    /// `dbinit.sql`):
    ///
    /// ```sql
    /// CREATE UNIQUE INDEX IF NOT EXISTS lookup_silo_group_by_silo ON omicron.public.silo_group (
    ///     silo_id,
    ///     external_id
    /// ) WHERE
    ///     time_deleted IS NULL;
    /// ```
    ///
    /// This index means that in the `omicron.public.silo_group` table, each
    /// pair of `silo_id` and `external_id` must be unique. However, the
    /// uniqueness is only enforced for rows where `time_deleted` is `NULL`.
    /// (Logically, this makes sense: rows that have been soft-deleted should
    /// not have an influence on uniqueness constraints.)
    ///
    /// ## 2. `INSERT INTO ... ON CONFLICT`
    ///
    /// In general, inserting a new row into an SQL table looks like:
    ///
    /// ```sql
    /// INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...);
    /// ```
    ///
    /// By default, if you try to insert a row that would violate a unique
    /// constraint, the database will raise an error. However, you can
    /// customize this behavior with [the `ON CONFLICT` clause].
    ///
    /// For example, for a table with five columns `pkey` (primary key),
    /// `external_id` (a UUID), `data`, `time_created`, and `time_deleted`, you
    /// can write:
    ///
    /// ```sql
    /// INSERT INTO my_table (pkey, external_id, data, time_created, time_deleted)
    ///     VALUES (1, '70b10c38-36aa-4be0-8ec5-e7923438b3b8', 'foo', now(), NULL)
    ///     ON CONFLICT (pkey)
    ///     DO UPDATE SET data = 'foo';
    /// ```
    ///
    /// In this case:
    ///
    /// * If the row doesn't exist, a new row is inserted with the data, and
    ///   `time_created` set to the current time.
    /// * If the row does exist, the `data` column is updated to `'foo'` but
    ///   `time_created` is not changed.
    ///
    /// ## 3. Partial indexes and `ON CONFLICT`
    ///
    /// Now consider what happens if there's a partial index on `my_table`,
    /// ensuring that `external_id` is unique if the row hasn't been
    /// soft-deleted:
    ///
    /// ```sql
    /// CREATE UNIQUE INDEX IF NOT EXISTS my_table_external_id
    ///     ON my_table (external_id)
    ///     WHERE time_deleted IS NULL;
    /// ```
    ///
    /// You can now try to write a query which takes into account the conflict
    /// on `external_id`:
    ///
    /// ```sql
    /// INSERT INTO my_table (pkey, external_id, data, time_created, time_deleted)
    ///     VALUES (1, '70b10c38-36aa-4be0-8ec5-e7923438b3b8', 'foo', now(), NULL)
    ///     ON CONFLICT (external_id)
    ///     DO UPDATE SET data = 'foo';
    /// ```
    ///
    /// But this query does not work, and Cockroach errors out with:
    ///
    /// ```text
    /// ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification
    /// ```
    ///
    /// The reason for that is that simply specifying the columns is not
    /// enough. Because the index is _partial_, Cockroach requires that the
    /// `WHERE` clause of the index be specified as well.
    ///
    /// ```sql
    /// INSERT INTO my_table (pkey, external_id, data, time_created, time_deleted)
    ///     VALUES (1, '70b10c38-36aa-4be0-8ec5-e7923438b3b8', 'foo', now(), NULL)
    ///     ON CONFLICT (external_id)
    ///     WHERE time_deleted IS NULL
    ///     DO UPDATE SET data = 'foo';
    /// ```
    ///
    /// Importantly, the `WHERE` clause of the index doesn't have to exactly
    /// _match_ the condition on the partial index; it can be anything that
    /// _logically implies_ the condition on the partial index. With something
    /// like `time_deleted IS NULL` the value of that is not exactly clear, but
    /// you can imagine a partial index on something like `col >= 10`, and
    /// write `ON CONFLICT (...) WHERE col >= 20`. This is allowed because
    /// `col >= 20` implies `col >= 10`. (But `WHERE col >= 5` is not allowed.)
    ///
    /// ## 4. A similar syntax with a different meaning
    ///
    /// There's a similar syntax which does something completely different: `ON
    /// CONFLICT ... DO UPDATE ... WHERE`. This syntax does a conditional
    /// update, whether or not the conflict is in a partial index. For example,
    /// `ON CONFLICT (pkey) DO UPDATE SET data = 'foo' WHERE data IS NULL` will
    /// update `data` to `'foo'` if the row already exists, but only if `data`
    /// is `NULL`.
    ///
    /// Now, what happens if you accidentally mix the two up, and write
    /// something like: `ON CONFLICT (pkey) WHERE data IS NULL DO UPDATE SET
    /// data = 'foo'`? You might imagine that this query is rejected, for two
    /// reasons:
    ///
    /// 1. `pkey` is a full index, so you might expect a blanket ban on `WHERE`
    ///    clauses.
    /// 2. `data IS NULL` is irrelevant to `pkey`.
    ///
    /// But in reality, this query is not only accepted---the `WHERE` clause is
    /// **completely, silently ignored**. `data` will be set to `'foo'` no
    /// matter if it is `NULL` or not. (This is true as of both CockroachDB
    /// 23.2.0 and PostgreSQL 16.2).
    ///
    /// This seems pretty bad! Why does it happen?
    ///
    /// ## 5. A digression into logical systems
    ///
    /// The reason for that ties back into the idea that in `ON CONFLICT ...
    /// WHERE`, the `WHERE` clause can be anything that _logically implies_ the
    /// index.
    ///
    /// Let's take a step back and think about what logical implication (often
    /// represented by `->`) means. In classical logic, `p -> q` is interpreted
    /// as *[material implication]*, and is logically equivalent to `!p || q`.
    ///
    /// Consider what happens if `q` is a _tautology_, i.e. it is always true.
    /// In that case, no matter what `p` is, `p -> q` is always true.
    ///
    /// Applying this to our SQL example: Since `pkey` is a full index, it is
    /// equivalent to a partial index with `WHERE true`. In other words, `q` is
    /// always true. This means that `p` (the `WHERE` clause) can be anything.
    /// As a result, Cockroach completely ignores the `WHERE` clause.
    ///
    /// **This seems really bad!** Our intuitive understanding of "p implies q"
    /// typically has some kind of notion of _relevance_ attached to it: we
    /// expect that `p` being true has to have some bearing on q. But material
    /// implication does not capture this idea at all. (Put a pin in this,
    /// we're going to come back to it shortly.)
    ///
    /// ## 6. Towards addressing this
    ///
    /// To summarize where we're at:
    ///
    /// * `ON CONFLICT ... WHERE ... DO UPDATE` is easy to mix up with `ON
    ///   CONFLICT ... DO UPDATE ... WHERE`.
    /// * `ON CONFLICT ... WHERE ... DO UPDATE` on a full index silently
    ///   accepts _anything_ in the `WHERE` clause.
    /// * `ON CONFLICT ... WHERE ... DO UPDATE` is *required* for a partial
    ///   index.
    ///
    /// How can we prevent misuse while still supporting legitimate uses?
    ///
    /// One option is to look at [Diesel](https://diesel.rs/), our Rust ORM and
    /// query builder. In Diesel, the usual way to add a `WHERE` clause is via
    /// [the `filter` method]. But the special case of `ON CONFLICT ... WHERE`
    /// is expressed via a separate [`filter_target` method].
    ///
    /// Our first inclination might be to just ban `filter_target` entirely,
    /// using Clippy's [`disallowed_methods`] lint. This would work great if
    /// all we had was full indexes! But we do necessarily have partial
    /// indexes, and we must do something about them.
    ///
    /// That's where `as_partial_index` comes in.
    ///
    /// ## 7. Back to logical systems
    ///
    /// How does `as_partial_index` address this problem? Let's go back to
    /// material implication: `p -> q` is logically equivalent to `!p || q`.
    ///
    /// So far, we've discussed the case where `q` is a tautology. But let's
    /// look at the other side of it: what if `p` is a _contradiction_? In
    /// other words, what if `p` is always false? In that case, it is clear
    /// that `p -> q` is always true, no matter what `q` is.
    ///
    /// It's worth sitting with this for a moment. What this means is that you
    /// can make statements that are genuinely nonsense, like "if 2 + 2 = 5,
    /// then the sky is green", but are valid in classical logic.
    ///
    /// The fact that contradictions are "poisonous" to classical logic is a
    /// well-known problem, called the [principle of explosion]. This is not
    /// new---Aristotle was likely aware of it, and it was first described
    /// fully by William of Soissons in the 12th century. Intuitively, **this
    /// is even worse** than the earlier case where `q` is a tautology: at
    /// least there, `q` is actually true so it doesn't feel too bad that `p`
    /// doesn't matter.
    ///
    /// ## 8. Putting it all together
    ///
    /// The good news about the principle of explosion, though, is that this
    /// gives us a path out. Remember that `ON CONFLICT ... WHERE ...` requires
    /// that the bit after `WHERE` logically imply the partial index. Since a
    /// contradiction implies anything, a clause like `WHERE 1 = 2`, or `WHERE
    /// 'x' = 'y'`, works.
    ///
    /// In SQL, the simplest contradiction is simply `false`. So we use `WHERE
    /// false` here. The effect of this is that it causes the database to
    /// automatically infer the right partial index constraints.
    ///
    /// So the solution we came up in the end is to:
    ///
    /// * Ban `filter_target` via Clippy's `disallowed_methods` lint, to avoid
    ///   misuse in cases where there isn't a partial index.
    /// * Define `as_partial_index` as a method that adds `WHERE false` to the
    ///  `ON CONFLICT` expression.
    /// * For the cases where there really is a partial index, use
    ///  `as_partial_index`.
    ///
    /// `as_partial_index` is also accepted in cases where there's a full
    /// index. That isn't a huge problem because a stray `WHERE false` is
    /// pretty harmless. The goal is to prevent misuse of `filter_target`, and
    /// that part is still upheld.
    ///
    /// ---
    ///
    /// ## Further notes
    ///
    /// The [paradoxes of material implication] are one of the central topics
    /// in philosophical logic. Many non-classical logical systems have been
    /// proposed to address this, including [paraconsistent logics] and
    /// [relevance logics]. The common thread among these systems is that they
    /// define implication differently from classical logic, in a way that
    /// tries to match our common understanding better.
    ///
    /// Above, we focused on classical logic. SQL uses the non-classical
    /// [three-valued logic] called **K3**. This logic defines implication in
    /// the same way, and with the same issues, as material implication in
    /// classical logic.
    ///
    /// With Cockroach 23.2.0 and earlier versions, for a conflict on a full
    /// index, you can even specify non-existent columns: `ON CONFLICT ...
    /// WHERE non_existent = 1`. This is rejected by Postgres, and has been
    /// acknowledged as a bug in Cockroach.
    ///
    /// There is also an `ON CONFLICT ON CONSTRAINT` syntax which allows users
    /// to specify the name of the constraint. That syntax only works for full
    /// (non-partial) indexes.
    ///
    /// Other references:
    ///
    /// * [Omicron issue
    ///   #5047](https://github.com/oxidecomputer/omicron/issues/5047)
    /// * [CockroachDB issue
    ///   #119197](https://github.com/cockroachdb/cockroach/issues/119117)
    ///
    /// [partial indexes]: https://www.cockroachlabs.com/docs/stable/partial-indexes.html
    /// [the `ON_CONFLICT` clause]:
    ///     https://www.cockroachlabs.com/docs/stable/insert#on-conflict-clause
    /// [material implication]:
    ///     https://en.wikipedia.org/wiki/Material_conditional
    /// [Diesel]: https://diesel.rs/
    /// [the `filter` method]:
    ///     https://docs.rs/diesel/2.1.4/diesel/query_dsl/methods/trait.FilterDsl.html#tymethod.filter
    /// [`filter_target` method]:
    ///     https://docs.rs/diesel/2.1.4/diesel/upsert/trait.DecoratableTarget.html#tymethod.filter_target
    /// [`disallowed_methods`]:
    ///     https://rust-lang.github.io/rust-clippy/master/index.html#disallowed_methods
    /// [principle of explosion]:
    ///     https://en.wikipedia.org/wiki/Principle_of_explosion
    /// [paradoxes of material implication]:
    ///     https://en.wikipedia.org/wiki/Paradoxes_of_material_implication
    /// [paraconsistent logics]:
    ///     https://en.wikipedia.org/wiki/Paraconsistent_logic
    /// [relevance logics]: https://en.wikipedia.org/wiki/Relevance_logic
    /// [three-valued logic]: https://en.wikipedia.org/wiki/Three-valued_logic
    fn as_partial_index(self) -> Self::AsPartialIndexOutput;
}

impl<Stmt, Target> IncompleteOnConflictExt
    for IncompleteOnConflict<Stmt, Target>
where
    Target: DecoratableTarget<SqlLiteral<Bool>>,
{
    type AsPartialIndexOutput =
        IncompleteOnConflict<Stmt, DecoratedAsPartialIndex<Target>>;

    fn as_partial_index(self) -> Self::AsPartialIndexOutput {
        // Bypass this clippy warning in this one place only.
        #[allow(clippy::disallowed_methods)]
        {
            // For why "false", see the doc comment above.
            self.filter_target(sql::<Bool>("false"))
        }
    }
}

type DecoratedAsPartialIndex<Target> =
    <Target as DecoratableTarget<SqlLiteral<Bool>>>::FilterOutput;
