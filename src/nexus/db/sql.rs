/*!
 * Facilities for working with and generating SQL strings
 *
 * This is a bit of a mini ORM.  The facilities in this file are intended to be
 * agnostic to the control plane.  There is a bit of leakage in a few places.
 */

use crate::api_error::ApiError;
use crate::api_model::ApiResourceType;
use crate::api_model::DataPageParams;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::ToSql;

/**
 * Interface for constructing SQL query strings and associated parameters for
 * handling untrusted input
 *
 * This interface helps you safely construct SQL strings with untrusted input.
 * **But it does not guarantee that the resulting SQL string is safe.**  You
 * must use `next_param()` for _all_ untrusted input for the resulting SQL
 * string to be safe from SQL injection.
 *
 * ## Example
 *
 * Here's how you can safely include untrusted input into a SQL string using
 * this interface:
 *
 * ```no_run
 * # #[tokio::main]
 * # async fn main() {
 * # use omicron::nexus::db::sql::SqlString;
 * let mut sql = SqlString::new();
 * let param = sql.next_param(&"Robert'); DROP TABLE Students;--");
 * sql.push_str(&format!("SELECT * FROM Students WHERE name = {}", param));
 * assert_eq!(sql.sql_fragment(), "SELECT * FROM Students WHERE name = $1");
 *
 * // You can safely use this like so:
 * # fn get_client() -> tokio_postgres::Client { unimplemented!(); }
 * let client: tokio_postgres::Client = get_client();
 * let rows = client.query(sql.sql_fragment(), sql.sql_params()).await;
 * # }
 * ```
 */
/*
 * TODO-design Would it be useful (and possible) to have a macro like
 * `sql!(sql_str[, param...])` where `sql_str` is `&'static str` and the params
 * are all numbered params?
 */
pub struct SqlString<'a> {
    contents: String,
    params: Vec<&'a (dyn ToSql + Sync)>,
}

impl<'a> SqlString<'a> {
    pub fn new() -> SqlString<'a> {
        SqlString { contents: String::new(), params: Vec::new() }
    }

    /**
     * Construct a new SqlString with an empty string, but whose first numbered
     * parameter starts after the given string.  This is useful for cases where
     * one chunk of code has generated a SqlString and another wants to insert
     * that string (and parameters) into a new one.  Since the parameter numbers
     * are baked into the SQL text, the only way this can work is if the
     * existing SqlString's parameters come first.  The caller is responsible
     * for joining the SQL text.
     * XXX TODO-coverage
     */
    pub fn new_with_params(s: SqlString<'a>) -> (SqlString<'a>, String) {
        let old_sql = s.sql_fragment().to_owned();
        let new_str = SqlString { contents: String::new(), params: s.params };
        (new_str, old_sql)
    }

    /**
     * Append `s` to the end of the SQL string being constructed
     *
     * `s` is not validated or escaped in any way.  The caller is responsible
     * for sending any untrusted input through `next_param()` and using only the
     * identifier returned by `next_param()` in this string.
     */
    pub fn push_str(&mut self, s: &str) {
        self.contents.push_str(s)
    }

    /**
     * Allocate a new parameter with value `v`, returning an identifier name
     * that can be inserted into the SQL string wherever you want to use the
     * parameter.
     */
    pub fn next_param<'b>(&'b mut self, v: &'a (dyn ToSql + Sync)) -> String
    where
        'a: 'b,
    {
        self.params.push(v);
        format!("${}", self.params.len())
    }

    /** Return the constructed SQL fragment. */
    pub fn sql_fragment(&self) -> &str {
        &self.contents
    }

    /** Return the parameters allocated by calls to `next_param()`. */
    pub fn sql_params(&self) -> &[&'a (dyn ToSql + Sync)] {
        &self.params
    }
}

// XXX remove this -- it's just a BTreeMap
// /**
//  * Build up a list of SQL name-value pairs
//  *
//  * This struct stores names and corresponding SQL values and provides a way to
//  * get them back out suitable for use in safe INSERT or UPDATE statements.  For
//  * both INSERT and UPDATE, the values are provided as parameters to the query.
//  *
//  * Like the other interfaces here, the names here must be `&'static str` to make
//  * it harder to accidentally provide user input here, as these cannot be passed
//  * as separate parameters for the query.
//  * TODO-cleanup should this just be a BTreeMap?
//  */
// pub struct SqlValueSet<'a> {
//     names: Vec<&'static str>,
//     values: Vec<&'a (dyn ToSql + Sync)>,
//     names_unique: BTreeSet<&'static str>,
// }
//
// impl<'a> SqlValueSet<'a> {
//     fn new() -> SqlValueSet<'a> {
//         SqlValueSet {
//             names: Vec::new(),
//             values: Vec::new(),
//             names_unique: BTreeSet::new(),
//         }
//     }
//
//     fn set(&mut self, name: &'static str, value: &'a (dyn ToSql + Sync)) {
//         assert!(
//             self.names_unique.insert(name),
//             "duplicate name specified for SqlValueSet"
//         );
//         self.names.push(name);
//         self.values.push(value);
//     }
//
//     fn names(&self) -> &[&'static str] {
//         &self.names
//     }
//
//     fn values(&self) -> &[&(dyn ToSql + Sync)] {
//         &self.values
//     }
// }

// XXX TODO-doc TODO-coverage
pub fn sql_update_from_set<'a, 'b>(
    kvpairs: &'a BTreeMap<&'static str, &'b (dyn ToSql + Sync)>,
    output: &'a mut SqlString<'b>,
) {
    let set_parts = kvpairs
        .iter()
        .map(|(name, value): (&&'static str, &&(dyn ToSql + Sync))| {
            assert!(valid_cockroachdb_identifier(*name));
            format!("{} = {}", *name, output.next_param(*value))
        })
        .collect::<Vec<String>>();
    output.push_str(&set_parts.join(", "));
}

/** Describes a table in the control plane database */
/*
 * TODO-design We want to find a better way to abstract this.  Diesel provides a
 * compelling model in terms of using it.  But it also seems fairly heavyweight
 * and seems to tightly couple the application to the current database schema.
 * This pattern of fetch-or-insert all-fields-of-an-object likely _isn't_ our
 * most common use case, even though we do it a lot for basic CRUD.
 * TODO-robustness it would also be great if this were generated from
 * src/sql/dbinit.sql or vice versa.  There is at least a test that verifies
 * they're in sync.
 */
pub trait Table {
    /** Struct that represents rows of this table when the full row is needed */
    /* TODO-cleanup what does the 'static actually mean here? */
    type ModelType: for<'a> TryFrom<&'a tokio_postgres::Row, Error = ApiError>
        + Send
        + 'static;
    // TODO-cleanup can we remove the RESOURCE_TYPE here?  And if so, can we
    // make this totally agnostic to the control plane?
    /** [`ApiResourceType`] that corresponds to rows of this table */
    const RESOURCE_TYPE: ApiResourceType;
    /** Name of the table */
    const TABLE_NAME: &'static str;
    /** List of names of all columns in the table. */
    const ALL_COLUMNS: &'static [&'static str];

    /**
     * Parts of a WHERE clause that should be included in all queries for live
     * records
     */
    const LIVE_CONDITIONS: &'static str = "time_deleted IS NULL";
}

/**
 * Used to generate WHERE clauses for individual row lookups and paginated scans
 *
 * Impls of `LookupKey` describe the various queries that are used to select
 * rows from the database.
 *
 * ## Looking up an object
 *
 * There are three common ways to identify an object:
 *
 *    o by its id, which is universally unique.  Any object can be looked up by
 *      its id.  This is implemented using [`LookupByUniqueId`].
 *    o by a name that is unique within the whole control plane.  Organizations
 *      have a unique name, for example, since they do not exist within any
 *      other scope.  This is implemented using [`LookupByUniqueName`].
 *    o by a name that is unique within the scope of a parent object, which
 *      itself has a unique id.  For example, Instances have names that are
 *      unique within their Project, so you can look up any Instance using the
 *      Project's id and the Instance's name.  This is implemented using
 *      [`LookupByUniqueNameInProject`].
 *
 * Other lookups are possible as well.  For example,
 * [`LookupByAttachedInstance`] specifically looks up disks based on the
 * instance that they're attached to.
 *
 *
 * ## Parts of a `LookupKey`
 *
 * Implementations of `LookupKey` define a _scope key_ and an _item key_.  For
 * example, Disks have a "name" that is unique within a Project.  For a Disk,
 * the usual scope key is a project id, and the usual item key is the name of
 * the disk.  That said, nothing requires that a row have only one way to look
 * it up -- and indeed, resources like Disks can be looked up by unique id alone
 * (null scope, id as item key) as well as by its name within a project
 * (project_id scope, name as item key).
 *
 * The scope key can be a combination of values (corresponding to a combination
 * of columns in the database).  We use the [`IntoToSqlVec`] trait to turn a
 * given scope key into a set of SQL values that we can insert into a query.
 *
 * The item key currently must be a single SQL value.  It would be pretty easy
 * to generalize this to a set of values just like the scope key.  The challenge
 * is only that item keys for pagination typically come from [`DataPageParams`],
 * which assumes a single field.
 *
 *
 * ## Pagination
 *
 * `LookupKey` supports both direct lookup of rows as described above as well as
 * _pagination_: fetching the next rows in an ordered sequence (without knowing
 * what the next row is).  The main difference is that a direct lookup fetches a
 * row _exactly_ matching the scope parameters and item key, while pagination
 * fetches rows exactly matching the scope parameters and sorted immediately
 * _after_ the given item key.
 */
pub trait LookupKey<'a> {
    /*
     * Items defined by the various impls of this trait
     */

    /** Names of the database columns that make up the scope key */
    const SCOPE_KEY_COLUMN_NAMES: &'static [&'static str];

    /**
     * Rust type describing the scope key
     *
     * This must be convertible into a list of SQL parameters of the same length
     * and sequence as the column names in [`SCOPE_KEY_COLUMN_NAMES`].  This
     * is done using the [`IntoToSqlVec`] trait.
     */
    type ScopeKey: IntoToSqlVec<'a> + 'a + Clone + Copy;

    /** Name of the database column storing the item key */
    const ITEM_KEY_COLUMN_NAME: &'static str;

    /** Rust type describing the item key */
    type ItemKey: ToSql + for<'f> FromSql<'f> + Sync + Clone + 'static;

    /**
     * Generates an error for the case where no item was found for a particular
     * lookup
     */
    fn where_select_error<T: Table>(
        scope_key: Self::ScopeKey,
        item_key: &Self::ItemKey,
    ) -> ApiError;

    /*
     * The rest of this trait provides common implementation for all impls
     * (in terms of the above items).
     */

    /**
     * Appends to `output` a WHERE clause for selecting a specific row based on
     * the given scope key and item key
     *
     * Typically we'll create a unique index on (scope key, item key) so that at
     * most one row will match.  However, the SQL generated here neither assumes
     * nor guarantees that only one row will match.
     */
    fn where_select_rows<'b>(
        scope_key: Self::ScopeKey,
        item_key: &'a Self::ItemKey,
        output: &'b mut SqlString<'a>,
    ) where
        'a: 'b,
    {
        let mut column_names =
            Vec::with_capacity(Self::SCOPE_KEY_COLUMN_NAMES.len() + 1);
        column_names.extend_from_slice(&Self::SCOPE_KEY_COLUMN_NAMES);
        column_names.push(Self::ITEM_KEY_COLUMN_NAME);
        let mut param_values = scope_key.to_sql_vec();
        param_values.push(item_key);
        where_cond(&column_names, &param_values, "=", output);
    }

    /**
     * Appends to `output` a a WHERE clause for selecting a page of rows
     *
     * A "page" here is a sequence of rows according to some sort order, as in
     * API pagination.  Callers of this function specify the page by specifying
     * values from the last row they saw, not necessarily knowing exactly which
     * row(s) are next.  By contrast, [`where_select_rows`] (besides usually
     * being used to select a only single row) does not assume anything about
     * the ordering and expects callers to provide the item key of the row they
     * want.
     */
    fn where_select_page<'b, 'c, 'd>(
        scope_key: Self::ScopeKey,
        pagparams: &'c DataPageParams<'c, Self::ItemKey>,
        output: &'b mut SqlString<'d>,
    ) where
        'a: 'b + 'd,
        'c: 'a,
    {
        let (operator, order) = match pagparams.direction {
            dropshot::PaginationOrder::Ascending => (">", "ASC"),
            dropshot::PaginationOrder::Descending => ("<", "DESC"),
        };

        /*
         * First, generate the conditions that are true for every page.  For
         * example, when listing Instances in a Project, this would specify the
         * project_id.
         */
        let fixed_column_names = Self::SCOPE_KEY_COLUMN_NAMES;
        if fixed_column_names.len() > 0 {
            let fixed_param_values = scope_key.to_sql_vec();
            output.push_str(" AND (");
            where_cond(fixed_column_names, &fixed_param_values, "=", output);
            output.push_str(") ");
        }

        /*
         * If a marker was provided, then generate conditions that resume the
         * scan after the marker value.
         */
        if let Some(marker) = pagparams.marker {
            let var_column_names = &[Self::ITEM_KEY_COLUMN_NAME];
            let marker_ref = marker as &(dyn ToSql + Sync);
            let var_param_values = vec![marker_ref];
            output.push_str(" AND (");
            where_cond(var_column_names, &var_param_values, operator, output);
            output.push_str(") ");
        }

        /*
         * Generate the ORDER BY clause based on the columns that make up the
         * marker.
         */
        output.push_str(&format!(
            " ORDER BY {} {} ",
            Self::ITEM_KEY_COLUMN_NAME,
            order,
        ));
    }
}

/**
 * Defines a conversion for a type into a list of SQL parameters
 *
 * This is used for the scope key used within a [`LookupKey`].  Currently, all
 * scope keys are tuples of 0 or 1 element so we define impls for those types.
 */
pub trait IntoToSqlVec<'a> {
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)>;
}

impl<'a> IntoToSqlVec<'a> for () {
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
        Vec::new()
    }
}

impl<'a, 't1, T1> IntoToSqlVec<'a> for (&'t1 T1,)
where
    T1: ToSql + Sync,
    't1: 'a,
{
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
        vec![self.0]
    }
}

impl<'a, 't1, 't2, T1, T2> IntoToSqlVec<'a> for (&'t1 T1, &'t2 T2)
where
    T1: ToSql + Sync,
    't1: 'a,
    T2: ToSql + Sync,
    't2: 'a,
{
    fn to_sql_vec(self) -> Vec<&'a (dyn ToSql + Sync)> {
        vec![self.0, self.1]
    }
}

/**
 * Appends to `output` a SQL fragment specifying that each column in
 * `column_names` match the corresponding value in `param_values`
 *
 * `operator` specifies which operator is used: `"="` would say that the values
 * in returned rows must exactly match those in `param_values`.
 *
 * This function uses SQL parameters for all the of the values, so the caller
 * need not escape the values or handle them specially in any way.
 *
 * The column names are required to be `&'static str` to make it more difficult
 * to accidentally sneak user input into the query string (SQL injection).  In
 * practice, column names are typically hardcoded wherever they come from and
 * they need to be passed between functions as `&'static str` to use them here.
 *
 * ## Example
 *
 * ```
 * # use omicron::nexus::db::sql::SqlString;
 * # use omicron::nexus::db::sql::where_cond;
 * use tokio_postgres::types::FromSql;
 * # use tokio_postgres::types::IsNull;
 * use tokio_postgres::types::ToSql;
 * use tokio_postgres::types::Type;
 *
 * let column_names: &[&'static str] = &["city", "racks"];
 * let param_values: &[&(dyn ToSql + Sync)] = &[&"Ashburn", &123i64];
 * let mut output = SqlString::new();
 * where_cond(column_names, param_values, "=", &mut output);
 * // The SQL string includes one condition for each column and value,
 * // using parameters for the values.
 * assert_eq!("( (city = $1) AND (racks = $2) )", output.sql_fragment());
 * let params = output.sql_params();
 * assert_eq!(params.len(), 2);
 *
 * // Parameter $1 will serialize to the SQL string `"Ashburn"`
 * let mut bytes = bytes::BytesMut::new();
 * # let there =
 * params[0].to_sql_checked(&Type::TEXT, &mut bytes);
 * # assert!(matches!(there.unwrap(), IsNull::No));
 * let back = <String as FromSql<'_>>::from_sql(&Type::TEXT, &bytes)
 *     .expect("failed to deserialize $1");
 * assert_eq!(back, "Ashburn");
 *
 * // Parameter $2 will serialize to the SQL int `123`
 * let mut bytes = bytes::BytesMut::new();
 * # let there =
 * params[1].to_sql_checked(&Type::INT8, &mut bytes);
 * # assert!(matches!(there.unwrap(), IsNull::No));
 * let back = <i64 as FromSql<'_>>::from_sql(&Type::INT8, &bytes)
 *     .expect("failed to deserialize $2");
 * assert_eq!(back, 123);
 * ```
 *
 * ## Panics
 *
 * If `column_names.len() != param_values.len()`.
 */
/*
 * NOTE: This function is public only so that we can access it from the example.
 */
pub fn where_cond<'a, 'b>(
    column_names: &'b [&'static str],
    param_values: &'b [&'a (dyn ToSql + Sync)],
    operator: &'static str,
    output: &'b mut SqlString<'a>,
) where
    'a: 'b,
{
    assert_eq!(param_values.len(), column_names.len());

    if column_names.is_empty() {
        output.push_str("TRUE");
    } else {
        let conditions = column_names
            .iter()
            .zip(param_values.iter())
            .map(|(name, value): (&&'static str, &&(dyn ToSql + Sync))| {
                assert!(valid_cockroachdb_identifier(name));
                format!(
                    "({} {} {})",
                    *name,
                    operator,
                    output.next_param(*value)
                )
            })
            .collect::<Vec<String>>()
            .join(" AND ");
        output.push_str(format!("( {} )", conditions).as_str());
    }
}

/**
 * Returns whether `name` is a valid CockroachDB identifier
 *
 * This is intended as a sanity-check, not an authoritative validator.
 */
pub fn valid_cockroachdb_identifier(name: &'static str) -> bool {
    /*
     * It would be nice if there were a supported library interface for this.
     * Instead, we rely on the CockroachDB documentation on the syntax for
     * identifiers.
     */
    let mut iter = name.chars();
    let maybe_first = iter.next();
    if maybe_first.is_none() {
        return false;
    }

    let first = maybe_first.unwrap();
    if !first.is_alphabetic() && first != '_' {
        return false;
    }

    for c in iter {
        if c != '_' && c != '$' && !c.is_alphanumeric() {
            return false;
        }
    }

    return true;
}

#[cfg(test)]
mod test {
    use super::valid_cockroachdb_identifier;
    use super::where_cond;
    use super::SqlString;
    use tokio_postgres::types::ToSql;

    #[test]
    fn test_validate_identifier() {
        assert!(valid_cockroachdb_identifier("abc"));
        assert!(valid_cockroachdb_identifier("abc123"));
        assert!(valid_cockroachdb_identifier("abc$123"));
        assert!(valid_cockroachdb_identifier("abc_123"));
        assert!(valid_cockroachdb_identifier("_abc_123"));
        assert!(valid_cockroachdb_identifier("_abc_123$"));

        assert!(!valid_cockroachdb_identifier(""));
        assert!(!valid_cockroachdb_identifier("ab\"cd"));
        assert!(!valid_cockroachdb_identifier("1abc"));
        assert!(!valid_cockroachdb_identifier("ab cd"));
        assert!(!valid_cockroachdb_identifier("$abcd"));
    }

    #[test]
    fn test_where_cond() {
        /*
         * We tested a basic case in the example above.  Here we test edge
         * cases.
         */
        let column_names: &[&'static str] = &["c1"];
        let param_values: &[&(dyn ToSql + Sync)] = &[&"v1"];
        /* List of length 1 */
        let mut output = SqlString::new();
        where_cond(column_names, &param_values, "=", &mut output);
        assert_eq!("( (c1 = $1) )", output.sql_fragment());

        /* Other operators besides "=" */
        let mut output = SqlString::new();
        where_cond(column_names, &param_values, ">=", &mut output);
        assert_eq!("( (c1 >= $1) )", output.sql_fragment());
        let mut output = SqlString::new();
        where_cond(column_names, &param_values, "<", &mut output);
        assert_eq!("( (c1 < $1) )", output.sql_fragment());

        /* Zero-length list */
        let mut output = SqlString::new();
        where_cond(&[], &[], "=", &mut output);
        assert_eq!("TRUE", output.sql_fragment());
    }

    #[test]
    #[should_panic]
    fn test_where_cond_bad_length() {
        let column_names: &[&'static str] = &["c1"];
        let mut output = SqlString::new();
        where_cond(column_names, &[], "=", &mut output);
    }

    #[test]
    #[should_panic]
    fn test_where_cond_bad_column() {
        let column_names: &[&'static str] = &["c1\"c2"];
        let param_values: &[&(dyn ToSql + Sync)] = &[&"v1"];
        let mut output = SqlString::new();
        where_cond(column_names, param_values, "=", &mut output);
    }

    /* TODO-coverage tests for SqlString */
    /* TODO-coverage tests for LookupKey */
}
