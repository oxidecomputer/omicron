//! Functions for querying the timeseries database.
// Copyright 2021 Oxide Computer Company

use crate::model::DATABASE_NAME;
use crate::Error;
use chrono::{DateTime, Utc};
use oximeter::types::{DatumType, FieldValue};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Object used to filter timestamps, specifying a start and/or end time.
///
/// Note that the endpoints are interpreted as inclusive, so a timestamp matching the endpoints is
/// also returned.
#[derive(Debug, Clone, Copy)]
pub enum TimeFilter {
    /// Passes timestamps before the contained value
    Before(DateTime<Utc>),
    /// Passes timestamps after the contained value
    After(DateTime<Utc>),
    /// Passes timestamps between the two contained values
    Between(DateTime<Utc>, DateTime<Utc>),
}

impl TimeFilter {
    pub fn start_time(&self) -> Option<&DateTime<Utc>> {
        match self {
            TimeFilter::Before(_) => None,
            TimeFilter::After(t) | TimeFilter::Between(t, _) => Some(t),
        }
    }

    pub fn end_time(&self) -> Option<&DateTime<Utc>> {
        match self {
            TimeFilter::Before(t) | TimeFilter::Between(_, t) => Some(t),
            TimeFilter::After(_) => None,
        }
    }

    fn as_where_fragment(&self) -> String {
        match self {
            TimeFilter::Before(end) => {
                format!("timestamp <= '{}'", end.naive_utc())
            }
            TimeFilter::After(start) => {
                format!("timestamp >= '{}'", start.naive_utc())
            }
            TimeFilter::Between(start, end) => format!(
                "timestamp >= '{}' AND timestamp <= '{}'",
                start.naive_utc(),
                end.naive_utc()
            ),
        }
    }

    /// Construct a `TimeFilter` which selects timestamps between `after` and `before`.
    ///
    /// If both `before` and `after` are `None`, then `None` is returned. Otherwise, a variant of
    /// `TimeFilter` is constructed and returned.
    pub fn from_timestamps(
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
    ) -> Result<Option<Self>, Error> {
        match (after, before) {
            (None, None) => Ok(None),
            (Some(after), None) => Ok(Some(TimeFilter::After(after))),
            (None, Some(before)) => Ok(Some(TimeFilter::Before(before))),
            (Some(after), Some(before)) => {
                if after < before {
                    Ok(Some(TimeFilter::Between(after, before)))
                } else {
                    Err(Error::QueryError(String::from("Invalid timestamps, end must be strictly later than start")))
                }
            }
        }
    }
}

/// A string-typed filter, used to build filters on timeseries fields from external input.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Filter {
    /// The name of the field.
    pub name: String,

    /// The value of the field as a string.
    pub value: String,
}

impl std::str::FromStr for Filter {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('=').collect::<Vec<_>>();
        if parts.len() == 2 {
            Ok(Filter {
                name: parts[0].to_string(),
                value: parts[1].to_string(),
            })
        } else {
            Err(Error::QueryError(String::from(
                "String filters must be specified as `name=value` pairs",
            )))
        }
    }
}

// Format the value for use in a query to the database, e.g., `... WHERE (field_value = {})`.
fn field_as_db_str(value: &FieldValue) -> String {
    match value {
        FieldValue::Bool(ref inner) => {
            format!("{}", if *inner { 1 } else { 0 })
        }
        FieldValue::I64(ref inner) => format!("{}", inner),
        FieldValue::IpAddr(ref inner) => {
            let addr = match inner {
                IpAddr::V4(ref v4) => v4.to_ipv6_mapped(),
                IpAddr::V6(ref v6) => *v6,
            };
            format!("'{}'", addr)
        }
        FieldValue::String(ref inner) => format!("'{}'", inner),
        FieldValue::Uuid(ref inner) => format!("'{}'", inner),
    }
}

// Return the name of the type as it's referred to in the timeseries database. This is used
// internally to build the table containing samples of the corresponding datum type.
fn db_type_name_for_datum(ty: &DatumType) -> &str {
    match ty {
        DatumType::Bool => "bool",
        DatumType::I64 => "i64",
        DatumType::F64 => "f64",
        DatumType::String => "string",
        DatumType::Bytes => "bytes",
        DatumType::CumulativeI64 => "cumulativei64",
        DatumType::CumulativeF64 => "cumulativef64",
        DatumType::HistogramI64 => "histogrami64",
        DatumType::HistogramF64 => "histogramf64",
    }
}

/// A `FieldFilter` specifies a field by name and one or more values to compare against by
/// equality.
#[derive(Debug, Clone)]
pub struct FieldFilter {
    field_name: String,
    field_values: Vec<FieldValue>,
}

impl FieldFilter {
    /// Construct a filter applied to a field of the given name.
    ///
    /// `field_values` is a slice of data that can be converted into a `FieldValue`. The filter
    /// matches any fields with the given name where the value is one of those specified in
    /// `field_values` (i.e., it matches `field_values[0]` OR `field_values[1]`, etc.).
    pub fn new<T>(field_name: &str, field_values: &[T]) -> Result<Self, Error>
    where
        T: Into<FieldValue> + Clone,
    {
        if field_values.is_empty() {
            return Err(Error::QueryError(String::from(
                "Field filters may not be empty",
            )));
        }
        let field_values = field_values.iter().map(FieldValue::from).collect();
        Ok(Self { field_name: field_name.to_string(), field_values })
    }

    pub fn field_name(&self) -> &String {
        &self.field_name
    }

    pub fn field_values(&self) -> &Vec<FieldValue> {
        &self.field_values
    }

    fn table_type(&self) -> String {
        self.field_values[0].field_type().to_string().to_lowercase()
    }

    fn as_where_fragment(&self) -> String {
        let field_value_fragment = self
            .field_values
            .iter()
            .map(|field| format!("(field_value = {})", field_as_db_str(field)))
            .collect::<Vec<_>>()
            .join(" OR ");
        format!(
            "field_name = '{field_name}' AND ({value_filter})",
            field_name = self.field_name,
            value_filter = field_value_fragment,
        )
    }
}

/// Object used to filter timeseries.
///
/// Timeseries must be selected by name. A list of filters applied to the fields of the timeseries
/// can be used to further restrict the matching timeseries. The `time_filter` is used to restrict
/// the data to the specified time window, and is applied to all matching timeseries.
#[derive(Debug, Clone)]
pub struct TimeseriesFilter {
    /// The name of the timeseries to select
    pub timeseries_name: String,
    /// Filters applied to the fields of the timeseries
    pub filters: Vec<FieldFilter>,
    /// Filter applied to the timestamps of the timeseries
    pub time_filter: Option<TimeFilter>,
}

impl TimeseriesFilter {
    /// Return the name of the each table this filter applies to.
    fn table_names(&self) -> Vec<String> {
        let table_name = |filter: &FieldFilter| {
            format!(
                "{db_name}.fields_{type_}",
                db_name = DATABASE_NAME,
                type_ = filter.table_type()
            )
        };
        self.filters().iter().map(table_name).collect()
    }

    /// Return the filters applied to each field of this filter
    fn filters(&self) -> &Vec<FieldFilter> {
        &self.filters
    }

    // Return the SELECT clauses used to apply each field filter
    fn field_select_queries(&self) -> Vec<String> {
        let select_query = |(table_name, filter): (&String, &FieldFilter)| {
            format!(
                concat!(
                    "SELECT\n",
                    "{timeseries_name},\n",
                    "{timeseries_key}\n",
                    "FROM {table_name}\n",
                    "WHERE ({where_fragment} AND (timeseries_name = '{ts_name}'))",
                ),
                timeseries_name = indent("timeseries_name", 4),
                timeseries_key = indent("timeseries_key", 4),
                table_name = table_name,
                where_fragment = filter.as_where_fragment(),
                ts_name = self.timeseries_name,
            )
        };
        self.table_names()
            .iter()
            .zip(self.filters())
            .map(select_query)
            .collect()
    }

    /// Generate a select query for this filter
    pub(crate) fn as_select_query(&self, datum_type: DatumType) -> String {
        let timestamp_filter = self
            .time_filter
            .map(|f| format!("AND {}", f.as_where_fragment()))
            .unwrap_or_else(String::new);

        let select_queries = self.field_select_queries();
        let (filter_columns, query) = match select_queries.len() {
            0 => (
                String::from("timeseries_name"),
                format!("'{}'", self.timeseries_name),
            ),
            1 => {
                // We only have one subquery, just use it directly
                (
                    String::from("timeseries_name, timeseries_key"),
                    select_queries[0].clone(),
                )
            }
            _ => {
                // We have a list of subqueries, which must be JOIN'd.
                // The JOIN occurs using equality between the timeseries keys and timestamps, with the
                // previous subquery alias.
                let subqueries = select_queries
                    .into_iter()
                    .enumerate()
                    .map(|(i, subquery)| {
                        let on_fragment = if i == 0 {
                            String::new()
                        } else {
                            format!(
                                " ON filter{i}.timeseries_name = filter{j}.timeseries_name \
                                AND filter{i}.timeseries_key = filter{j}.timeseries_key",
                                i = i, j = i - 1,
                            )
                        };
                        format!(
                            "(\n{subquery}\n) AS filter{i}{on_fragment}",
                            subquery = indent(&subquery, 4),
                            i = i,
                            on_fragment = on_fragment,
                        )
                    })
                .collect::<Vec<_>>().join("\nINNER JOIN\n");

                // We also need to write an _additional_ first select statement to extract the columns
                // from the subquery aliased as filter0.
                (
                    String::from("timeseries_name, timeseries_key"),
                    format!(
                        "SELECT\n\
                        {timeseries_name},\n\
                        {timeseries_key}\n\
                        FROM\n\
                        {subqueries}",
                        timeseries_name = indent("filter0.timeseries_name", 4),
                        timeseries_key = indent("filter0.timeseries_key", 4),
                        subqueries = subqueries,
                    ),
                )
            }
        };

        // Format the top-level query
        format!(
            "SELECT *\n\
            FROM {db_name}.measurements_{data_type}\n\
            WHERE ({filter_columns}) IN (\n\
            {query}\n\
            ){timestamp_filter}\n\
            ORDER BY (timeseries_name, timeseries_key, timestamp)\n\
            FORMAT JSONEachRow;",
            db_name = DATABASE_NAME,
            data_type = db_type_name_for_datum(&datum_type),
            filter_columns = filter_columns,
            query = indent(&query, 4),
            timestamp_filter = timestamp_filter,
        )
    }
}

// Helper to nicely indent lines with the given number of spaces
fn indent(s: &str, count: usize) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    let prefix = " ".repeat(count);
    for (i, line) in s.split_terminator('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        if line.trim().is_empty() {
            continue;
        }
        out.push_str(&prefix);
        out.push_str(line);
    }
    if s.ends_with('\n') {
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    const PROJECT_IDS: &[&str] = &[
        "44292322-34ed-4568-8b1a-3b48c58a2801",
        "4ddf0fdf-850e-44b7-b07a-0e6550d8c41b",
    ];

    #[test]
    fn test_time_filter() {
        use chrono::TimeZone;
        let start = Utc.ymd(2021, 01, 01).and_hms_micro(01, 01, 01, 123456);
        let end = Utc.ymd(2021, 01, 02).and_hms_micro(01, 01, 01, 123456);

        let f = TimeFilter::Before(end);
        assert_eq!(
            f.as_where_fragment(),
            "timestamp <= '2021-01-02 01:01:01.123456'"
        );
        let f = TimeFilter::After(start);
        assert_eq!(
            f.as_where_fragment(),
            "timestamp >= '2021-01-01 01:01:01.123456'"
        );
        let f = TimeFilter::Between(start, end);
        assert_eq!(
            f.as_where_fragment(),
            "timestamp >= '2021-01-01 01:01:01.123456' AND timestamp <= '2021-01-02 01:01:01.123456'"
        );

        assert!(TimeFilter::from_timestamps(Some(start), Some(end)).is_ok());
        assert!(TimeFilter::from_timestamps(Some(end), Some(start)).is_err());
    }

    #[test]
    fn test_field_filter() {
        let filter = FieldFilter::new::<Uuid>(
            &String::from("project_id"),
            &["44292322-34ed-4568-8b1a-3b48c58a2801".parse().unwrap()],
        )
        .unwrap();
        assert_eq!(
            filter.as_where_fragment(),
            "field_name = 'project_id' AND ((field_value = '44292322-34ed-4568-8b1a-3b48c58a2801'))",
        );

        let filter = FieldFilter::new::<Uuid>(
            &String::from("project_id"),
            &PROJECT_IDS
                .iter()
                .map(|id| id.parse().unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(
            filter.as_where_fragment(),
            "field_name = 'project_id' AND \
            ((field_value = '44292322-34ed-4568-8b1a-3b48c58a2801') OR (field_value = '4ddf0fdf-850e-44b7-b07a-0e6550d8c41b'))",
        );

        assert!(matches!(
            FieldFilter::new::<i64>(&String::from("project_id"), &[]),
            Err(Error::QueryError(_)),
        ));

        let filter = FieldFilter::new(&String::from("cpu_id"), &[0]).unwrap();
        assert_eq!(
            filter.as_where_fragment(),
            "field_name = 'cpu_id' AND ((field_value = 0))"
        );
    }

    #[test]
    fn test_timeseries_filter() {
        let field_filters = vec![
            FieldFilter::new::<Uuid>(
                &String::from("project_id"),
                &PROJECT_IDS
                    .iter()
                    .map(|id| id.parse().unwrap())
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
            FieldFilter::new("cpu_id", &[0i64]).unwrap(),
        ];
        let filter = TimeseriesFilter {
            timeseries_name: String::from("virtual_machine:cpu_busy"),
            filters: field_filters,
            time_filter: Some(TimeFilter::Before(Utc::now())),
        };
        assert_eq!(filter.table_names()[0], "oximeter.fields_uuid");
        let query = filter.as_select_query(DatumType::F64);
        assert!(query.contains("AS filter0"));
        assert!(query.contains("AS filter1"));
        assert!(query
            .contains("ON filter1.timeseries_name = filter0.timeseries_name"));
        assert!(query
            .contains("AND filter1.timeseries_key = filter0.timeseries_key"));
        println!("{}", filter.as_select_query(DatumType::F64));
    }

    #[test]
    fn test_timeseries_filter_empty() {
        let filter = TimeseriesFilter {
            timeseries_name: String::from("virtual_machine:cpu_busy"),
            filters: Vec::new(),
            time_filter: None,
        };
        let query = filter.as_select_query(DatumType::F64);
        let expected = "SELECT * FROM oximeter.measurements_f64 \
            WHERE (timeseries_name) IN ('virtual_machine:cpu_busy') \
            ORDER BY (timeseries_name, timeseries_key, timestamp) \
            FORMAT JSONEachRow;"
            .replace(" ", "");
        assert_eq!(query.replace(|c| c == '\n' || c == ' ', ""), expected);
    }

    #[test]
    fn test_timeseries_filter_empty_field_filters() {
        let time = TimeFilter::Before(Utc::now());
        let filter = TimeseriesFilter {
            timeseries_name: String::from("virtual_machine:cpu_busy"),
            filters: Vec::new(),
            time_filter: Some(time),
        };
        let query = filter.as_select_query(DatumType::F64);
        let expected = format!(
            "SELECT * FROM oximeter.measurements_f64 \
            WHERE (timeseries_name) IN ('virtual_machine:cpu_busy') \
            AND {} \
            ORDER BY (timeseries_name, timeseries_key, timestamp) \
            FORMAT JSONEachRow;",
            time.as_where_fragment(),
        )
        .replace(" ", "");
        assert_eq!(query.replace(|c| c == '\n' || c == ' ', ""), expected,);
    }

    #[test]
    fn test_timeseries_filter_empty_time_filter() {
        let filter = TimeseriesFilter {
            timeseries_name: String::from("virtual_machine:cpu_busy"),
            filters: vec![FieldFilter::new("cpu_id", &[0i64]).unwrap()],
            time_filter: None,
        };
        let query = filter.as_select_query(DatumType::F64);
        let expected = "SELECT * FROM oximeter.measurements_f64 \
            WHERE (timeseries_name, timeseries_key) IN ( \
                SELECT timeseries_name, timeseries_key \
                FROM oximeter.fields_i64 \
                WHERE (field_name = 'cpu_id' AND ((field_value = 0)) AND \
                    (timeseries_name = 'virtual_machine:cpu_busy'))) \
            ORDER BY (timeseries_name, timeseries_key, timestamp) \
            FORMAT JSONEachRow;"
            .replace(" ", "");
        assert_eq!(query.replace(|c| c == '\n' || c == ' ', ""), expected,);
    }

    #[test]
    fn test_field_value_as_db_str() {
        assert_eq!(field_as_db_str(&FieldValue::from(false)), "0");
        assert_eq!(field_as_db_str(&FieldValue::from(true)), "1");
        assert_eq!(field_as_db_str(&FieldValue::from(10i64)), "10");
        assert_eq!(
            field_as_db_str(&FieldValue::IpAddr("127.0.0.1".parse().unwrap())),
            "'::ffff:127.0.0.1'"
        );
        assert_eq!(
            field_as_db_str(&FieldValue::Uuid(
                "563f0076-2c22-4510-8fd9-bed1ed8c9ae1".parse().unwrap()
            )),
            "'563f0076-2c22-4510-8fd9-bed1ed8c9ae1'"
        );
    }
}
