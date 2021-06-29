//! Functions for querying the timeseries database.
// Copyright 2021 Oxide Computer Company

use std::net::IpAddr;

use chrono::{DateTime, Utc};

use crate::db::model::DATABASE_NAME;
use crate::types::{FieldValue, MeasurementType};
use crate::Error;

impl FieldValue {
    // Format the value for use as a value in a query to the database, e.g., `... WHERE
    // (field_value = {})`.
    pub(crate) fn as_db_str(&self) -> String {
        match self {
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
}

impl MeasurementType {
    pub(crate) fn as_db_str(&self) -> &str {
        match self {
            MeasurementType::Bool => "bool",
            MeasurementType::I64 => "i64",
            MeasurementType::F64 => "f64",
            MeasurementType::String => "string",
            MeasurementType::Bytes => "bytes",
            MeasurementType::CumulativeI64 => "cumulativei64",
            MeasurementType::CumulativeF64 => "cumulativef64",
            MeasurementType::HistogramI64 => "histogrami64",
            MeasurementType::HistogramF64 => "histogramf64",
        }
    }
}

/// Object used to filter timestamps
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
}

/// A `FieldFilter` specifies a field by name and one or more values to compare against by
/// equality.
#[derive(Debug, Clone)]
pub struct FieldFilter {
    field_name: String,
    field_values: Vec<FieldValue>,
}

impl FieldFilter {
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

    fn as_where_fragment(&self, time_filter: Option<TimeFilter>) -> String {
        let field_value_fragment = self
            .field_values
            .iter()
            .map(|field| format!("(field_value = {})", field.as_db_str()))
            .collect::<Vec<_>>()
            .join(" OR ");
        let time_filter_fragment = if let Some(filt) = time_filter {
            format!(" AND {}", filt.as_where_fragment())
        } else {
            String::new()
        };
        format!(
            "field_name = '{field_name}' AND ({value_filter}){time_filter}",
            field_name = self.field_name,
            value_filter = field_value_fragment,
            time_filter = time_filter_fragment
        )
    }
}

/// Object used to filter timeseries
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
                    "{timeseries_key},\n",
                    "{timestamp}\n",
                    "FROM {table_name}\n",
                    "WHERE ({where_fragment})",
                ),
                timeseries_key = indent("timeseries_key", 4),
                timestamp = indent("timestamp", 4),
                table_name = table_name,
                where_fragment = filter.as_where_fragment(self.time_filter),
            )
        };
        self.table_names()
            .iter()
            .zip(self.filters())
            .map(select_query)
            .collect()
    }

    /// Generate a select query for this filter
    pub fn as_select_query(
        &self,
        measurement_type: MeasurementType, // TODO remove
    ) -> String {
        let select_queries = self.field_select_queries();
        let query = if select_queries.len() == 1 {
            // We only have one subquery, just use it directly
            select_queries[0].clone()
        } else {
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
                            " ON filter{i}.timeseries_key = filter{j}.timeseries_key \
                            AND filter{i}.timestamp = filter{j}.timestamp",
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
            format!(
                "SELECT\n\
                {timeseries_key},\n\
                {timestamp}\n\
                FROM\n\
                {subqueries}",
                timeseries_key = indent("filter0.timeseries_key", 4),
                timestamp = indent("filter0.timestamp", 4),
                subqueries = subqueries,
            )
        };

        // Format the top-level query
        format!(
            "SELECT *\n\
            FROM {db_name}.measurements_{data_type}\n\
            WHERE (timeseries_key, timestamp) IN (\n\
            {query}\n\
            ) FORMAT JSONEachRow;",
            db_name = DATABASE_NAME,
            data_type = measurement_type.as_db_str(),
            query = indent(&query, 4),
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
    }

    #[test]
    fn test_field_filter() {
        let filter = FieldFilter::new::<Uuid>(
            &String::from("project_id"),
            &["44292322-34ed-4568-8b1a-3b48c58a2801".parse().unwrap()],
        )
        .unwrap();
        assert_eq!(
            filter.as_where_fragment(None),
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
            filter.as_where_fragment(None),
            "field_name = 'project_id' AND \
            ((field_value = '44292322-34ed-4568-8b1a-3b48c58a2801') OR (field_value = '4ddf0fdf-850e-44b7-b07a-0e6550d8c41b'))",
        );

        assert!(matches!(
            FieldFilter::new::<i64>(&String::from("project_id"), &[]),
            Err(Error::QueryError(_)),
        ));

        let filter = FieldFilter::new(&String::from("cpu_id"), &[0]).unwrap();
        assert_eq!(
            filter.as_where_fragment(None),
            "field_name = 'cpu_id' AND ((field_value = 0))"
        );

        let start = "2021-01-01 00:00:00.123456Z";
        let end = "2021-01-02 00:00:00.123456Z";
        let time_filter = TimeFilter::After(start.parse().unwrap());
        assert_eq!(
            filter.as_where_fragment(Some(time_filter)),
            format!(
                "field_name = 'cpu_id' AND ((field_value = 0)) AND timestamp >= '{}'",
                start.strip_suffix('Z').unwrap()
            ),
        );

        let time_filter = TimeFilter::Before(end.parse().unwrap());
        assert_eq!(
            filter.as_where_fragment(Some(time_filter)),
            format!(
                "field_name = 'cpu_id' AND ((field_value = 0)) AND timestamp <= '{}'",
                end.strip_suffix('Z').unwrap()
            ),
        );

        let time_filter =
            TimeFilter::Between(start.parse().unwrap(), end.parse().unwrap());
        assert_eq!(
            filter.as_where_fragment(Some(time_filter)),
            format!(
                "field_name = 'cpu_id' AND ((field_value = 0)) AND timestamp >= '{}' AND timestamp <= '{}'",
                start.strip_suffix('Z').unwrap(),
                end.strip_suffix('Z').unwrap()
            ),
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
        let query = filter.as_select_query(MeasurementType::F64);
        assert!(query.contains("AS filter0"));
        assert!(query.contains("AS filter1"));
        assert!(query
            .contains("ON filter1.timeseries_key = filter0.timeseries_key"));
        println!("{}", filter.as_select_query(MeasurementType::F64));
    }

    #[test]
    fn test_field_value_as_db_str() {
        assert_eq!(FieldValue::from(false).as_db_str(), "0");
        assert_eq!(FieldValue::from(true).as_db_str(), "1");
        assert_eq!(FieldValue::from(10i64).as_db_str(), "10");
        assert_eq!(
            FieldValue::IpAddr("127.0.0.1".parse().unwrap()).as_db_str(),
            "'::ffff:127.0.0.1'"
        );
        assert_eq!(
            FieldValue::Uuid(
                "563f0076-2c22-4510-8fd9-bed1ed8c9ae1".parse().unwrap()
            )
            .as_db_str(),
            "'563f0076-2c22-4510-8fd9-bed1ed8c9ae1'"
        );
    }
}
