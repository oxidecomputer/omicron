// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper for the OxQL and SQL shell implementations.

// Copyright 2024 Oxide Computer Company

use crate::Client;
use dropshot::EmptyScanParams;
use dropshot::WhichPage;
use oximeter::TimeseriesSchema;

#[cfg(any(feature = "oxql", test))]
pub mod oxql;
#[cfg(any(feature = "sql", test))]
pub mod sql;

/// Special identifiers for column names or other widely-used values.
pub mod special_idents {
    use oximeter::DatumType;

    macro_rules! gen_marker {
        ($p:expr, $field:expr) => {
            concat!("p", $p, "_", $field)
        };
    }

    pub const TIMESTAMP: &str = "timestamp";
    pub const START_TIME: &str = "start_time";
    pub const DATUM: &str = "datum";
    pub const BINS: &str = "bins";
    pub const COUNTS: &str = "counts";
    pub const MIN: &str = "min";
    pub const MAX: &str = "max";
    pub const SUM_OF_SAMPLES: &str = "sum_of_samples";
    pub const SQUARED_MEAN: &str = "squared_mean";
    pub const DATETIME64: &str = "DateTime64";
    pub const ARRAYU64: &str = "Array[u64]";
    pub const ARRAYFLOAT64: &str = "Array[f64]";
    pub const ARRAYINT64: &str = "Array[i64]";
    pub const FLOAT64: &str = "f64";
    pub const UINT64: &str = "u64";

    /// Distribution identifiers.
    pub const DISTRIBUTION_IDENTS: [&str; 15] = [
        "bins",
        "counts",
        "min",
        "max",
        "sum_of_samples",
        "squared_mean",
        gen_marker!("50", "marker_heights"),
        gen_marker!("50", "marker_positions"),
        gen_marker!("50", "desired_marker_positions"),
        gen_marker!("90", "marker_heights"),
        gen_marker!("90", "marker_positions"),
        gen_marker!("90", "desired_marker_positions"),
        gen_marker!("99", "marker_heights"),
        gen_marker!("99", "marker_positions"),
        gen_marker!("99", "desired_marker_positions"),
    ];

    /// Get the array type name for a histogram type.
    pub fn array_type_name_from_histogram_type(
        type_: DatumType,
    ) -> Option<String> {
        if !type_.is_histogram() {
            return None;
        }
        Some(format!(
            "Array[{}]",
            type_.to_string().strip_prefix("Histogram").unwrap().to_lowercase(),
        ))
    }
}

/// List the known timeseries.
pub async fn list_timeseries(client: &Client) -> anyhow::Result<()> {
    let mut page = WhichPage::First(EmptyScanParams {});
    let limit = 100.try_into().unwrap();
    loop {
        let results = client.timeseries_schema_list(&page, limit).await?;
        for schema in results.items.iter() {
            println!("{}", schema.timeseries_name);
        }
        if results.next_page.is_some() {
            if let Some(last) = results.items.last() {
                page = WhichPage::Next(last.timeseries_name.clone());
            } else {
                return Ok(());
            }
        } else {
            return Ok(());
        }
    }
}

/// Describe a single timeseries.
pub async fn describe_timeseries(
    client: &Client,
    timeseries: &str,
) -> anyhow::Result<()> {
    match timeseries.parse() {
        Err(_) => eprintln!(
            "Invalid timeseries name '{timeseries}, \
            use \\l to list available timeseries by name
        "
        ),
        Ok(name) => {
            if let Some(schema) = client.schema_for_timeseries(&name).await? {
                let (cols, types) = prepare_columns(&schema);
                let mut builder = tabled::builder::Builder::default();
                builder.push_record(cols); // first record is the header
                builder.push_record(types);
                println!(
                    "{}",
                    builder.build().with(tabled::settings::Style::psql())
                );
            } else {
                eprintln!("No such timeseries: {timeseries}");
            }
        }
    }
    Ok(())
}

/// Prepare the columns for a timeseries or virtual table.
pub(crate) fn prepare_columns(
    schema: &TimeseriesSchema,
) -> (Vec<String>, Vec<String>) {
    let mut cols = Vec::with_capacity(schema.field_schema.len() + 2);
    let mut types = cols.clone();

    for field in schema.field_schema.iter() {
        cols.push(field.name.clone());
        types.push(field.field_type.to_string());
    }

    cols.push(special_idents::TIMESTAMP.into());
    types.push(special_idents::DATETIME64.into());

    if schema.datum_type.is_histogram() {
        cols.push(special_idents::START_TIME.into());
        types.push(special_idents::DATETIME64.into());

        cols.push(special_idents::BINS.into());
        types.push(
            special_idents::array_type_name_from_histogram_type(
                schema.datum_type,
            )
            .unwrap(),
        );

        cols.push(special_idents::COUNTS.into());
        types.push(special_idents::ARRAYU64.into());

        cols.push(special_idents::MIN.into());
        types.push(special_idents::FLOAT64.into());

        cols.push(special_idents::MAX.into());
        types.push(special_idents::FLOAT64.into());

        cols.push(special_idents::SUM_OF_SAMPLES.into());
        types.push(special_idents::UINT64.into());

        cols.push(special_idents::SQUARED_MEAN.into());
        types.push(special_idents::UINT64.into());

        for quantile in ["P50", "P90", "P99"].iter() {
            cols.push(format!("{}_MARKER_HEIGHTS", quantile));
            types.push(special_idents::ARRAYFLOAT64.into());
            cols.push(format!("{}_MARKER_POSITIONS", quantile));
            types.push(special_idents::ARRAYINT64.into());
            cols.push(format!("{}_DESIRED_MARKER_POSITIONS", quantile));
            types.push(special_idents::ARRAYFLOAT64.into());
        }
    } else if schema.datum_type.is_cumulative() {
        cols.push(special_idents::START_TIME.into());
        types.push(special_idents::DATETIME64.into());
        cols.push(special_idents::DATUM.into());
        types.push(schema.datum_type.to_string());
    } else {
        cols.push(special_idents::DATUM.into());
        types.push(schema.datum_type.to_string());
    }

    (cols, types)
}
