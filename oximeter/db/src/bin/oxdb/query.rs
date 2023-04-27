// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Methods for querying the ClickHouse timeseries database.

use crate::Client;
use oximeter::types::FieldType;
use oximeter_db::FieldSchema;
use oximeter_db::FieldSource;
use oximeter_db::TimeseriesMetadata;
use oximeter_db::TimeseriesName;
use oximeter_db::TimeseriesPage;
use oximeter_db::Timestamp;
use std::num::NonZeroU32;
use tabled::Table;
use tabled::Tabled;

#[derive(Tabled)]
struct Fields {
    name: String,
    #[tabled(rename = "type")]
    ty: FieldType,
    source: FieldSource,
}

impl From<FieldSchema> for Fields {
    fn from(schema: FieldSchema) -> Self {
        Self { name: schema.name, ty: schema.ty, source: schema.source }
    }
}

/// Run a query against the database.
pub async fn run_query(
    client: &Client,
    timeseries_name: &TimeseriesName,
    filters: Vec<String>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<(), anyhow::Error> {
    let limit = NonZeroU32::new(100).unwrap();
    let mut scan = client
        .setup_timeseries_scan(timeseries_name, &filters, start, end, limit)
        .await?;
    let mut current_metadata = None;
    loop {
        let TimeseriesPage { timeseries, next_page } =
            client.compatible_timeseries_list(scan).await?;
        if timeseries.is_empty() {
            break;
        }
        let mut timeseries = timeseries.into_iter();
        while let Some(next) = timeseries.next() {
            if let Some(current) = &current_metadata {
                if !next.matches_metadata(current) {
                    println!("--> {:?}", next.metadata);
                    current_metadata.replace(next.metadata.clone());
                }
            } else {
                println!("--> {:?}", next.metadata);
                current_metadata.replace(next.metadata.clone());
            }
            for measurement in next.measurements {
                println!("{measurement:?}");
            }
        }
        match next_page {
            None => break,
            Some(s) => scan = s,
        }
    }

    Ok(())
}

fn print_metadata_header(md: &TimeseriesMetadata) {
    println!("Name: \"{}\"", md.timeseries_name);
    for (field, source) in md
        .target
        .fields
        .iter()
        .zip(std::iter::repeat('T'))
        .chain(md.metric.fields.iter().zip(std::iter::repeat('M')))
    {
        let name = &field.name;
        let ty = field.value.field_type();
        print!("{name} [{ty} / {source}] ");
    }
}

fn print_metadata_row(md: &TimeseriesMetadata) {
    for field in md.target.fields.iter().chain(md.metric.fields.iter()) {
        print!("{} ", field.value);
    }
}

/// List and print all timeseries schema in the database.
pub async fn list_schema(client: &Client) -> anyhow::Result<()> {
    use futures::stream::TryStreamExt;
    let mut stream = client.stream_timeseries_schema().await;
    while let Some(schema) = stream.try_next().await? {
        let mut table =
            Table::new(schema.field_schema.into_iter().map(Fields::from));
        table.with(tabled::settings::Style::psql());
        println!(
            "Name: {}\nCreated: {}\nFields:\n{}",
            schema.timeseries_name, schema.created, table,
        );
        println!("");
    }
    Ok(())
}

/// List and print all timeseries compatible with the set of filters.
pub async fn list_compatible_timeseries(
    client: &Client,
    timeseries_name: &str,
    filters: &[String],
) -> anyhow::Result<()> {
    use futures::stream::TryStreamExt;
    let name = timeseries_name.try_into()?;
    let mut stream =
        client.stream_compatible_timeseries_metadata(&name, filters).await;
    if let Some(md) = stream.try_next().await? {
        print_metadata_header(&md);
        println!();
        print_metadata_row(&md);
        println!();
    }
    while let Some(md) = stream.try_next().await? {
        print_metadata_row(&md);
        println!();
    }
    Ok(())
}
