// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Methods for querying the ClickHouse timeseries database.

use crate::Client;
use dropshot::EmptyScanParams;
use dropshot::WhichPage;
use oximeter_db::query::Timestamp;
use oximeter_db::TimeseriesSchema;

/// Run a query against the database.
pub async fn run_query(
    client: &Client,
    timeseries_name: String,
    filters: Vec<String>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<(), anyhow::Error> {
    let filters = filters.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let timeseries = client
        .select_timeseries_with(
            &timeseries_name,
            filters.as_slice(),
            start,
            end,
            None,
        )
        .await?;
    println!("{}", serde_json::to_string(&timeseries).unwrap());
    Ok(())
}

// Print a single timeseries schema.
fn print_schema(schema: &TimeseriesSchema) {
    println!("Timeseries \"{}\"", &schema.timeseries_name);
    println!(" Created: {}", schema.created);
    for (i, field) in schema.field_schema.iter().enumerate() {
        println!(" Field {}", i);
        println!("  Name: \"{}\"", field.name);
        println!("  Type: {}", field.ty);
    }
}

/// List and print all timeseries schema in the database.
pub async fn list_schema(client: &Client) -> anyhow::Result<()> {
    let mut page = WhichPage::First(EmptyScanParams {});
    let count = std::num::NonZeroU32::new(100).unwrap();
    loop {
        let results = client.timeseries_schema_list(&page, count).await?;
        if results.items.is_empty() {
            break;
        }
        for schema in results.items.iter() {
            print_schema(schema);
        }
        match results.next_page {
            None => break,
            Some(_) => {
                page = WhichPage::Next(
                    results.items.last().unwrap().timeseries_name.clone(),
                );
            }
        }
    }
    Ok(())
}
