// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db whatis` subcommand
//!
//! Heuristically determine what type of object a given UUID refers to

use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use clap::Args;
use nexus_db_queries::db::DataStore;
use uuid::Uuid;

#[derive(Debug, Args, Clone)]
pub(super) struct WhatisArgs {
    /// The UUID(s) to look up
    uuids: Vec<Uuid>,

    /// Show all tables that were checked
    #[clap(long)]
    debug: bool,
}

// `omdb db whatis UUID...` heuristically determines what type of object a UUID
// refers to by first searching the database for unique UUID columns and then
// searching those tables for a matching row.
pub(super) async fn cmd_db_whatis(
    datastore: &DataStore,
    args: &WhatisArgs,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    // Query CockroachDB's information schema to find UUID columns that are the
    // sole column in a unique constraint or primary key with no associated
    // WHERE clause (i.e., not a partial index) . This ensures we only query
    // columns that have their own index (avoiding table scans).
    //
    // We use this approach rather than hardcoding all the kinds of objects that
    // we know about so that this stays up-to-date with other changes to the
    // system.
    let query = "
        WITH constraint_column_counts AS (
            SELECT
                constraint_catalog,
                constraint_schema,
                constraint_name,
                table_catalog,
                table_schema,
                table_name,
                COUNT(*) as column_count
            FROM
                information_schema.key_column_usage
            GROUP BY
                constraint_catalog,
                constraint_schema,
                constraint_name,
                table_catalog,
                table_schema,
                table_name
        )
        SELECT DISTINCT
            tc.table_name,
            kcu.column_name
        FROM
            information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_catalog = kcu.constraint_catalog
                AND tc.constraint_schema = kcu.constraint_schema
                AND tc.constraint_name = kcu.constraint_name
                AND tc.table_catalog = kcu.table_catalog
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            JOIN information_schema.columns c
                ON kcu.table_catalog = c.table_catalog
                AND kcu.table_schema = c.table_schema
                AND kcu.table_name = c.table_name
                AND kcu.column_name = c.column_name
            JOIN constraint_column_counts ccc
                ON tc.constraint_catalog = ccc.constraint_catalog
                AND tc.constraint_schema = ccc.constraint_schema
                AND tc.constraint_name = ccc.constraint_name
                AND tc.table_catalog = ccc.table_catalog
                AND tc.table_schema = ccc.table_schema
                AND tc.table_name = ccc.table_name
            LEFT JOIN pg_indexes idx
                ON idx.tablename = tc.table_name
                AND idx.indexname = tc.constraint_name
                AND idx.schemaname = tc.table_schema
        WHERE
            tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
            AND tc.table_schema = 'public'
            AND c.data_type = 'uuid'
            AND ccc.column_count = 1
            AND (idx.indexdef IS NULL OR idx.indexdef NOT LIKE '% WHERE %')
        ORDER BY
            tc.table_name, kcu.column_name
    ";

    #[derive(Debug, diesel::QueryableByName)]
    struct UniqueUuidColumn {
        #[diesel(sql_type = diesel::sql_types::Text)]
        table_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        column_name: String,
    }

    let unique_columns: Vec<UniqueUuidColumn> = diesel::sql_query(query)
        .load_async(&*conn)
        .await
        .context("querying information_schema for unique UUID columns")?;

    // Search separately for each UUID provided on the command line.
    for uuid in &args.uuids {
        let mut found = false;

        // For each unique UUID column that we found above, see if there's a row
        // in the corresponding table for the given UUID value.
        for col in &unique_columns {
            let check_query = format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE {} = $1) as exists",
                col.table_name, col.column_name
            );

            let table_column =
                format!("{}.{}", col.table_name, col.column_name);
            if args.debug {
                eprintln!("checking table {:?}", table_column);
            }

            // `sql_query()` requires extracting the results with a struct.
            #[derive(Debug, diesel::QueryableByName)]
            struct ExistsResult {
                #[diesel(sql_type = diesel::sql_types::Bool)]
                exists: bool,
            }

            let exists_result: ExistsResult = diesel::sql_query(&check_query)
                .bind::<diesel::sql_types::Uuid, _>(*uuid)
                .get_result_async(&*conn)
                .await
                .with_context(|| {
                    format!("checking {} for UUID {}", table_column, *uuid)
                })?;

            if exists_result.exists {
                println!("{} found in {}", *uuid, table_column);
                found = true;
            }
        }

        if !found {
            println!("{} not found in any unique UUID column", uuid);
        }
    }

    Ok(())
}
