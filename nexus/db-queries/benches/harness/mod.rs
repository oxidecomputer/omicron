// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared Test Harness for benchmarking database queries
//!
//! This structure shares logic between benchmarks, making it easy
//! to perform shared tasks such as creating contention for reservations.

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::pub_test_utils::TestDatabase;
use nexus_db_queries::db::pub_test_utils::helpers::create_project;
use nexus_test_utils::sql::Row;
use nexus_test_utils::sql::process_rows;
use slog::Logger;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) mod db_utils;

use db_utils::*;

pub struct TestHarness {
    db: TestDatabase,
    authz_project: authz::Project,
}

struct ContentionQuery {
    sql: &'static str,
    description: &'static str,
}

const QUERIES: [ContentionQuery; 4] = [
    ContentionQuery {
        sql: "SELECT
            table_name, index_name, num_contention_events::TEXT
            FROM crdb_internal.cluster_contended_indexes",
        description: "Indexes which are experiencing contention",
    },
    ContentionQuery {
        sql: "SELECT
            table_name,num_contention_events::TEXT
            FROM crdb_internal.cluster_contended_tables",
        description: "Tables which are experiencing contention",
    },
    ContentionQuery {
        sql: "WITH c AS
            (SELECT DISTINCT ON (table_id, index_id)
                table_id,
                index_id,
                num_contention_events AS events,
                cumulative_contention_time AS time
            FROM crdb_internal.cluster_contention_events)
            SELECT
                i.descriptor_name as table_name,
                i.index_name,
                c.events::TEXT,
                c.time::TEXT FROM crdb_internal.table_indexes AS i
            JOIN c ON i.descriptor_id = c.table_id AND i.index_id = c.index_id
            ORDER BY c.time DESC LIMIT 10;",
        description: "Top ten longest contention events, grouped by table + index",
    },
    ContentionQuery {
        // See: https://www.cockroachlabs.com/docs/v22.1/crdb-internal#example
        // for the source here
        sql: "SELECT DISTINCT
  hce.blocking_statement,
  substring(ss2.metadata ->> 'query', 1, 120) AS waiting_statement,
  hce.contention_count::TEXT
FROM (SELECT
        blocking_txn_fingerprint_id,
        waiting_txn_fingerprint_id,
        contention_count,
        substring(ss.metadata ->> 'query', 1, 120) AS blocking_statement
      FROM (SELECT
              encode(blocking_txn_fingerprint_id, 'hex') as blocking_txn_fingerprint_id,
              encode(waiting_txn_fingerprint_id, 'hex') as waiting_txn_fingerprint_id,
              count(*) AS contention_count
            FROM
              crdb_internal.transaction_contention_events
            GROUP BY
              blocking_txn_fingerprint_id, waiting_txn_fingerprint_id
            ),
          crdb_internal.statement_statistics ss
      WHERE
        blocking_txn_fingerprint_id = encode(ss.transaction_fingerprint_id, 'hex')) hce,
      crdb_internal.statement_statistics ss2
WHERE
  hce.blocking_txn_fingerprint_id != '0000000000000000' AND
  hce.waiting_txn_fingerprint_id != '0000000000000000' AND
  hce.waiting_txn_fingerprint_id = encode(ss2.transaction_fingerprint_id, 'hex')
ORDER BY
  contention_count
DESC;",
        description: "Transaction statements which are blocking other statements",
    }
];

impl TestHarness {
    pub async fn new(log: &Logger, sled_count: usize) -> Self {
        let db = TestDatabase::new_with_datastore(log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let (authz_project, _project) =
            create_project(&opctx, &datastore, "project").await;
        create_sleds(&datastore, sled_count).await;

        Self { db, authz_project }
    }

    /// Emit internal CockroachDb information about contention
    pub async fn print_contention(&self) {
        let client =
            self.db.crdb().connect().await.expect("Failed to connect to db");

        // Used for padding: get a map of "column name" -> "max value length".
        let max_lengths_by_column = |rows: &Vec<Row>| {
            let mut lengths = HashMap::new();
            for row in rows {
                for column in &row.values {
                    let value_len = column.value().unwrap().as_str().len();
                    let name_len = column.name().len();
                    let len = std::cmp::max(value_len, name_len);

                    lengths
                        .entry(column.name().to_string())
                        .and_modify(|entry| {
                            if len > *entry {
                                *entry = len;
                            }
                        })
                        .or_insert(len);
                }
            }
            lengths
        };

        for ContentionQuery { sql, description } in QUERIES {
            let rows = client
                .query(sql, &[])
                .await
                .expect("Failed to query contended tables");
            let rows = process_rows(&rows);
            if rows.is_empty() {
                continue;
            }

            println!("{description}");
            let max_lengths = max_lengths_by_column(&rows);
            let mut header = true;

            for row in rows {
                if header {
                    let mut total_len = 0;
                    for column in &row.values {
                        let width = max_lengths.get(column.name()).unwrap();
                        print!(" {:width$} ", column.name());
                        print!("|");
                        total_len += width + 3;
                    }
                    println!("");
                    println!("{:-<total_len$}", "");
                }
                header = false;

                for column in row.values {
                    let width = max_lengths.get(column.name()).unwrap();
                    let value = column.value().unwrap().as_str();
                    print!(" {value:width$} ");
                    print!("|");
                }
                println!("");
            }
            println!("");
        }

        client.cleanup().await.expect("Failed to clean up db connection");
    }

    pub fn authz_project(&self) -> authz::Project {
        self.authz_project.clone()
    }

    /// Returns an owned reference to OpContext
    pub fn opctx(&self) -> Arc<OpContext> {
        Arc::new(self.db.opctx().child(std::collections::BTreeMap::new()))
    }

    /// Returns an owned reference to the datastore
    pub fn db(&self) -> Arc<DataStore> {
        self.db.datastore().clone()
    }

    pub async fn terminate(self) {
        self.db.terminate().await;
    }
}
