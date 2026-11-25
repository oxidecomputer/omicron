// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::test_util::LogContext;
use nexus_test_utils::db;
use omicron_test_utils::dev::db::CockroachInstance;
use omicron_test_utils::dev::test_setup_log;

struct Harness {
    logctx: LogContext,
    crdb: CockroachInstance,
}

impl Harness {
    async fn new(name: &str) -> Self {
        let logctx = test_setup_log(name);
        let log = &logctx.log;
        let crdb = db::test_setup_database_empty(log).await;
        Self { logctx, crdb }
    }

    async fn cleanup(mut self) {
        self.crdb.cleanup().await.unwrap();
        self.logctx.cleanup_successful();
    }
}

#[tokio::test]
async fn crdb_serialization() {
    let harness = Harness::new("crdb_serialization").await;

    let conn1 = harness.crdb.connect().await.unwrap();
    let conn2 = harness.crdb.connect().await.unwrap();

    // Setup: Create a table with a couple entries.
    //
    // - "n" acts as a generation number
    // - "name" acts as a label.
    conn1.batch_execute("
        CREATE DATABASE db;
        CREATE TABLE db.public.versions (id UUID PRIMARY KEY, n integer, name TEXT);
        INSERT INTO db.public.versions (id, n, name) values (gen_random_uuid(), 1, 'first');
        INSERT INTO db.public.versions (id, n, name) values (gen_random_uuid(), 2, 'second');
    ").await.unwrap();

    // On "conn2", we work in a transaction.
    // Read the latest generation number
    conn2
        .batch_execute(
            "
        BEGIN;
        SELECT * FROM db.public.versions ORDER BY n DESC LIMIT 1;
    ",
        )
        .await
        .unwrap();

    // Concurrently, on "conn1", we insert a newer generation number.
    conn1.batch_execute("
        INSERT INTO db.public.versions (id, n, name) values (gen_random_uuid(), 3, 'outside txn');
    ").await.unwrap();

    // Back on "conn2", we use "the last gen we saw, + 1", which should be 3.
    //
    // This is "bad", because someone concurrently inserted this number outside
    // the transaction. Do we catch it with a serialization error?
    conn2.batch_execute("
        INSERT INTO db.public.versions (id, n, name) values (gen_random_uuid(), 3, 'inside txn');
        COMMIT;
    ").await.unwrap();

    let rows = conn1
        .query("SELECT * FROM db.public.versions ORDER BY n ASC", &[])
        .await
        .unwrap();
    for row in rows {
        let n: i64 = row.get("n");
        let name: String = row.get("name");
        eprintln!("n: {n}, name: {name}");
    }

    harness.cleanup().await;
}
