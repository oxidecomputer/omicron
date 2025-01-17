#!/usr/sbin/dtrace -qs

/* Trace all transactions to the control plane database with their latency */

dtrace:::BEGIN
{
    printf("Tracing all database transactions for nexus PID %d, use Ctrl-C to exit\n", $target);
}

/*
 * Record the start and number of statements for each transaction.
 *
 * Note that we're using the Nexus-provided transaction start / done probes.
 * This lets us associate the other data we might collect (number of statements,
 * ustacks, etc) with the Nexus code itself. Although there are transaction
 * start / done probes in `diesel-dtrace`, the existing way we run transactions
 * with `async-bb8-diesel` involves spawning a future to run the transactions on
 * a blocking thread-pool. That spawning makes it impossible to associate the
 * context in which the `diesel-dtrace` probes fire with the Nexus code that
 * actually spawned the transaction itself.
 */
nexus_db_queries$target:::transaction-start
{
    this->key = copyinstr(arg0);
    transaction_names[this->key] = copyinstr(arg1);
    ts[this->key] = timestamp;
    n_statements[this->key] = 0;
    printf(
        "Started transaction '%s' on conn %s\n",
        transaction_names[this->key],
        json(this->key, "ok")
    );
}

/*
 * When a query runs in the context of a transaction (on the same connection),
 * bump the statement counter.
 */
diesel_db$target:::query-start
/ts[copyinstr(arg1)]/
{
    n_statements[copyinstr(arg1)] += 1
}

/*
 * As transactions complete, print the number of statements we ran and the
 * duration.
 */
nexus_db_queries$target:::transaction-done
/ts[copyinstr(arg0)]/
{
    this->key = copyinstr(arg0);
    this->conn_id = json(this->key, "ok");
    this->latency = (timestamp - ts[this->key]) / 1000;
    this->n_statements = n_statements[this->key];
    printf(
        "%s %d statement(s) in transaction '%s' on connection %s (%d us)\n",
        arg2 ? "COMMIT" : "ROLLBACK",
        n_statements[this->key],
        transaction_names[this->key],
        this->conn_id,
        this->latency
    );
    ts[this->key] = 0;
    n_statements[this->key] = 0;
    transaction_names[this->key] = 0;
}
