#!/usr/sbin/dtrace -qs

/* Trace all transactions to the control plane database with their latency */

dtrace:::BEGIN
{
    printf("Tracing all database transactions for nexus PID %d, use Ctrl-C to exit\n", $target);
}

/*
 * Record the start and number of statements for each transaction.
 *
 * We're only recording the initial transactions, which are not nested.
 */
diesel_db$target:::transaction-start
/arg1 == 0/
{
    this->key = copyinstr(arg0);
    ts[this->key] = timestamp;
    n_statements[this->key] = 0;
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
 *
 * NOTE: The depth, arg1, is intentionally 1 here. When we first enter a
 * non-nested transaction, the depth is 0, because there is no outer
 * transaction. However, when the `transaction-done` probe fires, we are still
 * in a transaction, in which case the depth is currently 1. It's decremented
 * _after_ the transaction itself is committed or rolled back.
 */
diesel_db$targe:::transaction-done
/arg1 == 1 && ts[copyinstr(arg0)]/
{
    this->key = copyinstr(arg0);
    this->conn_id = json(this->key, "ok");
    this->latency = (timestamp - ts[this->key]) / 1000;
    this->n_statements = n_statements[this->key];
    printf(
        "%s %d statements on connection %s (%d us)\n",
        arg2 ? "COMMIT" : "ROLLBACK",
        n_statements[this->key],
        this->conn_id,
        this->latency
    );
    ts[this->key] = 0;
    n_statements[this->key] = 0;
}
