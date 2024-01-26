#!/usr/sbin/dtrace -qs

#pragma D option strsize=4k

/* Print out the slowest 5 queries every 10 seconds. */

dtrace:::BEGIN
{
    printf("Tracing slowest queries for nexus PID %d, use Ctrl-C to exit\n", $target);
}

diesel_db$target:::query-start
{
    this->conn_id = json(copyinstr(arg1), "ok");
    ts[this->conn_id] = timestamp;
    query[this->conn_id] = copyinstr(arg2);
}


diesel_db$target:::query-done
{
    this->conn_id = json(copyinstr(arg1), "ok");
}

diesel_db$target:::query-done
/ts[this->conn_id]/
{
    this->latency = timestamp - ts[this->conn_id];
    /* There's only a single value here, so `max` is just used to "convert"
     * the latency as an integer to an aggregation. `min`, or any other scalar
     * aggregation function, would be equivalent.
     */
    @[this->conn_id, query[this->conn_id]] = max(this->latency);
    ts[this->conn_id] = 0;
    query[this->conn_id] = NULL;
}

tick-5s
{
    printf("\n%Y\n", walltimestamp);
    trunc(@, 5);
    normalize(@, 1000);
    printa("latency: %@ld us, conn_id: %s, query: '%s'\n", @);
    trunc(@);
}
