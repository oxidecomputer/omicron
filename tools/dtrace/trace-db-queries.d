#!/usr/sbin/dtrace -qs

#pragma D option strsize=16k

/* Trace all queries to the control plane database with their latency */

dtrace:::BEGIN
{
    printf("Tracing all database queries for nexus PID %d, use Ctrl-C to exit\n", $target);
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
    this->latency = (timestamp - ts[this->conn_id]) / 1000;
    printf(
        "conn_id: %s, latency: %lu us, query: '%s'\n",
        this->conn_id,
        this->latency,
        query[this->conn_id]
    );
    ts[this->conn_id] = 0;
    query[this->conn_id] = NULL;
}
