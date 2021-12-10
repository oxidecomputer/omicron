#!/usr/sbin/dtrace -qZs

#pragma strsize=4k

/* Print out the slowest query every 10 seconds. */

struct query_info {
    string conn_id;
    string query;
    uint64_t latency;
};

struct query_info slowest_query;
self struct query_info queries[string];

dtrace:::BEGIN
{
    printf("Tracing slowest queries for nexus PID %d, use Ctrl-C to exit\n", $target);
}

diesel_db$target:::query_start
{
    this->conn_id = json(copyinstr(arg1), "ok");
    self->queries[this->conn_id].conn_id = json(copyinstr(arg1), "ok");
    self->queries[this->conn_id].query = copyinstr(arg2);
    self->queries[this->conn_id].latency = timestamp;
}


diesel_db$target:::query_end
/self->queries[json(copyinstr(arg1), "ok")].latency != 0/
{
    this->conn_id = json(copyinstr(arg1), "ok");
    this->latency = timestamp - self->queries[this->conn_id].latency;
    if (this->latency > slowest_query.latency) {
        slowest_query.conn_id = self->queries[this->conn_id].conn_id;
        slowest_query.query = self->queries[this->conn_id].query;
        slowest_query.latency = this->latency;
    }
}

tick-10s
{
    printf("%Y\n", walltimestamp);
    if (strlen(slowest_query.query) > 0) {
        printf("latency: %lu us, conn_id: %s, query: '%s'\n", slowest_query.latency / 1000, slowest_query.conn_id, slowest_query.query);
        slowest_query.query = "";
        slowest_query.conn_id = "";
        slowest_query.latency = 0;
    } else {
        printf("No new queries\n");
    }
}
