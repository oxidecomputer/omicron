#!/usr/sbin/dtrace -qs

/* Aggregate database query latency by connection, printing every 10 seconds. */

dtrace:::BEGIN
{
    printf("Tracing database query latency by connection ID for nexus PID %d, use Ctrl-C to exit\n", $target);
}

diesel_db$target:::query-start
{
    @total_queries = count();
    this->conn_id = json(copyinstr(arg1), "ok");
    self->ts[this->conn_id] = timestamp;
}

diesel_db$target:::query-done
/self->ts[json(copyinstr(arg1), "ok")] != 0/
{
    this->conn_id = json(copyinstr(arg1), "ok");
    @latency[this->conn_id] = quantize((timestamp - self->ts[this->conn_id]) / 1000);
    self->ts[this->conn_id] = 0;
}

tick-10s
{
    printf("%Y\n", walltimestamp);
    printa("%@d total queries\n", @total_queries);
    printa(@latency);
    printf("\n");
}
