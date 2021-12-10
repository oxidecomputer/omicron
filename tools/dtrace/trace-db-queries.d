#!/usr/sbin/dtrace -qZs

#pragma strsize=16k

/* Trace all queries to the control plane database */

dtrace:::BEGIN
{
    printf("Tracing all database queries for nexus PID %d, use Ctrl-C to exit\n", $target);
}

diesel_db$target:::query_start
{
    printf("conn_id: %s, query: '%s'\n", json(copyinstr(arg1), "ok"), copyinstr(arg2));
}
