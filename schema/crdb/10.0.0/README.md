# Why?

This migration is part of a PR that adds a check to the schema tests ensuring that the order of enum members is the same when starting from scratch with `dbinit.sql` as it is when building up from existing deployments by running the migrations. The problem: there were already two enums, `dataset_kind` and `service_kind`, where the order did not match, so we have to fix that by putting the enums in the "right" order even on an existing deployment where the order is wrong. To do that, for each of those enums, we:

1. add `clickhouse_keeper2` member
1. change existing uses of `clickhouse_keeper` to `clickhouse_keeper2`
1. drop `clickhouse_keeper` member
1. add `clickhouse_keeper` back in the right order using `AFTER 'clickhouse'`
1. change uses of `clickhouse_keeper2` back to `clickhouse_keeper`
1. drop `clickhouse_keeper2`

As there are 6 steps here and two different enums to do them for, there are 12 `up*.sql` files.
