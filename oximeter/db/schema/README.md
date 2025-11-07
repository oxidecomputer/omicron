# ClickHouse schema files

This directory contains the SQL files for different versions of the ClickHouse
timeseries database used by `oximeter`. In general, schema are expected to be
applied while the database is online, but no other clients exist. This is
similar to the current situation for _offline upgrade_ we use when updating the
main control plane database in CockroachDB.

## Constraints, or why ClickHouse is weird

While this tool is modeled after the mechanism for applying updates in
CockroachDB, ClickHouse is a significantly different DBMS. There are no
transactions; no unique primary keys; a single DB server can house both
replicated and single-node tables. This means we need to be pretty careful when
updating the schema. Changes must be idempotent, as with the CRDB schema, but at
this point we do not support inserting or modifying data at all.

Similar to the CRDB offline update tool, we assume no non-update modifications
of the database are running concurrently. However, given ClickHouse's lack of
transactions, we actually require that there are no writes of any kind. In
practice, this means `oximeter` **must not** be running when this is called.
Similarly, there must be only a single instance of this program at a time.

_NB: Schema changes for the `oximeter` database are not allowed because there
is no mechanism to apply them to existing systems. If we need to support schema
changes at some point, we'll have to do the work to update them during automated
update. See omicron [#8862](https://github.com/oxidecomputer/omicron/issues/8862)
for details._

To run this program:

- Ensure the ClickHouse server is running, and grab its IP address;
 ```bash
 $ pfexec zlogin oxz_clickhouse_e449eb80-3371-40a6-a316-d6e64b039357 'ipadm show-addr -o addrobj,addr | grep omicron6'
 oxControlService20/omicron6 fd00:1122:3344:101::e/64
 ```
- Log into the `oximeter` zone, `zlogin oxz_oximeter_<UUID>`
- Run this tool, pointing it at the desired schema directory, e.g.:

```bash
# /opt/oxide/oximeter-collector/bin/clickhouse-schema-updater \
    --host <ADDR_FROM_ABOVE> \
    --schema-dir /opt/oxide/oximeter/sql
    up VERSION
```
