# `oxdb sql`

This is a short how-to for using SQL to query timeseries. If you're eager to get
started, find a ClickHouse server with `oximeter` data, and run:

```console
oxdb --address $CLICKHOUSE_ADDR sql
```

You can use `help` to get a help menu on the CLI, or run `\l` to list available
timeseries to start querying.

## `oximeter` overview

In general, `oximeter`'s architecture and data model are laid out in RFDs 161
and 162. These provide a good detailed look at the system.

### Terminology

`oximeter` is the subsystem for describing, collecting, and storing telemetry
data from the Oxide rack. Software components make data available to an
`oximeter` collector in the form of _samples_, which are timestamped datapoints
from a single timeseries.

Timeseries are named for their _target_, the component being measured or
monitored, and the _metric_, the measured feature or aspect of the target. The
timeseries name is derived as `target_name:metric_name`. The target and metric
can both have name-value pairs called _fields_, and the metric additionally has
a _measurement_, the actual measured value. Both are strongly typed.

### Data normalization

As samples are collected, `oximeter` normalizes them before storing in
ClickHouse. The database consists of a set of tables for fields and
measurements, with each _type_ stored in a different table. For fields, the name
of the field is also stored; for measurements, the timestamp and actual datum
are stored. Additionally, one table stores all the received _timeseries schema_,
which describes the name, fields, and measurement types for each timeseries.

Normalizing the tables has many benefits. Less duplicated data is stored;
simpler, more static table arrangements; better compression; and more. It does
have drawbacks. Querying becomes especially tricky, because one needs to join
many tables together the reconstitute the original samples. This is exacerbated
by ClickHouse's lack of unique primary keys, which means we need to generate a
tag used to associated records from a single timeseries. These are called
_timeseries keys_, and are just hashes computed when a sample is received.

## Oximeter SQL

While writing the full set of join expressions needed to denormalize samples is
not very human-friendly, it _is_ relatively easy to generate these in code.
Using the stored timeseries schema and timeseries keys, one can write a (huge)
join expression that results in the full timeseries _as if_ it were a real table
in the database. `oxdb sql` generates this expression, and then runs whatever
query the user supplied on _the resulting in-memory table_.

### Basic commands

After starting the SQL shell with `oxdb sql`, one can run the following basic
operations:

- `\h` or `help` will print a _help_ menu
- `\l` will _list_ all available timeseries by name
- `\d <timeseries>` will _describe_ the schema of a single named timeseries
- `\f` will list supported ClickHouse functions and `\f <function>` will print
  more details about the function and its usage

### SQL

In general, normal ANSI SQL is supported. Instead of _table_, however, one
queries against a _timeseries_. For example:

```sql
SELECT count() FROM physical_data_link:bytes_received;
```

This will return the total number of samples in the timeseries representing the
number of bytes received on an Ethernet data link on a Gimlet. Here are the
available fields:

```console
0x〉\d physical_data_link:bytes_received
 hostname | link_name | rack_id | serial | sled_id | timestamp  | start_time | datum
----------+-----------+---------+--------+---------+------------+------------+---------------
 String   | String    | Uuid    | String | Uuid    | DateTime64 | DateTime64 | CumulativeU64
```

Any of these fields can be queried, including aggregations, groupings, etc.

```console
0x〉select min(timestamp), max(timestamp) from physical_data_link:bytes_received;

 min(timestamp)                  | max(timestamp)
---------------------------------+---------------------------------
 "2023-11-09 04:24:53.284336528" | "2023-11-09 22:12:58.986751414"

Metadata
 Query ID:    66e68db5-8792-4e48-af2d-e5a2a117ab0d
 Result rows: 1
 Time:        72.371047ms
 Read:        19736 rows (1292186 bytes)

```

or

```console
0x〉select distinct route from http_service:request_latency_histogram where name = 'nexus-internal';

 route
-------------------------------------------------------------------------------------------------
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/d462a7f7-b628-40fe-80ff-4e4189e2d62b"
 "/metrics/producers"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/616b26df-e62a-4c68-b506-f4a923d8aaf7"
 "/metrics/collect/1e9a8843-2327-4d59-94b2-14f909b6f207"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/f4b4dc87-ab46-49fb-a4b4-d361ae214c03"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/a462a7f7-b628-40fe-80ff-4e4189e2d62b"
 "/metrics/collect/4b795850-8320-4b7d-9048-aa277653ab8e"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/14b4dc87-ab46-49fb-a4b4-d361ae214c03"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/e4b4dc87-ab46-49fb-a4b4-d361ae214c03"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/cd70d7f6-2354-4bf2-8012-55bf9eaf7930"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/31bd71cd-4736-4a12-a387-9b74b050396f"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/b462a7f7-b628-40fe-80ff-4e4189e2d62b"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/24b4dc87-ab46-49fb-a4b4-d361ae214c03"
 "/physical-disk"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803"
 "/sled-agents/a8c6432e-338f-4839-bfb5-297112b39803/zpools/ceb4461c-cf56-4719-ad3c-14430bfdfb60"
 "/metrics/collect/5c4f4629-1325-4123-bdcd-01bc9a18d740"
 "/metrics/collectors"

Metadata
 Query ID:    3107c7ca-6906-4ce9-9d57-19cf0c1d6c71
 Result rows: 18
 Time:        119.387667ms
 Read:        206840 rows (14749196 bytes)

```

or

```console
0x〉select link_name, formatReadableSize(max(datum)) from physical_data_link:bytes_sent group by link_name;

 link_name | formatReadableSize(max(datum))
-----------+--------------------------------
 "net0"    | "5.96 MiB"
 "net1"    | "0.00 B"

Metadata
 Query ID:    cd101b14-a91e-419b-b2d0-633047db219e
 Result rows: 2
 Time:        56.036663ms
 Read:        27025 rows (1558430 bytes)

```

> Note the metadata at the bottom. The query ID is assigned by the server, which
  also returns the number of rows / bytes read. The _time_ includes the server
  processing time and the network time, and is usually dominated by the latter.

### JOINs

SQL joins are also supported, as long as they are either _inner joins_ or the
ClickHoouse-specific _asof join_. Inner joins are pretty standard, but `ASOF
JOIN` is unique and very useful. It provides a way to match up rows that do not
have an _exact_ equal in each table. As an example, we can use this to match up
metrics from different timeseries

```console
0x〉select timestamp, datum as bytes_received, s.timestamp, s.datum as bytes_sent from physical_data_link:bytes_received asof join physical_data_link:bytes_sent as s using (link_name, timestamp) where link_name = 'net0' limit 10;

 timestamp                       | bytes_received | s.timestamp                     | bytes_sent
---------------------------------+----------------+---------------------------------+------------
 "2023-11-09 04:24:53.284336528" | 0              | "2023-11-09 04:24:53.284336528" | 10661
 "2023-11-09 04:25:06.960255374" | 1064           | "2023-11-09 04:25:06.960255374" | 11937
 "2023-11-09 04:25:16.962286001" | 3748           | "2023-11-09 04:25:16.962286001" | 15910
 "2023-11-09 04:25:26.964768912" | 5278           | "2023-11-09 04:25:26.964768912" | 18465
 "2023-11-09 04:25:36.966422345" | 7146           | "2023-11-09 04:25:36.966422345" | 24423
 "2023-11-09 04:25:46.969640057" | 8032           | "2023-11-09 04:25:46.969640057" | 25370
 "2023-11-09 04:25:57.589902294" | 8868           | "2023-11-09 04:25:57.589902294" | 26277
 "2023-11-09 04:26:07.590262491" | 13120          | "2023-11-09 04:26:07.590262491" | 30225
 "2023-11-09 04:26:17.592895364" | 14584          | "2023-11-09 04:26:17.592895364" | 31501
 "2023-11-09 04:26:27.594820340" | 15344          | "2023-11-09 04:26:27.594820340" | 32211

Metadata
 Query ID:    a13f3abf-9c57-4caa-bfc6-c1b26732d2ad
 Result rows: 10
 Time:        124.670777ms
 Read:        45858 rows (3331596 bytes)

```

Note that these happen to have exactly the same timestamp, based on how they
were generated, but that need not be the case.

## Warnings and caveats

First this is a **prototype**. It is also designed for testing and
experimentation, and little or none of the product should expect this to work on
customer sites any time soon.

Second, the abstraction here is pretty leaky. SQL expressions that might
normally work against a real table can easily fail here. Please file a bug if
you think something _should_ work.

Last, and maybe most important, be aware of the resource limitations here. This
all works by constructing an _enormous_ joined table in memory. ClickHouse is
extremely fast, but it is relatively simplistic when it comes to query planning
and optimization. That means it will do exactly what the query says, including
trying to create tables much larger than the available memory.

For the most part, the blast radius of those problems should be limited to the
ClickHouse zone itself. We also limit the total memory consumption of the
server, currently to 90% of the zone's memory. But since we don't limit the
_zone's_ memory, that's 90% of the physical memory, which is very large indeed.
If you're curious how a query will perform, it's probably a good idea to try it
out on a small subset of data, by adding a `LIMIT` clause or similar. You can
also run `oxdb sql --transform $QUERY_STRING` to print the full query that will
actually be executed on the server.
