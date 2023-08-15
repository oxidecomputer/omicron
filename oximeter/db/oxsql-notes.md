Want a real SQL interface to Oximeter's ClickHouse tables.

The query for selecting the _values_ from each field, such as `method == "GET"`
is rewritten as:

```sql
select *
from oximeter.fields_{dtype}
where (
    timeseries_name = '{timeseries_name}' and
    field_name = 'method' and
    field_value = 'GET'
)
```

Any non-zero number of these goes into the current `SelectQuery` as the field
queries. Then we create the "full" field query of that by joining them on the
`timeseries_name` and `timeseries_key` columns. Each entry here is provided a
table alias `filter{i}` where `i` is the index in the list of filters for this
query. (That's part of why we need more than zero, and that's something we'll
want to relax.)

So given a list of these, the full query is:

```sql
select filter0.timeseries_name, filter0.timeseries_key, filter0.field_name, filter0.field_value, filter1.field_name, filter1.field_value, ...
(
    select *
    from oximeter.fields_{dtype}
    where (
        timeseries_name = '{timeseries_name}' and
        field_name = 'method' and
        field_value = 'GET'
) as filter0
inner join
(
    select *
    from oximeter.fields_{dtype}
    where (
        timeseries_name = '{timeseries_name}' and
        field_name = 'route' and
        field_value = '/some/api/route'
    )
) as filter1
on (
    filter0.timeseries_key = filter1.timeseries_key and
    filter0.timeseries_name = filter1.timeseries_name
)
inner join
...
order by filter0.timeseries_name, filter0.timeseries_key
```

So, we'll need to rename those to the virtual table columns, which are derived
from the timeseries schema:

```sql
select
    filter0.timeseries_key as timeseries_key,
    filter0.field_value as stringify(filter0.field_name),
    filter1.field_value as stringify(filter1.field_name),
    ...
```

This really just selects the timeseries keys compatible with the provided
filters. Recall that we really issue up to three queries to ClickHouse for
handling one client query:

1. Fetch the timeseries schema
2. Construct a field query to fetch the compatible timeseries keys
3. Construct a measurement query.

The last is done by taking the timeseries keys from the above, and then doing:

```sql
select *
from oximeter.measurements_{dtype}
where timeseries_name = '{timeseries_name}'
and timeseries_keys in ({the list of keys from the above query})
and {maybe timestamp query}
{maybe order by clause}
{maybe limit/offset pagination clause}
```
