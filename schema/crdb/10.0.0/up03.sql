set local disallow_full_table_scans = off;

UPDATE omicron.public.dataset
SET kind = 'clickhouse_keeper2'
WHERE kind = 'clickhouse_keeper';