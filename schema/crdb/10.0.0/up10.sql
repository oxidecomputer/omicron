set local disallow_full_table_scans = off;

UPDATE omicron.public.service
SET kind = 'clickhouse_keeper'
WHERE kind = 'clickhouse_keeper2';