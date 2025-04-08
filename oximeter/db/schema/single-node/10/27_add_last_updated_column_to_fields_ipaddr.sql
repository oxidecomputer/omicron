ALTER TABLE oximeter.fields_ipaddr ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
