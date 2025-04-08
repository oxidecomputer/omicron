ALTER TABLE oximeter.fields_string ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
