ALTER TABLE oximeter.fields_i64 ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
