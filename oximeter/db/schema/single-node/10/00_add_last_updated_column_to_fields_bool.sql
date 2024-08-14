ALTER TABLE oximeter.fields_bool ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
