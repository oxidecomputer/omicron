ALTER TABLE oximeter.fields_i16 ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
