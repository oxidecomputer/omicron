ALTER TABLE oximeter.fields_i8 ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
