ALTER TABLE oximeter.fields_u16 ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
