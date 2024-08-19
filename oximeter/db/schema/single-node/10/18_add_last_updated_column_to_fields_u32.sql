ALTER TABLE oximeter.fields_u32 ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
