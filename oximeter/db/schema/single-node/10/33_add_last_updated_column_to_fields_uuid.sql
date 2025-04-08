ALTER TABLE oximeter.fields_uuid ADD COLUMN IF NOT EXISTS last_updated_at DateTime MATERIALIZED now();
