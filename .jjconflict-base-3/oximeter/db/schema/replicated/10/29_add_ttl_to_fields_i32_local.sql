ALTER TABLE oximeter.fields_i32_local ON CLUSTER oximeter_cluster MODIFY TTL last_updated_at + INTERVAL 30 DAY;
