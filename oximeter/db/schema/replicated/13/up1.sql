ALTER TABLE oximeter.fields_bool_local
ON CLUSTER oximeter_cluster
ALTER COLUMN IF EXISTS
field_value TYPE Bool;
