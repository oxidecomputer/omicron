ALTER TABLE oximeter.fields_bool
ON CLUSTER oximeter_cluster
ALTER COLUMN IF EXISTS
field_value TYPE Bool;
