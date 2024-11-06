ALTER TABLE oximeter.measurements_bool_local
ON CLUSTER oximeter_cluster
ALTER COLUMN IF EXISTS
datum TYPE Nullable(Bool);
