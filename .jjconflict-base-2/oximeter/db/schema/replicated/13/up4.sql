ALTER TABLE oximeter.measurements_bool
ON CLUSTER oximeter_cluster
ALTER COLUMN IF EXISTS
datum TYPE Nullable(Bool);
