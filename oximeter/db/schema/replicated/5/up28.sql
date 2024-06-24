CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami64 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogrami64_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami64_local', xxHash64(splitByChar(':', timeseries_name)[1]));
