CREATE TABLE IF NOT EXISTS oximeter.measurements_histogrami32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogrami32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogrami32_local', xxHash64(splitByChar(':', timeseries_name)[1]));
