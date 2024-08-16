CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramf32 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramf32_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramf32_local', xxHash64(splitByChar(':', timeseries_name)[1]));
