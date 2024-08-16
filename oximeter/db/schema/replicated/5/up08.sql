CREATE TABLE IF NOT EXISTS oximeter.measurements_histogramu8 ON CLUSTER oximeter_cluster
AS oximeter.measurements_histogramu8_local
ENGINE = Distributed('oximeter_cluster', 'oximeter', 'measurements_histogramu8_local', xxHash64(splitByChar(':', timeseries_name)[1]));
