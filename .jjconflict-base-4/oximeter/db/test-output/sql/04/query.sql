SELECT
    timeseries_key,
    count() AS total
FROM service:request_latency
GROUP BY timeseries_key;
