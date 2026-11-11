SELECT
    count() AS total
FROM service:request_latency
GROUP BY name, id, route, method, status_code;
