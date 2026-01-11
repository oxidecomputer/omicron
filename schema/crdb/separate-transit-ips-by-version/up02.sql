/*
 * Update the transit_ips and transit_ips_v6 columns to be specific to each IP
 * version.
 *
 * CRDB doesn't have methods for operating on arrays, like filter / map / etc,
 * so we unnest them into a sequence of rows and insert back.
 */
SET LOCAL disallow_full_table_scans = 'off';
UPDATE omicron.public.network_interface
SET
transit_ips = ARRAY(
    SELECT ip FROM UNNEST(transit_ips) AS ip
    WHERE family(ip) = 4
),
transit_ips_v6 = ARRAY(
    SELECT ip FROM UNNEST(transit_ips) AS ip
    WHERE family(ip) = 6
);
