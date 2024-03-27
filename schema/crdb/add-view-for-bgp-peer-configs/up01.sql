CREATE VIEW IF NOT EXISTS omicron.public.bgp_peer_view
AS
SELECT
 sp.switch_location,
 sp.port_name,
 bpc.addr,
 bpc.hold_time,
 bpc.idle_hold_time,
 bpc.delay_open,
 bpc.connect_retry,
 bpc.keepalive,
 bc.asn
FROM omicron.public.switch_port sp
JOIN omicron.public.switch_port_settings_bgp_peer_config bpc
ON sp.port_settings_id = bpc.port_settings_id
JOIN omicron.public.bgp_config bc ON bc.id = bpc.bgp_config_id;
