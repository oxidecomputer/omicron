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
FROM switch_port sp
JOIN switch_port_settings_bgp_peer_config bpc
ON sp.port_settings_id = bpc.port_settings_id
AND sp.port_name = bpc.interface_name
JOIN bgp_config bc ON bc.id = bpc.bgp_config_id;
