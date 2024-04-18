CREATE VIEW IF NOT EXISTS omicron.public.v2p_mapping_view
AS
SELECT
  n.id as nic_id,
  s.id as sled_id,
  s.ip as sled_ip,
  v.vni,
  n.mac,
  n.ip
FROM omicron.public.vmm vmm
JOIN omicron.public.sled s ON vmm.sled_id = s.id
JOIN omicron.public.network_interface n ON n.parent_id = vmm.instance_id
JOIN omicron.public.vpc_subnet vs ON vs.id = n.subnet_id
JOIN omicron.public.vpc v ON v.id = n.vpc_id
WHERE vmm.time_deleted IS NULL
AND n.kind = 'instance'
AND s.sled_policy = 'in_service'
AND s.sled_state = 'active';
