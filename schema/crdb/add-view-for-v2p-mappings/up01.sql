-- view for v2p mapping rpw
CREATE VIEW IF NOT EXISTS omicron.public.v2p_mapping_view
AS
WITH VmV2pMappings AS (
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
  AND n.kind != 'service'
  AND s.sled_policy = 'in_service'
  AND s.sled_state = 'active'
),
ProbeV2pMapping AS (
  SELECT
    n.id as nic_id,
    s.id as sled_id,
    s.ip as sled_ip,
    v.vni,
    n.mac,
    n.ip
  FROM omicron.public.network_interface n
  JOIN omicron.public.vpc_subnet vs ON vs.id = n.subnet_id
  JOIN omicron.public.vpc v ON v.id = n.vpc_id
  JOIN omicron.public.probe p ON n.parent_id = p.id
  JOIN omicron.public.sled s ON p.sled = s.id
  WHERE p.time_deleted IS NULL
  AND n.kind != 'service'
  AND s.sled_policy = 'in_service'
  AND s.sled_state = 'active'
)
SELECT nic_id, sled_id, sled_ip, vni, mac, ip FROM VmV2pMappings
UNION
SELECT nic_id, sled_id, sled_ip, vni, mac, ip FROM ProbeV2pMapping;
