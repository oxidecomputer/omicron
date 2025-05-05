-- backfill instance intended states based on the current state of the
-- instance's active VMM record.
SET LOCAL disallow_full_table_scans = off;
UPDATE instance SET intended_state = CASE
    WHEN instance.state = 'destroyed' THEN 'destroyed'
    WHEN instance.state = 'failed' THEN 'running'
    WHEN instance.state = 'vmm' AND vmm.state = 'stopped' THEN 'stopped'
    WHEN instance.state = 'vmm' AND vmm.state = 'stopping' THEN 'stopped'
    WHEN instance.state = 'vmm' THEN 'running'
    ELSE 'stopped'
END
FROM omicron.public.vmm
WHERE instance.active_propolis_id = vmm.id;
