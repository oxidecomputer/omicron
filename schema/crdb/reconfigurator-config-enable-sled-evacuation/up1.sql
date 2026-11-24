-- Enable sled evacuation during an update on systems that already have a
-- reconfigurator config stored in the database, and do not yet have the
-- `sled_update_reboot_policy` set to `evacuate`.
--
-- The statement inserts nothing if the table is empty (Nexus then uses its
-- in-memory default), or the latest config already uses `evacuate`.
INSERT INTO omicron.public.reconfigurator_config (
    version,
    planner_enabled,
    time_modified,
    tuf_repo_pruner_enabled,
    disruption_policy,
    blueprint_pruner_enabled,
    blueprint_pruner_nkeep,
    sled_update_reboot_policy
)
SELECT
    version + 1,
    planner_enabled,
    now(),
    tuf_repo_pruner_enabled,
    disruption_policy,
    blueprint_pruner_enabled,
    blueprint_pruner_nkeep,
    'evacuate'
FROM omicron.public.reconfigurator_config
WHERE version = (
    SELECT max(version) FROM omicron.public.reconfigurator_config
)
AND sled_update_reboot_policy = 'immediate_no_evacuation';
