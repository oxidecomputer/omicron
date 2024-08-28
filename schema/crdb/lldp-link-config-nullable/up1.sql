-- Refer to https://github.com/oxidecomputer/omicron/issues/6433 for the justificaiton
-- behind this schema change.
--
-- In short: the "collapse_lldp_settings" schema change was edited after
-- merging. That change included a schema change which added a non-null column
-- to an existing table. Such a data-modifying statement is only valid for
-- tables with no rows - however, in our test systems, we observed rows, which
-- prevented this schema change from progressing.
--
-- To resolve:
-- 1. Within the old "collapse_lldp_settings" change, we retroactively dropped the
-- non-null constraint. For systems with populated
-- "switch_port_settings_link_config" tables, this allows the schema update to
-- complete without an error.
-- 2. Within this new "lldp-link-config-nullable" change, we ALSO dropped the
-- non-null constraint. For systems without populated
-- "switch_port_settings_link_config" tables -- which may have been able to
-- apply the "collapse_lldp_settings" change successfully -- this converges the state
-- of the database to the same outcome, where the columns is nullable.
ALTER TABLE omicron.public.switch_port_settings_link_config ALTER COLUMN lldp_link_config_id DROP NOT NULL;
