-- "collapse_lldp_settings" created this column as STRING(512), but dbinit.sql
-- has always used STRING(612). Converge racks that were upgraded through that
-- migration with those that were installed fresh.
ALTER TABLE omicron.public.lldp_link_config
    ALTER COLUMN system_description TYPE STRING(612);
