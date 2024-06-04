SET LOCAL disallow_full_table_scans = off;
UPDATE omicron.public.vmm SET state = downlevel_state::text::vmm_state;
