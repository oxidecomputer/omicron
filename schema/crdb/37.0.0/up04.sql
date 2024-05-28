-- Mass-update the sled_policy column to match the sled_provision_state column.

-- This is a full table scan, but is unavoidable.
SET
  LOCAL disallow_full_table_scans = OFF;

UPDATE omicron.public.sled
    SET sled_policy = 
        (CASE provision_state
            WHEN 'provisionable' THEN 'in_service'
            WHEN 'non_provisionable' THEN 'no_provision'
            -- No need to specify the ELSE case because the enum has been
            -- exhaustively matched (sled_provision_state already bans NULL).
        END);
