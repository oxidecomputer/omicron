-- Drop the index which will depend on the `ipv6` column we're about to add.
DROP INDEX IF EXISTS network_interface@v2p_mapping_details;
