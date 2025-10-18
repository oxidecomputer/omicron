-- Drop the index which depends on a column we're about to rename
DROP INDEX IF EXISTS network_interface@v2p_mapping_details;
