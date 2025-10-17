-- Drop the index which depends on a column we're about to rename
DROP INDEX IF EXISTS network_interface@network_interface_by_parent;
