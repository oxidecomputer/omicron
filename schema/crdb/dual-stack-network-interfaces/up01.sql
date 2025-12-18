-- Drop the index which will depend on the `ipv6` column we're about to add.
DROP INDEX IF EXISTS network_interface@network_interface_by_parent;
