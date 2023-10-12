/*
 * Drop the instance-by-sled index since there will no longer be a sled ID in
 * the instance table.
 */

DROP INDEX IF EXISTS lookup_instance_by_sled;
