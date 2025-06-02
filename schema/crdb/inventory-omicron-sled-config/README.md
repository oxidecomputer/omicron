This directory contains migrations that make several changes to the
representation of inventory collections. The changes are not backwards
compatible: we're transitioning from recording only each sled's
`OmicronZonesConfig` to its entire `OmicronSledConfig`; we have no way of
backfilling the missing disk and dataset config information for preexisting
collections. Therefore, these migrations start by dropping all old collections.
