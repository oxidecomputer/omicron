CREATE TABLE IF NOT EXISTS omicron.public.inv_last_reconciliation_orphaned_dataset (
    inv_collection_id UUID NOT NULL,
    sled_id UUID NOT NULL,
    pool_id UUID NOT NULL,
    kind omicron.public.dataset_kind NOT NULL,
    zone_name TEXT NOT NULL,
    CONSTRAINT zone_name_for_zone_kind CHECK (
      (kind != 'zone' AND zone_name = '') OR
      (kind = 'zone' AND zone_name != '')
    ),
    reason TEXT NOT NULL,
    id UUID,
    mounted BOOL NOT NULL,
    available INT8 NOT NULL,
    used INT8 NOT NULL,
    PRIMARY KEY (inv_collection_id, sled_id, pool_id, kind, zone_name)
);
