SET LOCAL disallow_full_table_scans = off;

-- Pick `boot_disk_id` values for instances, when we know doing so will not
-- risk adverse impact to the instance.
--
-- Boot orders, before the change motivating this migration, are firstly
-- defined by disk PCI device ordering. But that initial order is then
-- persisted in user-provided EFI system partitions (ESP)s on those disks,
-- where guest OS code or guest OS operators may further change it. So, to know
-- exactly which disks an instance would try to boot, today, we would need to
-- do something like:
-- * figure out which, if any, OVMF would find an ESP
--   * this is probably "walk disks in PCI device order", but I'm not sure!
-- * find that ESP's persisted BootOrder
-- * walk devices (potentially by partition) according that boot order
-- * find whichever disk would actually get booted
-- * set `boot_disk_id` to this disk.
--
-- "find" and "figure out" are very load-bearing in the above, but answers
-- probably could be determined by careful reading of EDK2.
--
-- We would have to take such care in picking a `boot_disk_id` in this
-- migration because once we start setting `boot_disk_id` on an instance, and
-- that instance is booted, OVMF will persist a *new* boot order with the
-- indicated disk up at the front. *this will blow away whatever boot order was
-- present in the guest's ESP, potentially removing entries that named the
-- intended boot option*.
--
-- So, when an instance has multiple attached disks, exactly which is *really*
-- booted is approximately a black box. It's *probably* the lowest PCI-numbered
-- disk, but we don't know without careful inspection that we don't know how to
-- do. If we pick wrong, the instance might be rendered unbootable without user
-- intervention. If we just don't pick, the instance was probably in a usable
-- state and can continue to be used. The safer option is to just not guess at
-- boot disks in these cases.
--
-- If the instance only has one disk attached though, that's the sole disk it
-- would try booting, so just pick it here. That should be the majority of
-- instances anyway.
UPDATE instance SET boot_disk_id = d.id FROM (
    -- "min(id)" is incredibly gross, but because of `HAVING COUNT(*) = 1`
    -- below, this is the minimum of a single id. There just needs to be some
    -- aggregate function used to select the non-aggregated `id` from the group.
    SELECT attach_instance_id, min(id) as id
    FROM disk
    WHERE disk_state = 'attached' AND attach_instance_id IS NOT NULL
    GROUP BY attach_instance_id
    HAVING COUNT(*) = 1
) d
WHERE instance.id = d.attach_instance_id;
