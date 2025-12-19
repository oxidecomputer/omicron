-- Working with INET values in SQL is "fun".
--
-- In `up01.sql`, we added the new `last_allocated_ip_subnet_offset` column to
-- `bp_sled_metadata`, but left it as `NULL` for all rows. We now need to fill
-- in all rows with the correct value. The correct value is nontrivial:
--
-- For the given blueprint and the given sled in that blueprint, look at all of
-- its `bp_omicron_zone` rows (except internal DNS; more below). Extract the
-- last hextet of each of those rows. (This has to be done via a combination of
-- inet functions and string manipulations.) Convert it to an integer. Take the
-- maximum of these integers, or 32 if there are no zones or if there are no
-- zones with a final hextet of at least 32.
--
-- 32 is the magic value for `RSS_RESERVED_ADDRESSES` as defined in
-- `omicron-common`; Reconfigurator always starts new, empty sleds with a
-- `last_allocated_ip_subnet_offset` equal to 32. If Reconfigurator has added
-- zones to this sled in any given blueprint, it will have a zone with an IP
-- with a final hextet greater than 32, and we need to use that instead.
--
-- Finally, all of this logic also has to ignore `internal_dns` zones, because
-- they listen on an IP that's outside the sled subnet.
--
-- There are data migration tests that confirm this query behaves as expected.

SET LOCAL disallow_full_table_scans = off;

UPDATE omicron.public.bp_sled_metadata AS bpm SET
    last_allocated_ip_subnet_offset = (
        -- The `COALESCE` here (plus the `WHERE final_hextet > 32` below)
        -- guarantees we set every `last_allocated_ip_subnet_offset` to
        -- 32-or-higher. If there are any zones that return a final hextet
        -- greater than 32, we'll get that; otherwise, `MAX(final_hextet)` will
        -- be `NULL` and the `COALESCE` will give us 32.
        SELECT COALESCE(MAX(final_hextet), 32) FROM (
            SELECT ('x' ||
                lpad(
                    substr(
                        -- bitwise & the IP with a /112 hostmask, which
                        -- squishes this IP down to the string '::NNNN',
                        -- where there will be 1-4 hex-valued X characters.
                        host(primary_service_ip & hostmask('::/112')),
                    -- Final arg to `substr()`: this trims off the leading
                    -- `::` in the `::NNNN` we produced above.
                    3),
                -- Final args to `lpad()`: this ensure we always have
                -- exactly 4 hex digits, left padding with 0 if needed.
                4, '0')
            -- We concatenated a literal 'x' prefix onto the front of the
            -- `NNNN` value we just produced, which causes `bit(16)` to
            -- interpret it as a 16-bit hex value (which it is!). Then
            -- convert that bitstring into an integer.
            )::bit(16)::int AS final_hextet
            FROM bp_omicron_zone WHERE
                blueprint_id = bpm.blueprint_id
                AND sled_id = bpm.sled_id
                AND zone_type != 'internal_dns'
        ) WHERE final_hextet > 32
    )
    WHERE last_allocated_ip_subnet_offset IS NULL;
