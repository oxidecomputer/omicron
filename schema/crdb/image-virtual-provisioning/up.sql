SET LOCAL disallow_full_table_scans = OFF;

-- Insert virtual_provisioning_resource entries for all existing non-deleted images.
-- This tracks each image as a storage-consuming resource.
INSERT INTO omicron.public.virtual_provisioning_resource (
    id,
    time_modified,
    resource_type,
    virtual_disk_bytes_provisioned,
    cpus_provisioned,
    ram_provisioned
)
SELECT
    image.id,
    NOW(),
    'image',
    image.size_bytes,
    0,
    0
FROM
    omicron.public.image
WHERE
    image.time_deleted IS NULL
ON CONFLICT (id) DO NOTHING;

-- Update project-level collections for project-scoped images.
-- Project-scoped images have a non-null project_id.
UPDATE omicron.public.virtual_provisioning_collection AS vpc
SET
    virtual_disk_bytes_provisioned = vpc.virtual_disk_bytes_provisioned + totals.total_size,
    time_modified = NOW()
FROM (
    SELECT
        project_id,
        SUM(size_bytes) AS total_size
    FROM
        omicron.public.image
    WHERE
        time_deleted IS NULL AND project_id IS NOT NULL
    GROUP BY
        project_id
) AS totals
WHERE
    vpc.id = totals.project_id;

-- Update silo-level collections for all images (both project-scoped and silo-scoped).
-- All images belong to a silo, so all images roll up to their silo's collection.
UPDATE omicron.public.virtual_provisioning_collection AS vpc
SET
    virtual_disk_bytes_provisioned = vpc.virtual_disk_bytes_provisioned + totals.total_size,
    time_modified = NOW()
FROM (
    SELECT
        silo_id,
        SUM(size_bytes) AS total_size
    FROM
        omicron.public.image
    WHERE
        time_deleted IS NULL
    GROUP BY
        silo_id
) AS totals
WHERE
    vpc.id = totals.silo_id;

-- Update fleet-level collection with total image storage across all silos.
UPDATE omicron.public.virtual_provisioning_collection
SET
    virtual_disk_bytes_provisioned = virtual_disk_bytes_provisioned + (
        SELECT COALESCE(SUM(size_bytes), 0)
        FROM omicron.public.image
        WHERE time_deleted IS NULL
    ),
    time_modified = NOW()
WHERE
    collection_type = 'Fleet';
