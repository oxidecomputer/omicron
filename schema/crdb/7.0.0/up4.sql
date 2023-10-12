CREATE OR REPLACE VIEW omicron.public.sled_instance
AS SELECT
   instance.id,
   instance.name,
   silo.name as silo_name,
   project.name as project_name,
   vmm.sled_id as active_sled_id,
   instance.time_created,
   instance.time_modified,
   instance.migration_id,
   instance.ncpus,
   instance.memory,
   vmm.state
FROM
    omicron.public.instance AS instance
    JOIN omicron.public.project AS project ON
            instance.project_id = project.id
    JOIN omicron.public.silo AS silo ON
            project.silo_id = silo.id
    JOIN omicron.public.vmm AS vmm ON
            instance.active_propolis_id = vmm.id
WHERE
    instance.time_deleted IS NULL AND vmm.time_deleted IS NULL;
