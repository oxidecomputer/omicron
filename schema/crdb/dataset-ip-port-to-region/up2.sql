-- Set the ip of the region table to the one in the corresponding dataset table
UPDATE omicron.public.region SET (ip) = 
    (SELECT ip FROM omicron.public.dataset WHERE kind = 'crucible' 
         AND id=omicron.public.region.dataset_id);
