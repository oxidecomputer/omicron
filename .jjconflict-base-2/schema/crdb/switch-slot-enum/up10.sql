-- Before dropping `switch_port.switch_location` in favor of the new
-- `switch_port.switch_slot`, we have to drop the view that references
-- `switch_location`. We'll recreate it to reference `switch_slot` afterwards.
DROP VIEW IF EXISTS omicron.public.bgp_peer_view;
