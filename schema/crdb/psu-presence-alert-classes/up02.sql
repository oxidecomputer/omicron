ALTER TYPE
   omicron.public.alert_class
ADD VALUE IF NOT EXISTS
   'hardware.power_shelf.psu.remove'
AFTER
   'hardware.power_shelf.psu.insert'
