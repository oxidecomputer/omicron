ALTER TYPE
   omicron.public.alert_class
ADD VALUE IF NOT EXISTS
   'hardware.power_shelf.psu.insert'
AFTER
   'test.quux.bar.baz'
