ALTER TYPE
   omicron.public.alert_class
ADD VALUE IF NOT EXISTS
   'hw.insert.power.power_shelf.psu'
AFTER
   'test.quux.bar.baz'
