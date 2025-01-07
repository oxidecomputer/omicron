CREATE TYPE IF NOT EXISTS omicron.public.support_bundle_state AS ENUM (
  -- The bundle is currently being created.
  --
  -- It might have storage that is partially allocated on a sled.
  'collecting',

  -- The bundle has been collected successfully, and has storage on
  -- a particular sled.
  'active',

  -- The user has explicitly requested that a bundle be destroyed.
  -- We must ensure that storage backing that bundle is gone before
  -- it is automatically deleted.
  'destroying',

  -- The support bundle is failing.
  -- This happens when Nexus is expunged partway through collection.
  --
  -- A different Nexus must ensure that storage is gone before the
  -- bundle can be marked "failed".
  'failing',

  -- The bundle has finished failing.
  --
  -- The only action that can be taken on this bundle is to delete it.
  'failed'
);

