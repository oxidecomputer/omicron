-- The control plane uses the built-in "default" router configuration,
-- as it did before router configurations existed.
INSERT INTO omicron.public.control_plane_router_configuration
    (priority, router_configuration_id)
VALUES (1000, '001de000-defa-4000-8000-000000000000')
ON CONFLICT DO NOTHING;
