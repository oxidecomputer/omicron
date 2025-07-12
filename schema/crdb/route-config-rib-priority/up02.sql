UPDATE omicron.public.switch_port_settings_route_config
SET rib_priority = 
    CASE 
        WHEN local_pref > 255 THEN 255
        WHEN local_pref < 0 THEN 0
        ELSE local_pref::INT2
    END;
