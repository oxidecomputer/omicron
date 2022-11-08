# Base update case: any version trivially updates to itself.
update(_resource, from, from, []);

# Unit tests: update anything trivially, but not arbitrarily.
?= update("foo", "foo", "foo", []);
?= not update("foo", "foo", "bar", []);

# Recursive update case: update each sub-component.
# E.g., to update a rack, update each of its sleds;
# to update a sled, update each of its RoT & SP.
update(resource: CompoundComponent, from, to, plan) if
  #to > from and
  #resource.version = from and
  update_components(resource.components, from, to, plan);

# Handle sub-components one at a time.
update_components([], _from, _to, []);
update_components([first, *rest], from, to, [plan1, *rest_plan]) if
  update(first, from, to, plan1) and
  update_components(rest, from, to, rest_plan);

# Handle components with images.
update(resource: Component, from, to, plan) if
  image in resource.image and
  update(image, from, to, plan);

# Update a Hubris image and reboot into the new one.
update(resource: HubrisImage, from, to, plan) if
  plan = [[new Update(resource, from, to),
           new Reboot(resource, to)]];
