# Base update case: any version trivially updates to itself.
update(_component, from, from, []);

# Unit tests: update anything trivially, but not arbitrarily.
?= update("foo", "foo", "foo", []);
?= not update("foo", "foo", "bar", []);

# Recursive update case: update each sub-component to a newer version.
# E.g., to update a rack, update each of its sleds; to update a sled,
# update its RoT, SP, etc.
update(component: Component, from: Integer, to: Integer, plan) if
  to > from and
  component.version = from and
  not component.components = [] and
  update_components(component.components, from, to, plan);

# Handle sub-components one at a time.
update_components([], _from, _to, []);
update_components([first, *rest], from, to, [plan1, *rest_plan]) if
  update(first, from, to, plan1) and
  update_components(rest, from, to, rest_plan);

# Handle components with images.
update(component: Component, from, to, plan) if
  image in component.image and
  update_image(component, image, from, to, plan);

# Update an image and reboot into the new one.
update_image(component: Component, image: Image, from, to, plan) if
  plan = [[new Update(component, image, from, to),
           new Reboot(component, image, to)]];
