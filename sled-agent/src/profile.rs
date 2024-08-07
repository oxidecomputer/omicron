// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to build SMF profiles.

use illumos_utils::running_zone::InstalledZone;
use slog::Logger;
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

pub struct ProfileBuilder {
    name: String,
    services: Vec<ServiceBuilder>,
}

impl ProfileBuilder {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), services: vec![] }
    }

    pub fn add_service(mut self, service: ServiceBuilder) -> Self {
        self.services.push(service);
        self
    }

    pub async fn add_to_zone(
        &self,
        log: &Logger,
        installed_zone: &InstalledZone,
    ) -> Result<(), std::io::Error> {
        info!(log, "Profile for {}:\n{}", installed_zone.name(), self);
        let profile_path = installed_zone.site_profile_xml_path();

        tokio::fs::write(&profile_path, format!("{self}").as_bytes()).await?;
        Ok(())
    }
}

impl Display for ProfileBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="{name}">
"#,
            name = self.name
        )?;
        for service in &self.services {
            write!(f, "{}", service)?;
        }
        write!(f, "</service_bundle>")?;

        Ok(())
    }
}

pub struct ServiceBuilder {
    name: String,
    instances: Vec<ServiceInstanceBuilder>,
    property_groups: Vec<PropertyGroupBuilder>,
}

impl ServiceBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            instances: vec![],
            property_groups: vec![],
        }
    }

    pub fn add_instance(mut self, instance: ServiceInstanceBuilder) -> Self {
        self.instances.push(instance);
        self
    }

    pub fn add_property_group(
        mut self,
        property_group: PropertyGroupBuilder,
    ) -> Self {
        self.property_groups.push(property_group);
        self
    }
}

impl Display for ServiceBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            r#"  <service version="1" type="service" name="{name}">
"#,
            name = self.name
        )?;

        for property_group in &self.property_groups {
            write!(f, "{}", property_group)?;
        }

        for instance in &self.instances {
            write!(f, "{}", instance)?;
        }

        write!(
            f,
            r#"  </service>
"#
        )?;

        Ok(())
    }
}

pub struct ServiceInstanceBuilder {
    name: String,
    enabled: bool,
    property_groups: Vec<PropertyGroupBuilder>,
}

impl ServiceInstanceBuilder {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), enabled: true, property_groups: vec![] }
    }

    pub fn disable(mut self) -> Self {
        self.enabled = false;
        self
    }

    pub fn add_property_group(
        mut self,
        property_group: PropertyGroupBuilder,
    ) -> Self {
        self.property_groups.push(property_group);
        self
    }
}

impl Display for ServiceInstanceBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            r#"    <instance enabled="{enabled}" name="{name}">
"#,
            name = self.name,
            enabled = self.enabled
        )?;

        for property_group in &self.property_groups {
            write!(f, "{}", property_group)?;
        }

        write!(
            f,
            r#"    </instance>
"#
        )?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct PropertyGroupBuilder {
    name: String,
    /// names of the properties that were added, in the order they were added
    properties_seen: Vec<String>,
    /// type for each property, by name
    property_types: BTreeMap<String, String>,
    /// values seen for each property, by name
    property_values: BTreeMap<String, Vec<String>>,
}

impl PropertyGroupBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            properties_seen: vec![],
            property_types: BTreeMap::new(),
            property_values: BTreeMap::new(),
        }
    }

    pub fn add_property<S: Into<String>>(
        mut self,
        name: &str,
        ty: &str,
        value: S,
    ) -> Self {
        // The data structures here are oriented around a few goals:
        //
        // - Properties will be written out in the order that they were added.
        //   This does not affect correctness but is a nicer developer
        //   experience than writing them out alphabetically or in some other
        //   arbitrary order.  This tends to keep related properties together,
        //   since the code that adds them in the first place tends to do so in
        //   a logical way.
        //
        // - We support multi-valued properties using the `<property>` syntax.
        //
        // - If there's only one value for a property, we use the more concise
        //   `<propval>` syntax.
        if self
            .property_types
            .insert(name.to_string(), ty.to_string())
            .is_none()
        {
            self.properties_seen.push(name.to_string());
        }

        let values = self
            .property_values
            .entry(name.to_string())
            .or_insert_with(Vec::new);
        values.push(value.into());
        self
    }
}

impl Display for PropertyGroupBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            r#"      <property_group type="application" name="{name}">
"#,
            name = self.name
        )?;
        for property_name in &self.properties_seen {
            let ty = self.property_types.get(property_name).unwrap();
            let values = self.property_values.get(property_name).unwrap();
            if values.len() == 1 {
                write!(
                    f,
                    r#"        <propval type="{ty}" name="{name}" value='{value}'/>
"#,
                    name = property_name,
                    value = &values[0],
                )?;
            } else {
                write!(
                    f,
                    r#"        <property type="{ty}" name="{name}">
"#,
                    name = property_name,
                )?;
                write!(f, "          <{ty}_list>\n")?;
                for v in values {
                    write!(f, "            <value_node value={:?} />\n", v,)?;
                }
                write!(f, "          </{ty}_list>\n")?;
                write!(f, "        </property>\n")?;
            }
        }
        write!(f, "      </property_group>\n")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn empty_profile() {
        let builder = ProfileBuilder::new("myprofile");
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
</service_bundle>"#,
        );
    }

    #[test]
    fn test_service() {
        let builder = ProfileBuilder::new("myprofile")
            .add_service(ServiceBuilder::new("myservice"));
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
  </service>
</service_bundle>"#,
        );
    }

    #[test]
    fn test_service_property_group() {
        let builder = ProfileBuilder::new("myprofile").add_service(
            ServiceBuilder::new("myservice").add_property_group(
                PropertyGroupBuilder::new("mypg")
                    .add_property("myprop", "astring", "myvalue"),
            ),
        );
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
      <property_group type="application" name="mypg">
        <propval type="astring" name="myprop" value='myvalue'/>
      </property_group>
  </service>
</service_bundle>"#,
        );
    }

    #[test]
    fn test_instance() {
        let builder = ProfileBuilder::new("myprofile").add_service(
            ServiceBuilder::new("myservice")
                .add_instance(ServiceInstanceBuilder::new("default")),
        );
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
    <instance enabled="true" name="default">
    </instance>
  </service>
</service_bundle>"#,
        );
    }

    #[test]
    fn test_disabled_instance() {
        let builder = ProfileBuilder::new("myprofile")
            .add_service(ServiceBuilder::new("myservice").add_instance(
                ServiceInstanceBuilder::new("default").disable(),
            ));
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
    <instance enabled="false" name="default">
    </instance>
  </service>
</service_bundle>"#,
        );
    }

    #[test]
    fn test_property_group() {
        let builder = ProfileBuilder::new("myprofile").add_service(
            ServiceBuilder::new("myservice").add_instance(
                ServiceInstanceBuilder::new("default")
                    .add_property_group(PropertyGroupBuilder::new("mypg")),
            ),
        );
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
    <instance enabled="true" name="default">
      <property_group type="application" name="mypg">
      </property_group>
    </instance>
  </service>
</service_bundle>"#,
        );
    }

    #[test]
    fn test_property() {
        let builder = ProfileBuilder::new("myprofile").add_service(
            ServiceBuilder::new("myservice").add_instance(
                ServiceInstanceBuilder::new("default").add_property_group(
                    PropertyGroupBuilder::new("mypg")
                        .add_property("prop", "type", "value"),
                ),
            ),
        );
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
    <instance enabled="true" name="default">
      <property_group type="application" name="mypg">
        <propval type="type" name="prop" value='value'/>
      </property_group>
    </instance>
  </service>
</service_bundle>"#,
        );
    }

    #[test]
    fn test_multiple() {
        let builder = ProfileBuilder::new("myprofile").add_service(
            ServiceBuilder::new("myservice")
                .add_instance(
                    ServiceInstanceBuilder::new("default")
                        .add_property_group(
                            PropertyGroupBuilder::new("mypg")
                                .add_property("prop", "type", "value")
                                .add_property("prop2", "type", "value2"),
                        )
                        .add_property_group(
                            PropertyGroupBuilder::new("mypg2")
                                .add_property("prop3", "type", "value3"),
                        ),
                )
                .add_property_group(
                    PropertyGroupBuilder::new("mypgsvc")
                        .add_property("prop", "astring", "valuesvc")
                        .add_property("prop", "astring", "value2svc"),
                ),
        );
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
      <property_group type="application" name="mypgsvc">
        <property type="astring" name="prop">
          <astring_list>
            <value_node value="valuesvc" />
            <value_node value="value2svc" />
          </astring_list>
        </property>
      </property_group>
    <instance enabled="true" name="default">
      <property_group type="application" name="mypg">
        <propval type="type" name="prop" value='value'/>
        <propval type="type" name="prop2" value='value2'/>
      </property_group>
      <property_group type="application" name="mypg2">
        <propval type="type" name="prop3" value='value3'/>
      </property_group>
    </instance>
  </service>
</service_bundle>"#,
        );
    }
}
