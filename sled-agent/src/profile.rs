// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to build SMF profiles.

use std::fmt::{Display, Error, Formatter};

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
}

impl Display for ProfileBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
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
    property_groups: Vec<PropertyGroupBuilder>,
}

impl ServiceBuilder {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), property_groups: vec![] }
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
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            r#"  <service version="1" type="service" name="{name}">
    <instance enabled="true" name="default">
"#,
            name = self.name
        )?;

        for property_group in &self.property_groups {
            write!(f, "{}", property_group)?;
        }

        write!(
            f,
            r#"    </instance>
  </service>
"#
        )?;

        Ok(())
    }
}

pub struct PropertyGroupBuilder {
    name: String,
    properties: Vec<Property>,
}

impl PropertyGroupBuilder {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), properties: vec![] }
    }

    pub fn add_property(mut self, name: &str, ty: &str, value: &str) -> Self {
        self.properties.push(Property {
            name: name.to_string(),
            ty: ty.to_string(),
            value: value.to_string(),
        });
        self
    }
}

impl Display for PropertyGroupBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            r#"      <property_group type="application" name="{name}">
"#,
            name = self.name
        )?;
        for property in &self.properties {
            write!(f, "{}", property)?;
        }
        write!(
            f,
            r#"      </property_group>
"#
        )?;
        Ok(())
    }
}

pub struct Property {
    name: String,
    ty: String,
    value: String,
}

impl Display for Property {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            r#"        <propval type="{ty}" name="{name}" value="{value}"/>
"#,
            name = self.name,
            ty = self.ty,
            value = self.value
        )?;
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
    <instance enabled="true" name="default">
    </instance>
  </service>
</service_bundle>"#,
        );
    }

    #[test]
    fn test_property_group() {
        let builder = ProfileBuilder::new("myprofile").add_service(
            ServiceBuilder::new("myservice")
                .add_property_group(PropertyGroupBuilder::new("mypg")),
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
            ServiceBuilder::new("myservice").add_property_group(
                PropertyGroupBuilder::new("mypg")
                    .add_property("prop", "type", "value"),
            ),
        );
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
    <instance enabled="true" name="default">
      <property_group type="application" name="mypg">
        <propval type="type" name="prop" value="value"/>
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
                .add_property_group(
                    PropertyGroupBuilder::new("mypg")
                        .add_property("prop", "type", "value")
                        .add_property("prop2", "type", "value2"),
                )
                .add_property_group(
                    PropertyGroupBuilder::new("mypg2")
                        .add_property("prop3", "type", "value3"),
                ),
        );
        assert_eq!(
            format!("{}", builder),
            r#"<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
<service_bundle type="profile" name="myprofile">
  <service version="1" type="service" name="myservice">
    <instance enabled="true" name="default">
      <property_group type="application" name="mypg">
        <propval type="type" name="prop" value="value"/>
        <propval type="type" name="prop2" value="value2"/>
      </property_group>
      <property_group type="application" name="mypg2">
        <propval type="type" name="prop3" value="value3"/>
      </property_group>
    </instance>
  </service>
</service_bundle>"#,
        );
    }
}
