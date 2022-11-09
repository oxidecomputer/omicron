// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rust/Polar types for software update plans.

use anyhow::Result;
use oso::Oso;
use oso::PolarValue;
use oso::ToPolar;

type Version = i64;

/// Updatable components.
#[derive(Clone, Debug, PartialEq)]
pub enum Component {
    PSC(CompoundComponent),
    RoT(HubrisImage),
    SP(HubrisImage),
    Gimlet(CompoundComponent),
    Scrimlet(CompoundComponent),
    Sidecar(CompoundComponent),
    Rack(CompoundComponent),
}

impl Component {
    fn components(&self) -> Vec<Component> {
        match self {
            Self::RoT(_) | Self::SP(_) => vec![],
            Self::PSC(cc)
            | Self::Gimlet(cc)
            | Self::Scrimlet(cc)
            | Self::Sidecar(cc)
            | Self::Rack(cc) => cc.components(),
        }
    }

    fn image(&self) -> Option<HubrisImage> {
        match self {
            Self::RoT(h) | Self::SP(h) => Some(h.clone()),
            Self::PSC(_)
            | Self::Gimlet(_)
            | Self::Scrimlet(_)
            | Self::Sidecar(_)
            | Self::Rack(_) => None,
        }
    }

    fn version(&self) -> Version {
        match self {
            Self::RoT(h) | Self::SP(h) => h.version(),
            Self::PSC(cc)
            | Self::Gimlet(cc)
            | Self::Scrimlet(cc)
            | Self::Sidecar(cc)
            | Self::Rack(cc) => cc.version(),
        }
    }
}

impl oso::PolarClass for Component {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("components", Self::components)
            .add_attribute_getter("image", Self::image)
            .add_attribute_getter("version", Self::version)
    }
}

/// Describes an updatable component with sub-components,
/// like a `Gimlet` or a `Rack`.
#[derive(Clone, Debug, PartialEq)]
pub struct CompoundComponent(Vec<Component>);

impl CompoundComponent {
    fn new(components: Vec<Component>) -> Self {
        Self(components)
    }

    fn components(&self) -> Vec<Component> {
        self.0.clone()
    }

    fn version(&self) -> Version {
        self.0
            .iter()
            .max_by_key(|c| c.version())
            .expect("sub-components")
            .version()
    }
}

impl oso::PolarClass for CompoundComponent {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::ClassBuilder::with_constructor(Self::new)
            .with_equality_check()
            .add_attribute_getter("components", Self::components)
            .add_attribute_getter("version", Self::version)
    }
}

/// Describes an updatable Hubris image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubrisImage {
    version: Version,
}

impl HubrisImage {
    fn new(version: Version) -> Self {
        Self { version }
    }

    fn components(&self) -> Vec<Component> {
        vec![]
    }

    fn version(&self) -> Version {
        self.version.clone()
    }
}

impl oso::PolarClass for HubrisImage {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::ClassBuilder::with_constructor(Self::new)
            .with_equality_check()
            .add_attribute_getter("components", Self::components)
            .add_attribute_getter("version", Self::version)
    }
}

/// Describes a Hubris update being planned or executed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Update {
    image: HubrisImage,
    from: Version,
    to: Version,
}

impl Update {
    fn new(image: HubrisImage, from: Version, to: Version) -> Self {
        Self { image, from, to }
    }
}

impl oso::PolarClass for Update {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::ClassBuilder::with_constructor(Self::new).with_equality_check()
    }
}

/// Describes a planned reboot of some component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reboot {
    image: HubrisImage,
    into: Version,
}

impl Reboot {
    fn new(image: HubrisImage, into: Version) -> Self {
        Self { image, into }
    }
}

impl oso::PolarClass for Reboot {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::ClassBuilder::with_constructor(Self::new).with_equality_check()
    }
}

/// Produce a list of plans for updating `resource` from version `from`
/// to `to` that is consistent with the (already loaded into `oso`) Polar
/// update policy (see `update.polar`). Works by querying the `update`
/// rule with an unbound `plan` variable and accumulating the results.
fn plan_update(
    oso: &Oso,
    resource: impl ToPolar,
    from: &Version,
    to: &Version,
) -> Result<Vec<PolarValue>> {
    let plan = PolarValue::Variable("plan".to_string());
    let mut plans = Vec::<PolarValue>::new();
    let mut query = oso.query_rule(
        "update",
        (resource.to_polar(), from.to_polar(), to.to_polar(), plan),
    )?;
    loop {
        match query.next() {
            Some(Ok(result)) => {
                if let Some(plan) = result.get("plan") {
                    plans.push(plan);
                }
            }
            Some(Err(e)) => return Err(e.into()),
            None => return Ok(plans),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::authz::oso_generic::make_omicron_oso;
    use omicron_test_utils::dev;
    use oso::PolarValue;

    #[test]
    fn test_trivial_update_plan() {
        let logctx = dev::test_setup_log("test_trivial_update_plan");
        let oso_init = make_omicron_oso(&logctx.log).expect("oso init");
        let oso = oso_init.oso;
        assert_eq!(plan_update(&oso, "foo", &0, &1).expect("plans"), vec![]);
        assert_eq!(
            plan_update(&oso, "foo", &0, &0).expect("plans"),
            vec![PolarValue::List(vec![])]
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn test_simple_update_plan() {
        let logctx = dev::test_setup_log("test_simple_update_plan");
        let oso_init = make_omicron_oso(&logctx.log).expect("oso init");
        let oso = oso_init.oso;
        let h = HubrisImage::new(0);
        let u = Update::new(h.clone(), 0, 1);
        let r = Reboot::new(h.clone(), 1);
        match &plan_update(&oso, h, &0, &1).expect("plans").as_slice() {
            [PolarValue::List(plan)] => match plan.as_slice() {
                [PolarValue::List(plan)] => match plan.as_slice() {
                    [PolarValue::Instance(x), PolarValue::Instance(y)] => {
                        let x: &Update = x.downcast(None).unwrap();
                        let y: &Reboot = y.downcast(None).unwrap();
                        assert_eq!(&u, x);
                        assert_eq!(&r, y);
                    }
                    _ => assert!(false),
                },
                _ => assert!(false),
            },
            _ => assert!(false),
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn test_compound_update_plan() {
        let logctx = dev::test_setup_log("test_compound_update_plan");
        let oso_init = make_omicron_oso(&logctx.log).expect("oso init");
        let oso = oso_init.oso;
        let h = HubrisImage::new(0);
        let u = Update::new(h.clone(), 0, 1);
        let r = Reboot::new(h.clone(), 1);
        let c = CompoundComponent(vec![
            Component::RoT(h.clone()),
            Component::SP(h.clone()),
        ]);
        match &plan_update(&oso, c, &0, &1).expect("plans").as_slice() {
            [PolarValue::List(plan)] => match plan.as_slice() {
                [PolarValue::List(plan0), PolarValue::List(plan1)] => {
                    match (plan0.as_slice(), plan1.as_slice()) {
                        (
                            [PolarValue::List(actions0)],
                            [PolarValue::List(actions1)],
                        ) => match (actions0.as_slice(), actions1.as_slice()) {
                            (
                                [PolarValue::Instance(x), PolarValue::Instance(y)],
                                [PolarValue::Instance(z), PolarValue::Instance(w)],
                            ) => {
                                let x: &Update = x.downcast(None).unwrap();
                                let y: &Reboot = y.downcast(None).unwrap();
                                let z: &Update = z.downcast(None).unwrap();
                                let w: &Reboot = w.downcast(None).unwrap();
                                assert_eq!(&u, x);
                                assert_eq!(&r, y);
                                assert_eq!(&u, z);
                                assert_eq!(&r, w);
                            }
                            _ => assert!(false),
                        },
                        _ => assert!(false),
                    }
                }
                _ => assert!(false),
            },
            _ => assert!(false),
        }
        logctx.cleanup_successful();
    }
}
