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
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Component {
    Host(Image),
    PSC(CompoundComponent),
    RoT(Image),
    SP(Image),
    Gemini(CompoundComponent), // SP, RoT
    Gimlet(CompoundComponent), // SP, RoT, Host
    Scrimlet(CompoundComponent),
    Sidecar(CompoundComponent),
    Rack(CompoundComponent),
}

impl Component {
    fn components(&self) -> Vec<Component> {
        match self {
            Self::Host(_) | Self::RoT(_) | Self::SP(_) => vec![],
            Self::PSC(cc)
            | Self::Gemini(cc)
            | Self::Gimlet(cc)
            | Self::Scrimlet(cc)
            | Self::Sidecar(cc)
            | Self::Rack(cc) => cc.components(),
        }
    }

    fn image(&self) -> Option<Image> {
        match self {
            Self::Host(i) => Some(i.clone()),
            Self::RoT(i) | Self::SP(i) => Some(i.clone()),
            Self::PSC(_)
            | Self::Gemini(_)
            | Self::Gimlet(_)
            | Self::Scrimlet(_)
            | Self::Sidecar(_)
            | Self::Rack(_) => None,
        }
    }

    fn version(&self) -> Version {
        match self {
            Self::Host(i) => i.version(),
            Self::RoT(i) | Self::SP(i) => i.version(),
            Self::PSC(cc)
            | Self::Gemini(cc)
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
#[derive(Clone, Debug, Eq, PartialEq)]
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

/// Describes an updatable processor image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Image {
    Host(HostImage),
    Hubris(HubrisImage),
}

impl Image {
    fn components(&self) -> Vec<Component> {
        vec![]
    }

    fn version(&self) -> Version {
        match self {
            Self::Host(i) => i.version(),
            Self::Hubris(i) => i.version(),
        }
    }
}

impl oso::PolarClass for Image {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::Class::builder()
            .with_equality_check()
            .add_attribute_getter("version", Self::version)
    }
}

/// Describes an updatable, multi-phase host processor image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostImage {
    version: Version,
    //phase1, phase2, phase?_tramp, ...
}

impl HostImage {
    fn new(version: Version) -> Self {
        Self { version }
    }

    fn version(&self) -> Version {
        self.version.clone()
    }
}

impl oso::PolarClass for HostImage {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::ClassBuilder::with_constructor(Self::new)
            .with_equality_check()
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

    fn version(&self) -> Version {
        self.version.clone()
    }
}

impl oso::PolarClass for HubrisImage {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::ClassBuilder::with_constructor(Self::new)
            .with_equality_check()
            .add_attribute_getter("version", Self::version)
    }
}

/// Describes an update being planned or executed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Update {
    pub component: Component,
    pub image: Image,
    pub from: Version,
    pub to: Version,
}

impl Update {
    fn new(
        component: Component,
        image: Image,
        from: Version,
        to: Version,
    ) -> Self {
        Self { component, image, from, to }
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
    pub component: Component,
    pub image: Image,
    pub to: Version,
}

impl Reboot {
    fn new(component: Component, image: Image, to: Version) -> Self {
        Self { component, image, to }
    }
}

impl oso::PolarClass for Reboot {
    fn get_polar_class_builder() -> oso::ClassBuilder<Self> {
        oso::ClassBuilder::with_constructor(Self::new).with_equality_check()
    }
}

/// Produce a list of plans for updating `component` from version `from`
/// to `to` that is consistent with the (already loaded into `oso`) Polar
/// update policy (see `update.polar`). Works by querying the `update`
/// rule with an unbound `plan` variable and accumulating the results.
fn plan_update(
    oso: &Oso,
    component: impl ToPolar,
    from: &Version,
    to: &Version,
) -> Result<Vec<PolarValue>> {
    let plan = PolarValue::Variable("plan".to_string());
    let mut plans = Vec::<PolarValue>::new();
    let mut query = oso.query_rule(
        "update",
        (component.to_polar(), from.to_polar(), to.to_polar(), plan),
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
        let i = Image::Hubris(h);
        let sp = Component::SP(i.clone());
        let update = Update::new(sp.clone(), i.clone(), 0, 1);
        let reboot = Reboot::new(sp.clone(), i.clone(), 1);
        match &plan_update(&oso, sp, &0, &1).expect("plans").as_slice() {
            [PolarValue::List(plan)] => match plan.as_slice() {
                [PolarValue::List(plan)] => match plan.as_slice() {
                    [PolarValue::Instance(x), PolarValue::Instance(y)] => {
                        let x: &Update = x.downcast(None).unwrap();
                        let y: &Reboot = y.downcast(None).unwrap();
                        assert_eq!(&update, x);
                        assert_eq!(&reboot, y);
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
        let i = Image::Hubris(h);
        let sp = Component::SP(i.clone());
        let rot = Component::RoT(i.clone());
        let update_sp = Update::new(sp.clone(), i.clone(), 0, 1);
        let reboot_sp = Reboot::new(sp.clone(), i.clone(), 1);
        let update_rot = Update::new(rot.clone(), i.clone(), 0, 1);
        let reboot_rot = Reboot::new(rot.clone(), i.clone(), 1);
        let gemini = Component::Gemini(CompoundComponent(vec![sp, rot]));
        match &plan_update(&oso, gemini, &0, &1).expect("plans").as_slice() {
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
                                assert_eq!(&update_sp, x);
                                assert_eq!(&reboot_sp, y);
                                assert_eq!(&update_rot, z);
                                assert_eq!(&reboot_rot, w);
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

    fn make_gimlet(version: Version) -> Component {
        let h = HubrisImage::new(version);
        let i = Image::Hubris(h);
        let rot = Component::RoT(i.clone());
        let sp = Component::SP(i.clone());
        let hi = Image::Host(HostImage { version });
        let host = Component::Host(hi);
        Component::Gimlet(CompoundComponent::new(vec![rot, sp, host]))
    }

    #[test]
    fn test_gimlet_update_plan() {
        let logctx = dev::test_setup_log("test_gimlet_update_plan");
        let oso_init = make_omicron_oso(&logctx.log).expect("oso init");
        let oso = oso_init.oso;

        let gimlet = make_gimlet(0);
        match &plan_update(&oso, gimlet, &0, &1).expect("plans").as_slice() {
            [PolarValue::List(plan)] => match plan.as_slice() {
                [PolarValue::List(plan0), PolarValue::List(plan1), PolarValue::List(plan2)] => {
                    match (plan0.as_slice(), plan1.as_slice(), plan2.as_slice())
                    {
                        (
                            [PolarValue::List(actions0)],
                            [PolarValue::List(actions1)],
                            [PolarValue::List(actions2)],
                        ) => match (
                            actions0.as_slice(),
                            actions1.as_slice(),
                            actions2.as_slice(),
                        ) {
                            (
                                [PolarValue::Instance(x), PolarValue::Instance(y)],
                                [PolarValue::Instance(z), PolarValue::Instance(w)],
                                [PolarValue::Instance(u), PolarValue::Instance(v)],
                            ) => {
                                let x: &Update = x.downcast(None).unwrap();
                                let y: &Reboot = y.downcast(None).unwrap();
                                let z: &Update = z.downcast(None).unwrap();
                                let w: &Reboot = w.downcast(None).unwrap();
                                let u: &Update = u.downcast(None).unwrap();
                                let v: &Reboot = v.downcast(None).unwrap();
                                assert_eq!(x.to, 1);
                                assert_eq!(y.to, 1);
                                assert_eq!(z.to, 1);
                                assert_eq!(w.to, 1);
                                assert_eq!(u.to, 1);
                                assert_eq!(v.to, 1);

                                use Component::*;
                                assert!(matches!(x.component, RoT(_)));
                                assert!(matches!(y.component, RoT(_)));
                                assert!(matches!(z.component, SP(_)));
                                assert!(matches!(w.component, SP(_)));
                                assert!(matches!(u.component, Host(_)));
                                assert!(matches!(v.component, Host(_)));
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
