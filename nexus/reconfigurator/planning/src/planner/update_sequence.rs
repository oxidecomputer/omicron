// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Updatable components ordered by dependencies (RFD 565).

use nexus_sled_agent_shared::inventory::ZoneKind;
use strum::{EnumIter, IntoEnumIterator as _};

/// Update sequence as defined by RFD 565 §6.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, PartialOrd, Ord,
)]
pub enum OrderedComponent {
    HostOs,
    SpRot,
    ControlPlaneZone,
    NexusZone,
}

/// To implement the checks in RFD 565 §9, we need to go backwards
/// (and mabye forwards) through the component ordering. The simple
/// linear scans here are fine as long as `OrderedComponent` has only
/// a few variants; if it starts getting large (e.g., programatically
/// generated from `ls-apis`), we can switch to something faster.
impl OrderedComponent {
    #[allow(dead_code)]
    pub fn next(&self) -> Option<Self> {
        let mut found = false;
        for comp in OrderedComponent::iter() {
            if found {
                return Some(comp);
            } else if comp == *self {
                found = true;
            }
        }
        None
    }

    pub fn prev(&self) -> Option<Self> {
        let mut prev = None;
        for comp in OrderedComponent::iter() {
            if comp == *self {
                return prev;
            }
            prev = Some(comp);
        }
        prev
    }
}

impl From<ZoneKind> for OrderedComponent {
    fn from(zone_kind: ZoneKind) -> Self {
        match zone_kind {
            ZoneKind::Nexus => Self::NexusZone,
            _ => Self::ControlPlaneZone,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ordered_component_next_prev() {
        // Exhaustive checks ok for a few variants, revisit if it grows.
        assert_eq!(OrderedComponent::HostOs.prev(), None);
        assert_eq!(
            OrderedComponent::HostOs.next(),
            Some(OrderedComponent::SpRot)
        );
        assert_eq!(
            OrderedComponent::SpRot.prev(),
            Some(OrderedComponent::HostOs)
        );
        assert_eq!(
            OrderedComponent::SpRot.next(),
            Some(OrderedComponent::ControlPlaneZone)
        );
        assert_eq!(
            OrderedComponent::ControlPlaneZone.prev(),
            Some(OrderedComponent::SpRot)
        );
        assert_eq!(
            OrderedComponent::ControlPlaneZone.next(),
            Some(OrderedComponent::NexusZone)
        );
        assert_eq!(
            OrderedComponent::NexusZone.prev(),
            Some(OrderedComponent::ControlPlaneZone)
        );
        assert_eq!(OrderedComponent::NexusZone.next(), None);
        assert!(OrderedComponent::HostOs < OrderedComponent::NexusZone);
    }

    #[test]
    fn ordered_component_from_zone_kind() {
        assert!(matches!(
            OrderedComponent::from(ZoneKind::CruciblePantry),
            OrderedComponent::ControlPlaneZone
        ));
        assert!(matches!(
            OrderedComponent::from(ZoneKind::Nexus),
            OrderedComponent::NexusZone
        ));
    }
}
