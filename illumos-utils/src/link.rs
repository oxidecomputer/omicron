// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for allocating and managing data links.

use crate::destructor::{Deletable, Destructor};
use crate::dladm::{
    CreateVnicError, DeleteVnicError, VNIC_PREFIX, VNIC_PREFIX_BOOTSTRAP,
    VNIC_PREFIX_CONTROL, VnicSource,
};
use omicron_common::api::external::MacAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// A shareable wrapper around an atomic counter.
/// May be used to allocate runtime-unique IDs for objects
/// which have naming constraints - such as VNICs.
#[derive(Clone)]
pub struct VnicAllocator<DL: VnicSource + 'static> {
    value: Arc<AtomicU64>,
    scope: String,
    data_link: DL,
    dladm: Arc<dyn crate::dladm::Api>,
    // Manages dropped Vnics, and repeatedly attempts to delete them.
    destructor: Destructor<VnicDestruction>,
}

impl<DL: VnicSource + Clone> VnicAllocator<DL> {
    /// Creates a new Vnic name allocator with a particular scope.
    ///
    /// The intent with varying scopes is to create non-overlapping
    /// ranges of Vnic names, for example:
    ///
    /// VnicAllocator::new("Instance")
    /// - oxGuestInstance0
    /// - oxControlInstance0
    ///
    /// VnicAllocator::new("Storage") produces
    /// - oxControlStorage0
    pub fn new<S: AsRef<str>>(
        scope: S,
        data_link: DL,
        dladm: Arc<dyn crate::dladm::Api>,
    ) -> Self {
        Self {
            value: Arc::new(AtomicU64::new(0)),
            scope: scope.as_ref().to_string(),
            data_link,
            dladm,
            destructor: Destructor::new(),
        }
    }

    /// Creates a new NIC, intended for allowing Propolis to communicate
    /// with the control plane.
    pub async fn new_control(
        &self,
        mac: Option<MacAddr>,
    ) -> Result<Link, CreateVnicError> {
        let allocator = self.new_superscope("Control");
        let name = allocator.next();
        debug_assert!(name.starts_with(VNIC_PREFIX));
        debug_assert!(name.starts_with(VNIC_PREFIX_CONTROL));
        self.dladm.create_vnic(&self.data_link, &name, mac, None, 9000).await?;
        Ok(Link {
            name,
            deleted: false,
            kind: LinkKind::OxideControlVnic,
            api: Some(self.dladm.clone()),
            destructor: Some(self.destructor.clone()),
        })
    }

    /// Takes ownership of an existing VNIC.
    pub fn wrap_existing<S: AsRef<str>>(
        &self,
        name: S,
    ) -> Result<Link, InvalidLinkKind> {
        match LinkKind::from_name(name.as_ref()) {
            Some(kind) => Ok(Link {
                name: name.as_ref().to_owned(),
                deleted: false,
                kind,
                api: Some(self.dladm.clone()),
                destructor: Some(self.destructor.clone()),
            }),
            None => Err(InvalidLinkKind(name.as_ref().to_owned())),
        }
    }

    fn new_superscope<S: AsRef<str>>(&self, scope: S) -> Self {
        Self {
            value: self.value.clone(),
            scope: format!("{}{}", scope.as_ref(), self.scope),
            data_link: self.data_link.clone(),
            dladm: self.dladm.clone(),
            destructor: self.destructor.clone(),
        }
    }

    pub async fn new_bootstrap(&self) -> Result<Link, CreateVnicError> {
        let name = self.next();
        self.dladm
            .create_vnic(&self.data_link, &name, None, None, 1500)
            .await?;
        Ok(Link {
            name,
            deleted: false,
            kind: LinkKind::OxideBootstrapVnic,
            api: Some(self.dladm.clone()),
            destructor: Some(self.destructor.clone()),
        })
    }

    /// Allocates a new VNIC name, which should be unique within the
    /// scope of this allocator.
    fn next(&self) -> String {
        format!("{}{}{}", VNIC_PREFIX, self.scope, self.next_id())
    }

    fn next_id(&self) -> u64 {
        self.value.fetch_add(1, Ordering::SeqCst)
    }
}

/// Represents the kind of a Link, such as whether it's for guest networking or
/// communicating with Oxide services.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkKind {
    Physical,
    OxideControlVnic,
    OxideBootstrapVnic,
}

impl LinkKind {
    /// Infer the kind from a VNIC's name, if this one the sled agent can
    /// manage, and `None` otherwise.
    pub fn from_name(name: &str) -> Option<Self> {
        if name.starts_with(VNIC_PREFIX) {
            Some(LinkKind::OxideControlVnic)
        } else if name.starts_with(VNIC_PREFIX_BOOTSTRAP) {
            Some(LinkKind::OxideBootstrapVnic)
        } else {
            None
        }
    }

    /// Return `true` if this link is a VNIC.
    pub const fn is_vnic(&self) -> bool {
        match self {
            LinkKind::Physical => false,
            LinkKind::OxideControlVnic | LinkKind::OxideBootstrapVnic => true,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("VNIC with name '{0}' is not valid for sled agent management")]
pub struct InvalidLinkKind(String);

/// Represents an allocated VNIC on the system.
/// The VNIC is de-allocated when it goes out of scope.
///
/// Note that the "ownership" of the VNIC is based on convention;
/// another process in the global zone could also modify / destroy
/// the VNIC while this object is alive.
pub struct Link {
    name: String,
    deleted: bool,
    kind: LinkKind,
    api: Option<Arc<dyn crate::dladm::Api>>,
    destructor: Option<Destructor<VnicDestruction>>,
}

impl std::fmt::Debug for Link {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("Link")
            .field("name", &self.name)
            .field("deleted", &self.deleted)
            .field("kind", &self.kind)
            .finish()
    }
}

impl Link {
    /// Wraps a physical nic in a Link structure.
    ///
    /// It is the caller's responsibility to ensure this is a physical link.
    pub fn wrap_physical<S: AsRef<str>>(name: S) -> Self {
        Link {
            name: name.as_ref().to_owned(),
            deleted: false,
            kind: LinkKind::Physical,
            api: None,
            destructor: None,
        }
    }

    /// Deletes a NIC (if it has not already been deleted).
    pub async fn delete(&mut self) -> Result<(), DeleteVnicError> {
        if self.deleted || self.kind == LinkKind::Physical {
            Ok(())
        } else {
            self.api.as_ref().unwrap().delete_vnic(&self.name).await?;
            self.deleted = true;
            Ok(())
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> LinkKind {
        self.kind
    }

    /// Return true if this is a VNIC.
    pub fn is_vnic(&self) -> bool {
        self.kind.is_vnic()
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        if let Some(destructor) = self.destructor.take() {
            destructor.enqueue_destroy(VnicDestruction {
                name: self.name.clone(),
                api: self.api.take(),
            });
        }
    }
}

// Represents the request to destroy a VNIC
struct VnicDestruction {
    name: String,
    api: Option<Arc<dyn crate::dladm::Api>>,
}

#[async_trait::async_trait]
impl Deletable for VnicDestruction {
    async fn delete(&self) -> Result<(), anyhow::Error> {
        if let Some(api) = self.api.as_ref() {
            api.delete_vnic(&self.name).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dladm::Etherstub;

    #[tokio::test]
    async fn test_allocate() {
        let allocator = VnicAllocator::new(
            "Foo",
            Etherstub("mystub".to_string()),
            crate::fakes::dladm::Dladm::new(),
        );
        assert_eq!("oxFoo0", allocator.next());
        assert_eq!("oxFoo1", allocator.next());
        assert_eq!("oxFoo2", allocator.next());
    }

    #[tokio::test]
    async fn test_allocate_within_scopes() {
        let allocator = VnicAllocator::new(
            "Foo",
            Etherstub("mystub".to_string()),
            crate::fakes::dladm::Dladm::new(),
        );
        assert_eq!("oxFoo0", allocator.next());
        let allocator = allocator.new_superscope("Baz");
        assert_eq!("oxBazFoo1", allocator.next());
    }
}
