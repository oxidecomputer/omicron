// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An API modelled after [`syn::visit`](https://docs.rs/syn/1/syn/visit).
use crate::deployment::Blueprint;
use diffus::edit::{collection, enm, map, Edit};
use diffus::Diffable;
use uuid::Uuid;

use super::EditedBlueprint;

pub struct Change<'edit, T> {
    before: &'edit T,
    after: &'edit T,
}

pub trait Visit<'edit> {
    fn visit_blueprint_edit(&mut self, node: &'edit Edit<'edit, Blueprint>) {
        visit_blueprint_edit(self, node);
    }
    fn visit_blueprint_copy(&mut self, node: &'edit Blueprint) {}
    fn visit_blueprint_change(&mut self, node: &'edit EditedBlueprint) {}
    fn visit_blueprint_id_copy(&mut self, node: &'edit Uuid) {}
    fn visit_bluprint_id_change(&mut self, node: &'edit Change<Uuid>) {}
}

pub fn visit_blueprint_edit<'edit, V>(
    v: &mut V,
    node: &'edit Edit<'edit, Blueprint>,
) where
    V: Visit<'edit> + ?Sized,
{
    match node {
        Edit::Copy(blueprint) => v.visit_blueprint_copy(*blueprint),
        Edit::Change(edited_blueprint) => {
            v.visit_blueprint_change(edited_blueprint)
        }
    }
}

pub fn visit_blueprint_copy<'edit, V>(v: &mut V, node: &'edit Blueprint)
where
    V: Visit<'edit> + ?Sized,
{
}

pub fn visit_blueprint_change<'edit, V>(v: &mut V, node: &'edit EditedBlueprint)
where
    V: Visit<'edit> + ?Sized,
{
}

/// A visitor for debug printing walks of a blueprint
pub struct DebugVisitor;

impl<'edit> Visit<'edit> for DebugVisitor {
    fn visit_blueprint_edit(&mut self, node: &'edit Edit<'edit, Blueprint>) {
        println!("blueprint edit");

        visit_blueprint_edit(self, node);
    }

    fn visit_blueprint_copy(&mut self, node: &'edit Blueprint) {
        println!("blueprint copy");
        visit_blueprint_copy(self, node);
    }

    fn visit_blueprint_change(&mut self, node: &'edit EditedBlueprint) {
        println!("blueprint change");
        visit_blueprint_change(self, node);
    }
}
