// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::app::background::BackgroundTask;
use futures::future::BoxFuture;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use std::sync::Arc;

// The Deliverator belongs to an elite order, a hallowed sub-category. He's got
// esprit up to here. Right now he is preparing to carry out his third mission
// of the night. His uniform is black as activated charcoal, filtering the very
// light out of the air. A bullet will bounce off its arachno-fiber weave like a
// wren hitting a patio door, but excess perspiration wafts through it like a
// breeze through a freshly napalmed forest. Where his body has bony
// extremities, the suit has sintered armorgel: feels like gritty jello,
// protects like a stack of telephone books.
//
// When they gave him the job, they gave him a gun. The Deliverator never deals
// in cash, but someone might come after him anyway–might want his car, or his
// cargo. The gun is tiny, aero-styled, lightweight, the kind of a gun a
// fashion designer would carry; it fires teensy darts that fly at five times
// the velocity of an SR-71 spy plane, and when you get done using it, you have
// to plug it in to the cigarette lighter, because it runs on electricity.
//
// The Deliverator never pulled that gun in anger, or in fear. He pulled it once
// in Gila Highlands. Some punks in Gila Highlands, a fancy Burbclave, wanted
// themselves a delivery, and they didn't want to pay for it. Thought they would
// impress the Deliverator with a baseball bat. The Deliverator took out his
// gun, centered its laser doo-hickey on that poised Louisville Slugger, fired
// it. The recoil was immense, as though the weapon had blown up in his hand.
// The middle third of the baseball bat turned into a column of burning sawdust
// accelerating in all directions like a bursting star. Punk ended up holding
// this bat handle with milky smoke pouring out the end. Stupid look on his
// face. Didn't get nothing but trouble from the Deliverator.
//
// Since then the Deliverator has kept the gun in the glove compartment and
// relied, instead, on a matched set of samurai swords, which have always been
// his weapon of choice anyhow. The punks in Gila Highlands weren't afraid of
// the gun, so the Deliverator was forced to use it. But swords need no
// demonstration.
//
// The Deliverator's car has enough potential energy packed into its batteries
// to fire a pound of bacon into the Asteroid Belt. Unlike a bimbo box or a Burb
// beater, the Deliverator's car unloads that power through gaping, gleaming,
// polished sphincters. When the Deliverator puts the hammer down, shit happens.
// You want to talk contact patches? Your car's tires have tiny contact patches,
// talk to the asphalt in four places the size of your tongue. The Deliverator's
// car has big sticky tires with contact patches the size of a fat lady's
// thighs. The Deliverator is in touch with the road, starts like a bad day,
// stops on a peseta.
//
// Why is the Deliverator so equipped? Because people rely on him. He is a role
// model.
//
// --- Neal Stephenson, _Snow Crash_
pub struct WebhookDeliverator {
    datastore: Arc<DataStore>,
}

impl BackgroundTask for WebhookDeliverator {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        Box::pin(
            async move { todo!("eliza: draw the rest of the deliverator") },
        )
    }
}

impl WebhookDeliverator {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        Self { datastore }
    }
}
