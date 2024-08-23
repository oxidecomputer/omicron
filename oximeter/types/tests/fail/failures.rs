// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[derive(oximeter::Target)]
pub struct WrongTargetFieldType {
    pub x: f32,
}

#[derive(oximeter::Metric)]
pub struct WrongMetricFieldType {
    pub x: f32,
    pub value: f64,
}

#[derive(oximeter::Metric)]
pub struct WrongMetricDataType {
    pub value: f32,
}

#[derive(oximeter::Target)]
pub enum CantUseEnum {
    A,
    B,
}

#[derive(oximeter::Target)]
pub struct CantUseTupleStruct(i64);

#[derive(oximeter::Metric)]
pub enum CantUseEnumMetric {
    A,
}

#[derive(oximeter::Target)]
pub struct CantUseTupleStructMetric(i64);

#[derive(oximeter::Metric)]
pub struct NeedValueField {
    pub x: i64,
}

fn main() { }
