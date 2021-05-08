use oximeter::{metric, Target};
use uuid::Uuid;

#[derive(Debug, Clone, Target)]
pub struct Sled {
    pub name: String,
    pub id: Uuid,
}

#[allow(dead_code)]
impl Sled {
    pub fn new() -> Self {
        Sled {
            name: "a-sled-name".to_string(),
            id: Uuid::new_v4(),
        }
    }
}

#[derive(Debug, Clone, Target)]
pub struct Disk {
    pub name: String,
    pub id: Uuid,
}

#[allow(dead_code)]
impl Disk {
    pub fn new() -> Self {
        Self {
            name: "some-disk-name".to_string(),
            id: Uuid::new_v4(),
        }
    }
}

#[metric(i64)]
pub struct SimpleGauge {
    field: bool,
}

#[allow(dead_code)]
impl SimpleGauge {
    pub fn new() -> Self {
        Self { field: false }
    }
}
