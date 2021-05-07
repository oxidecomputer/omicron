use oximeter::{metric, Error, Measurement, Producer, Target};
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

#[metric("cumulative", "DistributionF64")]
#[derive(Debug, Clone)]
pub struct IoLatency {
    pub volume: String,
}

#[allow(dead_code)]
impl IoLatency {
    pub fn new() -> Self {
        Self {
            volume: "some-volume-name".to_string(),
        }
    }
}

#[metric("gauge", "i64")]
#[derive(Debug, Clone)]
pub struct CpuBusy {
    pub cpu_id: i64,
}

#[allow(dead_code)]
impl CpuBusy {
    pub fn new() -> Self {
        CpuBusy { cpu_id: 0 }
    }
}

#[metric("cumulative", "i64")]
#[derive(Debug, Clone)]
pub struct Interrupts {
    pub cpu_id: i64,
}

#[allow(dead_code)]
impl Interrupts {
    pub fn new() -> Self {
        Interrupts { cpu_id: 0 }
    }
}

#[derive(Debug, Clone)]
pub struct CpuSampler {
    count: usize,
    pub value: i64,
}

#[allow(dead_code)]
impl CpuSampler {
    pub fn new(count: usize) -> Self {
        Self { count, value: 0 }
    }
}

impl Producer for CpuSampler {
    fn setup_collection(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn collect(&mut self) -> Result<Vec<Measurement>, Error> {
        let mut results = Vec::with_capacity(self.count);
        for _ in 0..self.count {
            results.push(Measurement::I64(self.value));
            self.value += 1;
        }
        Ok(results)
    }
}
