// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use oximeter_types_versions::latest::histogram::HistogramSupport;
pub use oximeter_types_versions::latest::traits::*;

// These tests live here instead of in `oximeter_types_versions` because
// `oximeter_macro_impl` wants to use paths from this crate, not
// `oximeter_types_versions`.
#[cfg(test)]
mod tests {
    use crate::types;
    use crate::{
        Datum, DatumType, FieldType, FieldValue, Metric, MetricsError,
        Producer, Target,
    };
    use oximeter_macro_impl::{Metric, Target};
    use std::boxed::Box;

    #[derive(Debug, Clone, Target)]
    struct Targ {
        pub happy: bool,
        pub tid: i64,
    }

    #[derive(Debug, Clone, Metric)]
    struct Met {
        good: bool,
        id: i64,
        datum: i64,
    }

    #[derive(Debug, Clone)]
    struct Prod {
        pub target: Targ,
        pub metric: Met,
    }

    impl Producer for Prod {
        fn produce(
            &mut self,
        ) -> Result<Box<dyn Iterator<Item = types::Sample>>, MetricsError>
        {
            Ok(Box::new(
                vec![types::Sample::new(&self.target, &self.metric)?]
                    .into_iter(),
            ))
        }
    }

    #[test]
    fn test_target_trait() {
        let t = Targ { happy: false, tid: 2 };

        assert_eq!(t.name(), "targ");
        assert_eq!(t.field_names(), &["happy", "tid"]);
        assert_eq!(t.field_types(), &[FieldType::Bool, FieldType::I64]);
        assert_eq!(
            t.field_values(),
            &[FieldValue::Bool(false), FieldValue::I64(2)]
        );
    }

    #[test]
    fn test_metric_trait() {
        let m = Met { good: false, id: 2, datum: 0 };
        assert_eq!(m.name(), "met");
        assert_eq!(m.field_names(), &["good", "id"]);
        assert_eq!(m.field_types(), &[FieldType::Bool, FieldType::I64]);
        assert_eq!(
            m.field_values(),
            &[FieldValue::Bool(false), FieldValue::I64(2)]
        );
        assert_eq!(m.datum_type(), DatumType::I64);
        assert!(m.start_time().is_none());
    }

    #[test]
    fn test_producer_trait() {
        let t = Targ { happy: false, tid: 2 };
        let m = Met { good: false, id: 2, datum: 0 };
        let mut p = Prod { target: t.clone(), metric: m.clone() };
        let sample = p.produce().unwrap().next().unwrap();
        assert_eq!(
            sample.timeseries_name,
            format!("{}:{}", t.name(), m.name())
        );
        assert_eq!(sample.measurement.datum(), &Datum::I64(0));
        p.metric.datum += 10;
        let sample = p.produce().unwrap().next().unwrap();
        assert_eq!(sample.measurement.datum(), &Datum::I64(10));
    }
}
