// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types related to zone bundles.

use std::cmp::Ordering;

use camino::Utf8PathBuf;

pub use sled_agent_types_migrations::latest::zone_bundle::*;

/// Information about a zone bundle.
///
/// This type is not published in the API, so it remains defined here
/// rather than in the migrations crate.
#[derive(Clone, Debug, PartialEq)]
pub struct ZoneBundleInfo {
    /// The raw metadata for the bundle
    pub metadata: ZoneBundleMetadata,
    /// The full path to the bundle
    pub path: Utf8PathBuf,
    /// The number of bytes consumed on disk by the bundle
    pub bytes: u64,
}

/// Order zone bundle info according to the contained priority.
///
/// We sort the info by each dimension, in the order in which it appears.
/// That means earlier dimensions have higher priority than later ones.
pub fn compare_bundles(
    order: &PriorityOrder,
    lhs: &ZoneBundleInfo,
    rhs: &ZoneBundleInfo,
) -> Ordering {
    order.compare_metadata(&lhs.metadata, &rhs.metadata)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn test_compare_bundles() {
        use PriorityDimension::*;
        let time_first = PriorityOrder::new(&[Time, Cause]).unwrap();
        let cause_first = PriorityOrder::new(&[Cause, Time]).unwrap();

        fn make_info(
            year: i32,
            month: u32,
            day: u32,
            cause: ZoneBundleCause,
        ) -> ZoneBundleInfo {
            ZoneBundleInfo {
                metadata: ZoneBundleMetadata {
                    id: ZoneBundleId {
                        zone_name: String::from("oxz_whatever"),
                        bundle_id: uuid::Uuid::new_v4(),
                    },
                    time_created: Utc
                        .with_ymd_and_hms(year, month, day, 0, 0, 0)
                        .single()
                        .unwrap(),
                    cause,
                    version: 0,
                },
                path: Utf8PathBuf::from("/some/path"),
                bytes: 0,
            }
        }

        let info = [
            make_info(2020, 1, 2, ZoneBundleCause::UnexpectedZone),
            make_info(2020, 1, 2, ZoneBundleCause::TerminatedInstance),
            make_info(2020, 1, 1, ZoneBundleCause::UnexpectedZone),
            make_info(2020, 1, 1, ZoneBundleCause::TerminatedInstance),
        ];

        let mut sorted = info.clone();
        sorted.sort_by(|lhs, rhs| compare_bundles(&time_first, lhs, rhs));
        // Low -> high priority
        // [old/unexpected, old/terminated, new/unexpected, new/terminated]
        let expected = [
            info[2].clone(),
            info[3].clone(),
            info[0].clone(),
            info[1].clone(),
        ];
        assert_eq!(
            sorted, expected,
            "sorting zone bundles by time-then-cause failed"
        );

        let mut sorted = info.clone();
        sorted.sort_by(|lhs, rhs| compare_bundles(&cause_first, lhs, rhs));
        // Low -> high priority
        // [old/unexpected, new/unexpected, old/terminated, new/terminated]
        let expected = [
            info[2].clone(),
            info[0].clone(),
            info[3].clone(),
            info[1].clone(),
        ];
        assert_eq!(
            sorted, expected,
            "sorting zone bundles by cause-then-time failed"
        );
    }
}
