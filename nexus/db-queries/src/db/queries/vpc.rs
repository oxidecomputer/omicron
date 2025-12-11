// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Queries for inserting a candidate, incomplete VPC

use crate::db::column_walker::AllColumnsOf;
use crate::db::model::IncompleteVpc;
use crate::db::model::Vni;
use crate::db::queries::next_item::DefaultShiftGenerator;
use crate::db::queries::next_item::NextItem;
use diesel::sql_types;
use omicron_common::api::external;

#[derive(Debug)]
enum VniSubquery {
    /// Attempt to insert VPC with a fixed VNI.
    Fixed(i32),
    /// Use a `NextItem` sub-query to pick the next available VNI.
    Next(NextVni),
}

/// A query used to insert a candidate VPC into the database.
///
/// This query mostly just inserts the data provided in the `IncompleteVpc`
/// argument. However, it selects a random, available VNI for the VPC, should
/// one be available.
#[derive(Debug)]
pub struct InsertVpcQuery {
    pub(crate) vpc: IncompleteVpc,
    vni_subquery: VniSubquery,
}

type AllVpcColumns = AllColumnsOf<nexus_db_schema::schema::vpc::table>;

impl InsertVpcQuery {
    pub fn new(vpc: IncompleteVpc) -> Self {
        let vni_subquery = NextVni::new(vpc.vni);
        Self { vpc, vni_subquery: VniSubquery::Next(vni_subquery) }
    }

    pub fn new_system(mut vpc: IncompleteVpc, vni: Option<Vni>) -> Self {
        let vni_subquery = match vni {
            // If an explicit VNI was provided, we want to use it.
            Some(vni) => {
                vpc.vni = vni;
                // Ok to unwrap here since the `Vni` type guarantees we fit
                VniSubquery::Fixed(i32::try_from(u32::from(vni.0)).unwrap())
            }
            // Otherwise, starting from a random seed, use a query to
            // select the next available system VNI.
            None => {
                vpc.vni = Vni(external::Vni::random_system());
                VniSubquery::Next(NextVni::new_system(vpc.vni))
            }
        };
        Self { vpc, vni_subquery }
    }

    /// Builds the complete INSERT query using QueryBuilder.
    pub fn to_insert_query(
        self,
    ) -> crate::db::raw_query_builder::TypedSqlQuery<
        crate::db::raw_query_builder::SelectableSql<crate::db::model::Vpc>,
    > {
        use crate::db::raw_query_builder::QueryBuilder;

        let mut builder = QueryBuilder::new();

        builder.sql("INSERT INTO vpc (");
        builder.sql(AllVpcColumns::as_str());
        builder.sql(") ");
        builder.sql("SELECT ");
        builder.param().bind::<sql_types::Uuid, _>(self.vpc.identity.id);
        builder.sql(", ");
        builder.param().bind::<sql_types::Text, _>(self.vpc.identity.name);
        builder.sql(", ");
        builder
            .param()
            .bind::<sql_types::Text, _>(self.vpc.identity.description);
        builder.sql(", ");
        builder
            .param()
            .bind::<sql_types::Timestamptz, _>(self.vpc.identity.time_created);
        builder.sql(", ");
        builder
            .param()
            .bind::<sql_types::Timestamptz, _>(self.vpc.identity.time_modified);
        builder.sql(", ");
        builder.param().bind::<sql_types::Nullable<sql_types::Timestamptz>, _>(
            self.vpc.identity.time_deleted,
        );
        builder.sql(", ");
        builder.param().bind::<sql_types::Uuid, _>(self.vpc.project_id);
        builder.sql(", ");
        builder.param().bind::<sql_types::Uuid, _>(self.vpc.system_router_id);
        builder.sql(", ");

        // VNI - either fixed or from NextItem query
        match self.vni_subquery {
            VniSubquery::Fixed(vni) => {
                builder.param().bind::<sql_types::Int4, _>(vni);
            }
            VniSubquery::Next(vni_subquery) => {
                builder.sql("(");
                vni_subquery.inner.build_query(&mut builder);
                builder.sql(")");
            }
        }
        builder.sql(", ");

        builder.param().bind::<sql_types::Inet, _>(self.vpc.ipv6_prefix);
        builder.sql(", ");
        builder.param().bind::<sql_types::Text, _>(self.vpc.dns_name);
        builder.sql(", ");
        builder.param().bind::<sql_types::Int8, _>(self.vpc.firewall_gen);
        builder.sql(", ");
        builder.param().bind::<sql_types::Int8, _>(self.vpc.subnet_gen);

        builder.sql(" RETURNING ");
        builder.sql(AllVpcColumns::as_str());

        builder.query()
    }
}

/// A `NextItem` query to select a Geneve Virtual Network Identifier (VNI) for a
/// new VPC.
struct NextVni {
    inner: NextItem<Vni, DefaultShiftGenerator<Vni>>,
}

impl NextVni {
    fn new(vni: Vni) -> Self {
        let VniShifts { min_shift, max_shift } = VniShifts::new(vni);
        let generator = DefaultShiftGenerator::new(vni, max_shift, min_shift)
            .expect("invalid min/max shift");
        let inner = NextItem::new_unscoped("vpc", "vni", generator);
        Self { inner }
    }

    /// Returns a query fragment to select an available VNI from the Oxide-reserved space.
    fn new_system(vni: Vni) -> Self {
        let base_u32 = u32::from(vni.0);
        // System VNI's are in the range [0, 1024) so adjust appropriately.
        let max_shift =
            i64::from((external::Vni::MIN_GUEST_VNI - 1) - base_u32);
        let min_shift = i64::from(
            -i32::try_from(base_u32)
                .expect("Expected a valid VNI at this point"),
        );
        let generator = DefaultShiftGenerator::new(vni, max_shift, min_shift)
            .expect("invalid min/max shift");
        let inner = NextItem::new_unscoped("vpc", "vni", generator);
        Self { inner }
    }
}

impl std::fmt::Debug for NextVni {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NextVni").finish_non_exhaustive()
    }
}

// Helper type to compute the shift for a `NextItem` query to find VNIs.
#[derive(Clone, Copy, Debug, PartialEq)]
struct VniShifts {
    // The minimum `ShiftGenerator` shift.
    min_shift: i64,
    // The maximum `ShiftGenerator` shift.
    max_shift: i64,
}

/// Restrict the search for a VNI to a small range.
///
/// VNIs are pretty sparsely allocated (the number of VPCs), and the range is
/// quite large (24 bits). To avoid memory issues, we'll restrict a search
/// for an available VNI to a small range starting from the random starting
/// VNI.
//
// NOTE: This is very small for tests, to ensure we can accurately test the
// failure mode where there are no available VNIs.
#[cfg(not(test))]
pub const MAX_VNI_SEARCH_RANGE_SIZE: u32 = 2048;
#[cfg(test)]
pub const MAX_VNI_SEARCH_RANGE_SIZE: u32 = 10;

// Ensure that we cannot search a range that extends beyond the valid guest VNI
// range.
static_assertions::const_assert!(
    MAX_VNI_SEARCH_RANGE_SIZE
        <= (external::Vni::MAX_VNI - external::Vni::MIN_GUEST_VNI)
);

impl VniShifts {
    fn new(vni: Vni) -> Self {
        let base_u32 = u32::from(vni.0);
        let range_end = base_u32 + MAX_VNI_SEARCH_RANGE_SIZE;

        // Clamp the maximum shift at the distance to the maximum allowed VNI,
        // or the maximum of the range.
        let max_shift = i64::from(
            (external::Vni::MAX_VNI - base_u32).min(MAX_VNI_SEARCH_RANGE_SIZE),
        );

        // And any remaining part of the range wraps around starting at the
        // beginning.
        let min_shift = -i64::from(
            range_end.checked_sub(external::Vni::MAX_VNI).unwrap_or(0),
        );
        Self { min_shift, max_shift }
    }
}

/// An iterator yielding sequential starting VNIs.
///
/// The VPC insertion query requires a search for the next available VNI, using
/// the `NextItem` query. We limit the search for each query to avoid memory
/// issues on any one query. If we fail to find a VNI, we need to search the
/// next range. This iterator yields the starting positions for the `NextItem`
/// query, so that the entire range can be search in chunks until a free VNI is
/// found.
//
// NOTE: It's technically possible for this to lead to searching the very
// initial portion of the range twice. If we end up wrapping around so that the
// last position yielded by this iterator is `start - x`, then we'll end up
// searching from `start - x` to `start + (MAX_VNI_SEARCH_RANGE_SIZE - x)`, and
// so search those first few after `start` again. This is both innocuous and
// really unlikely.
#[derive(Clone, Copy, Debug)]
pub struct VniSearchIter {
    start: u32,
    current: u32,
    has_wrapped: bool,
}

impl VniSearchIter {
    pub const STEP_SIZE: u32 = MAX_VNI_SEARCH_RANGE_SIZE;

    /// Create a search range, starting from the provided VNI.
    pub fn new(start: external::Vni) -> Self {
        let start = u32::from(start);
        Self { start, current: start, has_wrapped: false }
    }
}

impl std::iter::Iterator for VniSearchIter {
    type Item = external::Vni;

    fn next(&mut self) -> Option<Self::Item> {
        // If we've wrapped around and the computed position is beyond where we
        // started, then the ite
        if self.has_wrapped && self.current > self.start {
            return None;
        }

        // Compute the next position.
        //
        // Make sure we wrap around to the mininum guest VNI. Note that we
        // consider the end of the range inclusively, so we subtract one in the
        // offset below to end up _at_ the min guest VNI.
        let mut next = self.current + MAX_VNI_SEARCH_RANGE_SIZE;
        if next > external::Vni::MAX_VNI {
            next -= external::Vni::MAX_VNI;
            next += external::Vni::MIN_GUEST_VNI - 1;
            self.has_wrapped = true;
        }
        let current = self.current;
        self.current = next;
        Some(external::Vni::try_from(current).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::MAX_VNI_SEARCH_RANGE_SIZE;
    use super::Vni;
    use super::VniSearchIter;
    use super::VniShifts;
    use super::external;

    // Ensure that when the search range lies entirely within the range of VNIs,
    // we search from the start VNI through the maximum allowed range size.
    #[test]
    fn test_vni_shift_no_wrapping() {
        let vni = Vni(external::Vni::try_from(2048).unwrap());
        let VniShifts { min_shift, max_shift } = VniShifts::new(vni);
        assert_eq!(min_shift, 0);
        assert_eq!(max_shift, i64::from(MAX_VNI_SEARCH_RANGE_SIZE));
        assert_eq!(max_shift - min_shift, i64::from(MAX_VNI_SEARCH_RANGE_SIZE));
    }

    // Ensure that we wrap correctly, when the starting VNI happens to land
    // quite close to the end of the allowed range.
    #[test]
    fn test_vni_shift_with_wrapping() {
        let offset = 5;
        let vni =
            Vni(external::Vni::try_from(external::Vni::MAX_VNI - offset)
                .unwrap());
        let VniShifts { min_shift, max_shift } = VniShifts::new(vni);
        assert_eq!(min_shift, -i64::from(MAX_VNI_SEARCH_RANGE_SIZE - offset));
        assert_eq!(max_shift, i64::from(offset));
        assert_eq!(max_shift - min_shift, i64::from(MAX_VNI_SEARCH_RANGE_SIZE));
    }

    #[test]
    fn test_vni_search_iter_steps() {
        let start = external::Vni::try_from(2048).unwrap();
        let mut it = VniSearchIter::new(start);
        let next = it.next().unwrap();
        assert_eq!(next, start);
        let next = it.next().unwrap();
        assert_eq!(
            next,
            external::Vni::try_from(
                u32::from(start) + MAX_VNI_SEARCH_RANGE_SIZE
            )
            .unwrap()
        );
    }

    #[test]
    fn test_vni_search_iter_full_count() {
        let start =
            external::Vni::try_from(external::Vni::MIN_GUEST_VNI).unwrap();

        let last = VniSearchIter::new(start).last().unwrap();
        println!("{:?}", last);

        pub const fn div_ceil(x: u32, y: u32) -> u32 {
            let d = x / y;
            let r = x % y;
            if r > 0 && y > 0 { d + 1 } else { d }
        }
        const N_EXPECTED: u32 = div_ceil(
            external::Vni::MAX_VNI - external::Vni::MIN_GUEST_VNI,
            MAX_VNI_SEARCH_RANGE_SIZE,
        );
        let count = u32::try_from(VniSearchIter::new(start).count()).unwrap();
        assert_eq!(count, N_EXPECTED);
    }

    #[test]
    fn test_vni_search_iter_wrapping() {
        // Start from just before the end of the range.
        let start =
            external::Vni::try_from(external::Vni::MAX_VNI - 1).unwrap();
        let mut it = VniSearchIter::new(start);

        // We should yield that start position first.
        let next = it.next().unwrap();
        assert_eq!(next, start);

        // The next value should be wrapped around to the beginning.
        //
        // Subtract 2 because we _include_ the max VNI in the search range.
        let next = it.next().unwrap();
        assert_eq!(
            u32::from(next),
            external::Vni::MIN_GUEST_VNI + MAX_VNI_SEARCH_RANGE_SIZE - 2
        );
    }
}
