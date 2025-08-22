// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Pagination support
//!
//! All list operations in the API are paginated, meaning that there's a limit on
//! the number of objects returned in a single request and clients are expected
//! to make additional requests to fetch the next page of results until the end
//! of the list is reached or the client has found what it needs.  For any list
//! operation, objects are sorted by a particular field that is unique among
//! objects in the list (usually a UTF-8 name or a UUID).  With each response,
//! the server will return a page of objects, plus a token that can be used to
//! fetch the next page.
//!
//! See Dropshot's pagination documentation for more background on this.
//!
//! For our API, we expect that most resources will support pagination in the
//! same way, which will include:
//!
//! * definitely: sorting in ascending order of the resource's "name"
//! * maybe in the future: sorting in descending order of the resource's "name"
//! * maybe in the future: sorting in ascending order of the resource's "id"
//! * maybe in the future: sorting in descending order of the resource's "id"
//! * maybe in the future: sorting in descending order of the resource's "mtime"
//!   and then "name" or "id"
//!
//! Dropshot's pagination support requires that we define the query parameters we
//! support with the first request ("scan params"), the information we need on
//! subsequent requests to resume a scan ("page selector"), and a way to generate
//! the page selector from a given object in the collection.  We can share these
//! definitions across as many resources as we want.  Below, we provide
//! definitions for resources that implement the `ObjectIdentity` trait.  With
//! these definitions, any type that has identity metadata can be paginated by
//! "name" in ascending order, "id" in ascending order, or either of those (plus
//! name in descending order) without any new boilerplate for that type.
//!
//! There may be resources that can't be paginated using one of the above three
//! ways, and we can define new ways to paginate them.  As you will notice below,
//! there's a fair bit of boilerplate for each way of paginating (rather than for
//! each resource paginated that way).  Where possible, we should share code.

use crate::api::external::DataPageParams;
use crate::api::external::Name;
use crate::api::external::NameOrId;
use crate::api::external::ObjectIdentity;
use crate::api::external::PaginationOrder;
use chrono::DateTime;
use chrono::Utc;
use dropshot::HttpError;
use dropshot::PaginationParams;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::WhichPage;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::num::NonZeroU32;
use uuid::Uuid;

use super::SimpleIdentity;
use super::SimpleIdentityOrName;

// General pagination infrastructure

/// Specifies which page of results we're on
///
/// This type is generic over the different scan modes that we support.
#[derive(Debug, Deserialize, JsonSchema, Serialize)]
pub struct PageSelector<ScanParams, MarkerType> {
    /// parameters describing the scan
    #[serde(flatten)]
    scan: ScanParams,
    /// value of the marker field last seen by the client
    last_seen: MarkerType,
}

/// Describes one of our supported scan modes
///
/// To minimize boilerplate, we provide common functions needed by our consumers
/// (e.g., `ScanParams::results_page`) as well as the Dropshot interface (e.g.,
/// `page_selector_for`).  This trait encapsulates the functionality that differs
/// among the different scan modes that we support.  Much of the functionality
/// here isn't so much a property of the Dropshot "scan parameters" as much as it
/// is specific to a scan using those parameters.  As a result, several of these
/// are associated functions rather than methods.
pub trait ScanParams:
    Clone + Debug + DeserializeOwned + JsonSchema + PartialEq + Serialize
{
    /// Type of the "marker" field for this scan mode
    ///
    /// For example, when scanning by name, this would be `Name`.
    type MarkerValue: Clone + Debug + DeserializeOwned + PartialEq + Serialize;

    /// Return the direction of the scan
    fn direction(&self) -> PaginationOrder;

    /// Given pagination parameters, return the current scan parameters
    ///
    /// This can fail if the pagination parameters are not self-consistent (e.g.,
    /// if the scan parameters indicate we're going in ascending order by name,
    /// but the marker is an id rather than a name).
    fn from_query(
        q: &PaginationParams<Self, PageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError>;

    /// Generate a page of results for a paginated endpoint that lists items of
    /// type `T`
    ///
    /// `list` contains the items that should appear on the page.  It's not
    /// expected that consumers would override this implementation.
    ///
    /// `marker_for_item` is a function that returns the appropriate marker
    /// value for a given item.  For example, when scanning by name, this
    /// returns the "name" field of the item.
    fn results_page<T, F>(
        query: &PaginationParams<Self, PageSelector<Self, Self::MarkerValue>>,
        list: Vec<T>,
        marker_for_item: &F,
    ) -> Result<ResultsPage<T>, dropshot::HttpError>
    where
        F: Fn(&Self, &T) -> Self::MarkerValue,
        T: Serialize,
    {
        let scan_params = Self::from_query(query)?;
        let page_selector =
            |item: &T, s: &Self| page_selector_for(item, s, marker_for_item);
        ResultsPage::new(list, scan_params, page_selector)
    }
}

/// Marker function that extracts the "name" from an object
///
/// This is intended for use with [`ScanByName::results_page`] with objects that
/// impl [`ObjectIdentity`].
pub fn marker_for_name<S, T: ObjectIdentity>(_: &S, t: &T) -> Name {
    t.identity().name.clone()
}

/// Marker function that extracts the "id" from an object
///
/// This is intended for use with [`ScanById::results_page`] with objects that
/// impl [`ObjectIdentity`].
pub fn marker_for_id<S, T: SimpleIdentity>(_: &S, t: &T) -> Uuid {
    t.id()
}

/// Marker function that extracts the "name" or "id" from an object, depending
/// on the scan in use
///
/// This is intended for use with [`ScanByNameOrId::results_page`] with objects
/// that impl [`ObjectIdentity`].
pub fn marker_for_name_or_id<T: SimpleIdentityOrName, Selector>(
    scan: &ScanByNameOrId<Selector>,
    item: &T,
) -> NameOrId {
    match scan.sort_by {
        NameOrIdSortMode::NameAscending => item.name().clone().into(),
        NameOrIdSortMode::NameDescending => item.name().clone().into(),
        NameOrIdSortMode::IdAscending => item.id().into(),
    }
}

/// See `dropshot::ResultsPage::new`
fn page_selector_for<F, T, S, M>(
    item: &T,
    scan_params: &S,
    marker_for_item: &F,
) -> PageSelector<S, M>
where
    F: Fn(&S, &T) -> M,
    S: ScanParams<MarkerValue = M>,
    M: Clone + Debug + DeserializeOwned + PartialEq + Serialize,
{
    PageSelector {
        scan: scan_params.clone(),
        last_seen: marker_for_item(scan_params, item),
    }
}

/// Given a request and pagination parameters, return a [`DataPageParams`]
/// describing the current page of results to return
pub fn data_page_params_for<'a, S, C>(
    rqctx: &'a RequestContext<C>,
    pag_params: &'a PaginationParams<S, PageSelector<S, S::MarkerValue>>,
) -> Result<DataPageParams<'a, S::MarkerValue>, HttpError>
where
    S: ScanParams,
    C: dropshot::ServerContext,
{
    let limit = rqctx.page_limit(pag_params)?;
    data_page_params_with_limit(limit, pag_params)
}

/// Provided separately from data_page_params_for() so that the test suite can
/// test the bulk of the logic without needing to cons up a Dropshot
/// `RequestContext` just to get the limit.
fn data_page_params_with_limit<S>(
    limit: NonZeroU32,
    pag_params: &PaginationParams<S, PageSelector<S, S::MarkerValue>>,
) -> Result<DataPageParams<'_, S::MarkerValue>, HttpError>
where
    S: ScanParams,
{
    let marker = match &pag_params.page {
        WhichPage::First(..) => None,
        WhichPage::Next(PageSelector { last_seen, .. }) => Some(last_seen),
    };
    let scan_params = S::from_query(pag_params)?;
    let direction = scan_params.direction();

    Ok(DataPageParams { marker, direction, limit })
}

// Pagination by name in ascending order only (most resources today)

/// Query parameters for pagination by name only
pub type PaginatedByName = PaginationParams<ScanByName, PageSelectorByName>;
/// Page selector for pagination by name only
pub type PageSelectorByName = PageSelector<ScanByName, Name>;
/// Scan parameters for resources that support scanning by name only
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ScanByName {
    #[serde(default = "default_name_sort_mode")]
    sort_by: NameSortMode,
}
/// Supported set of sort modes for scanning by name only
///
/// Currently, we only support scanning in ascending order.
#[derive(Copy, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameSortMode {
    /// sort in increasing order of "name"
    NameAscending,
}

fn default_name_sort_mode() -> NameSortMode {
    NameSortMode::NameAscending
}

impl ScanParams for ScanByName {
    type MarkerValue = Name;
    fn direction(&self) -> PaginationOrder {
        PaginationOrder::Ascending
    }
    fn from_query(
        p: &PaginationParams<Self, PageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(PageSelector { ref scan, .. }) => scan,
        })
    }
}

// Pagination by id in ascending order only (for some anonymous resources today)

/// Query parameters for pagination by id only
pub type PaginatedById<Selector = ()> =
    PaginationParams<ScanById<Selector>, PageSelectorById<Selector>>;
/// Page selector for pagination by name only
pub type PageSelectorById<Selector = ()> =
    PageSelector<ScanById<Selector>, Uuid>;
/// Scan parameters for resources that support scanning by id only
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ScanById<Selector = ()> {
    #[serde(default = "default_id_sort_mode")]
    sort_by: IdSortMode,
    #[serde(flatten)]
    pub selector: Selector,
}

/// Supported set of sort modes for scanning by id only.
///
/// Currently, we only support scanning in ascending order.
#[derive(Copy, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IdSortMode {
    /// sort in increasing order of "id"
    IdAscending,
}

fn default_id_sort_mode() -> IdSortMode {
    IdSortMode::IdAscending
}

impl<T: Clone + Debug + DeserializeOwned + JsonSchema + PartialEq + Serialize>
    ScanParams for ScanById<T>
{
    type MarkerValue = Uuid;
    fn direction(&self) -> PaginationOrder {
        PaginationOrder::Ascending
    }
    fn from_query(p: &PaginatedById<T>) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(PageSelector { ref scan, .. }) => scan,
        })
    }
}

// Pagination by any of: name ascending, name descending, or id ascending.
// We include this now primarily to exercise the interface for doing so.

/// Query parameters for pagination by name or id
pub type PaginatedByNameOrId<Selector = ()> = PaginationParams<
    ScanByNameOrId<Selector>,
    PageSelectorByNameOrId<Selector>,
>;
/// Page selector for pagination by name or id
pub type PageSelectorByNameOrId<Selector = ()> =
    PageSelector<ScanByNameOrId<Selector>, NameOrId>;

pub fn id_pagination<'a, Selector>(
    pag_params: &'a DataPageParams<Uuid>,
    scan_params: &'a ScanById<Selector>,
) -> Result<PaginatedBy<'a>, HttpError>
where
    Selector:
        Clone + Debug + DeserializeOwned + JsonSchema + PartialEq + Serialize,
{
    match scan_params.sort_by {
        IdSortMode::IdAscending => Ok(PaginatedBy::Id(pag_params.clone())),
    }
}

pub fn name_or_id_pagination<'a, Selector>(
    pag_params: &'a DataPageParams<NameOrId>,
    scan_params: &'a ScanByNameOrId<Selector>,
) -> Result<PaginatedBy<'a>, HttpError>
where
    Selector:
        Clone + Debug + DeserializeOwned + JsonSchema + PartialEq + Serialize,
{
    match scan_params.sort_by {
        NameOrIdSortMode::NameAscending => {
            Ok(PaginatedBy::Name(pag_params.try_into()?))
        }
        NameOrIdSortMode::NameDescending => {
            Ok(PaginatedBy::Name(pag_params.try_into()?))
        }
        NameOrIdSortMode::IdAscending => {
            Ok(PaginatedBy::Id(pag_params.try_into()?))
        }
    }
}

/// Scan parameters for resources that support scanning by name or id
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ScanByNameOrId<Selector> {
    #[serde(default = "default_nameid_sort_mode")]
    sort_by: NameOrIdSortMode,

    #[serde(flatten)]
    pub selector: Selector,
}
/// Supported set of sort modes for scanning by name or id
#[derive(Copy, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameOrIdSortMode {
    /// sort in increasing order of "name"
    NameAscending,
    /// sort in decreasing order of "name"
    NameDescending,
    /// sort in increasing order of "id"
    IdAscending,
}

fn default_nameid_sort_mode() -> NameOrIdSortMode {
    NameOrIdSortMode::NameAscending
}

fn bad_token_error() -> HttpError {
    HttpError::for_bad_request(None, String::from("invalid page token"))
}

#[derive(Debug)]
pub enum PaginatedBy<'a> {
    Id(DataPageParams<'a, Uuid>),
    Name(DataPageParams<'a, Name>),
}

impl<T: Clone + Debug + DeserializeOwned + JsonSchema + PartialEq + Serialize>
    ScanParams for ScanByNameOrId<T>
{
    type MarkerValue = NameOrId;

    fn direction(&self) -> PaginationOrder {
        match self.sort_by {
            NameOrIdSortMode::NameAscending => PaginationOrder::Ascending,
            NameOrIdSortMode::NameDescending => PaginationOrder::Descending,
            NameOrIdSortMode::IdAscending => PaginationOrder::Ascending,
        }
    }

    fn from_query(
        p: &PaginationParams<Self, PageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError> {
        match &p.page {
            WhichPage::First(scan_mode) => Ok(scan_mode),

            WhichPage::Next(PageSelectorByNameOrId {
                scan,
                last_seen: NameOrId::Name(_),
            }) => match scan.sort_by {
                NameOrIdSortMode::NameAscending => Ok(scan),
                NameOrIdSortMode::NameDescending => Ok(scan),
                NameOrIdSortMode::IdAscending => Err(()),
            },

            WhichPage::Next(PageSelectorByNameOrId {
                scan,
                last_seen: NameOrId::Id(_),
            }) => match scan.sort_by {
                NameOrIdSortMode::NameAscending => Err(()),
                NameOrIdSortMode::NameDescending => Err(()),
                NameOrIdSortMode::IdAscending => Ok(scan),
            },
        }
        .map_err(|_| bad_token_error())
    }
}

/// Query parameters for pagination by timestamp and ID
pub type PaginatedByTimeAndId<Selector = ()> = PaginationParams<
    ScanByTimeAndId<Selector>,
    PageSelectorByTimeAndId<Selector>,
>;
/// Page selector for pagination by timestamp and ID
pub type PageSelectorByTimeAndId<Selector = ()> =
    PageSelector<ScanByTimeAndId<Selector>, (DateTime<Utc>, Uuid)>;

/// Scan parameters for resources that support scanning by (timestamp, id)
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ScanByTimeAndId<Selector = ()> {
    #[serde(default = "default_ts_id_sort_mode")]
    sort_by: TimeAndIdSortMode,

    #[serde(flatten)]
    pub selector: Selector,
}

/// Supported set of sort modes for scanning by timestamp and ID
#[derive(Copy, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeAndIdSortMode {
    /// sort in increasing order of timestamp and ID, i.e., earliest first
    TimeAndIdAscending,
    /// sort in increasing order of timestamp and ID, i.e., most recent first
    TimeAndIdDescending,
}

fn default_ts_id_sort_mode() -> TimeAndIdSortMode {
    TimeAndIdSortMode::TimeAndIdAscending
}

impl<T: Clone + Debug + DeserializeOwned + JsonSchema + PartialEq + Serialize>
    ScanParams for ScanByTimeAndId<T>
{
    type MarkerValue = (DateTime<Utc>, Uuid);
    fn direction(&self) -> PaginationOrder {
        match self.sort_by {
            TimeAndIdSortMode::TimeAndIdAscending => PaginationOrder::Ascending,
            TimeAndIdSortMode::TimeAndIdDescending => {
                PaginationOrder::Descending
            }
        }
    }
    fn from_query(p: &PaginatedByTimeAndId<T>) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(PageSelector { ref scan, .. }) => scan,
        })
    }
}

#[cfg(test)]
mod test {
    use super::IdSortMode;
    use super::Name;
    use super::NameOrId;
    use super::NameOrIdSortMode;
    use super::NameSortMode;
    use super::PageSelector;
    use super::PageSelectorById;
    use super::PageSelectorByName;
    use super::PageSelectorByNameOrId;
    use super::PageSelectorByTimeAndId;
    use super::PaginatedBy;
    use super::PaginatedById;
    use super::PaginatedByName;
    use super::PaginatedByNameOrId;
    use super::PaginatedByTimeAndId;
    use super::ScanById;
    use super::ScanByName;
    use super::ScanByNameOrId;
    use super::ScanByTimeAndId;
    use super::ScanParams;
    use super::TimeAndIdSortMode;
    use super::data_page_params_with_limit;
    use super::marker_for_id;
    use super::marker_for_name;
    use super::marker_for_name_or_id;
    use super::page_selector_for;
    use crate::api::external::IdentityMetadata;
    use crate::api::external::ObjectIdentity;
    use crate::api::external::http_pagination::name_or_id_pagination;
    use chrono::DateTime;
    use chrono::TimeZone;
    use chrono::Utc;
    use dropshot::PaginationOrder;
    use dropshot::PaginationParams;
    use dropshot::WhichPage;
    use expectorate::assert_contents;
    use schemars::schema_for;
    use serde::Serialize;
    use serde_json::to_string_pretty;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    // It's important to verify the schema for the page selectors because this
    // is a part of our interface that does not appear in the OpenAPI spec
    // because it's obscured by Dropshot's automatic encoding of the page
    // selector.
    //
    // Below, we also check the schema for the scan parameters because it's easy
    // to do and useful to have the examples there.  We may want to remove this
    // if/when we add a test case that checks the entire OpenAPI schema for our
    // various APIs, since this will then be redundant.
    #[test]
    fn test_pagination_schemas() {
        let schemas = vec![
            ("scan parameters, scan by name only", schema_for!(ScanByName)),
            ("scan parameters, scan by id only", schema_for!(ScanById)),
            (
                "scan parameters, scan by name or id",
                schema_for!(ScanByNameOrId<()>),
            ),
            (
                "page selector, scan by name only",
                schema_for!(PageSelectorByName),
            ),
            ("page selector, scan by id only", schema_for!(PageSelectorById)),
            (
                "page selector, scan by name or id",
                schema_for!(PageSelectorByNameOrId),
            ),
            (
                "page selector, scan by time and id",
                schema_for!(PageSelectorByTimeAndId),
            ),
        ];

        let mut found_output = String::new();
        for (label, output) in schemas {
            found_output.push_str(&format!(
                "schema for pagination parameters: {}\n{}\n",
                label,
                to_string_pretty(&output).unwrap()
            ));
        }

        assert_contents("tests/output/pagination-schema.txt", &found_output);
    }

    // As much for illustration as anything, we check examples of the scan
    // parameters and page selectors here.
    #[test]
    fn test_pagination_examples() {
        let scan_by_id =
            ScanById { sort_by: IdSortMode::IdAscending, selector: () };
        let scan_by_name = ScanByName { sort_by: NameSortMode::NameAscending };
        let scan_by_nameid_name = ScanByNameOrId::<()> {
            sort_by: NameOrIdSortMode::NameAscending,
            selector: (),
        };
        let scan_by_nameid_id = ScanByNameOrId::<()> {
            sort_by: NameOrIdSortMode::IdAscending,
            selector: (),
        };
        let scan_by_time_and_id = ScanByTimeAndId::<()> {
            sort_by: TimeAndIdSortMode::TimeAndIdAscending,
            selector: (),
        };
        let id: Uuid = "61a78113-d3c6-4b35-a410-23e9eae64328".parse().unwrap();
        let name: Name = "bort".parse().unwrap();
        let time: DateTime<Utc> =
            Utc.with_ymd_and_hms(2025, 3, 20, 10, 30, 45).unwrap();
        let examples = vec![
            // scan parameters only
            ("scan by id ascending", to_string_pretty(&scan_by_id).unwrap()),
            (
                "scan by name ascending",
                to_string_pretty(&scan_by_name).unwrap(),
            ),
            (
                "scan by name or id, using id ascending",
                to_string_pretty(&scan_by_nameid_id).unwrap(),
            ),
            (
                "scan by name or id, using name ascending",
                to_string_pretty(&scan_by_nameid_name).unwrap(),
            ),
            (
                "scan by name or id, using name ascending",
                to_string_pretty(&scan_by_nameid_name).unwrap(),
            ),
            (
                "scan by time and id, ascending",
                to_string_pretty(&scan_by_time_and_id).unwrap(),
            ),
            // page selectors
            (
                "page selector: by id ascending",
                to_string_pretty(&PageSelectorById {
                    scan: scan_by_id,
                    last_seen: id,
                })
                .unwrap(),
            ),
            (
                "page selector: by name ascending",
                to_string_pretty(&PageSelectorByName {
                    scan: scan_by_name,
                    last_seen: name.clone(),
                })
                .unwrap(),
            ),
            (
                "page selector: by name or id, using id ascending",
                to_string_pretty(&PageSelectorByNameOrId {
                    scan: scan_by_nameid_id,
                    last_seen: NameOrId::Id(id),
                })
                .unwrap(),
            ),
            (
                "page selector: by name or id, using id ascending",
                to_string_pretty(&PageSelectorByNameOrId {
                    scan: scan_by_nameid_name,
                    last_seen: NameOrId::Name(name),
                })
                .unwrap(),
            ),
            (
                "page selector: by time and id, ascending",
                to_string_pretty(&PageSelectorByTimeAndId {
                    scan: scan_by_time_and_id,
                    last_seen: (time, id),
                })
                .unwrap(),
            ),
        ];

        let mut found_output = String::new();
        for (label, output) in examples {
            found_output.push_str(&format!(
                "example pagination parameters: {}\n{}\n",
                label, output
            ));
        }

        assert_contents("tests/output/pagination-examples.txt", &found_output);
    }

    #[derive(ObjectIdentity, Clone, Debug, PartialEq, Serialize)]
    struct MyThing {
        identity: IdentityMetadata,
    }

    fn list_of_things() -> Vec<MyThing> {
        (0..20)
            .map(|i| {
                let name = format!("thing{}", i).parse().unwrap();
                let now = Utc::now();
                MyThing {
                    identity: IdentityMetadata {
                        id: Uuid::new_v4(),
                        name,
                        description: String::from(""),
                        time_created: now,
                        time_modified: now,
                    },
                }
            })
            .collect()
    }

    /// Function for running a bunch of tests on a ScanParams type.
    #[allow(clippy::type_complexity)]
    fn test_scan_param_common<F, S>(
        list: &Vec<MyThing>,
        scan: &S,
        querystring: &str,
        item0_marker: &S::MarkerValue,
        itemlast_marker: &S::MarkerValue,
        scan_default: &S,
        marker_for_item: &F,
    ) -> (
        PaginationParams<S, PageSelector<S, S::MarkerValue>>,
        PaginationParams<S, PageSelector<S, S::MarkerValue>>,
    )
    where
        S: ScanParams,
        F: Fn(&S, &MyThing) -> S::MarkerValue,
    {
        let li = list.len() - 1;

        // Test basic parts of ScanParams interface.
        assert_eq!(&marker_for_item(scan, &list[0]), item0_marker);
        assert_eq!(&marker_for_item(scan, &list[li]), itemlast_marker);

        // Test page_selector_for().
        let page_selector = page_selector_for(&list[0], scan, marker_for_item);
        assert_eq!(&page_selector.scan, scan);
        assert_eq!(&page_selector.last_seen, item0_marker);

        let page_selector = page_selector_for(&list[li], scan, marker_for_item);
        assert_eq!(&page_selector.scan, scan);
        assert_eq!(&page_selector.last_seen, itemlast_marker);

        // Test from_query() with the default scan parameters.
        let p: PaginationParams<S, PageSelector<S, S::MarkerValue>> =
            serde_urlencoded::from_str("").unwrap();
        assert_eq!(S::from_query(&p).unwrap(), scan_default);

        // Test from_query() based on an explicit querystring corresponding to
        // the first page in a scan with "scan" as the scan parameters.
        let p0: PaginationParams<S, PageSelector<S, S::MarkerValue>> =
            serde_urlencoded::from_str(querystring).unwrap();
        assert_eq!(S::from_query(&p0).unwrap(), scan);

        // Generate a results page from that, verify it, pull the token out, and
        // use it to generate pagination parameters for a NextPage request.
        let page = S::results_page(&p0, list.clone(), marker_for_item).unwrap();
        assert_eq!(&page.items, list);
        assert!(page.next_page.is_some());
        let q = format!("page_token={}", page.next_page.unwrap());
        let p1: PaginationParams<S, PageSelector<S, S::MarkerValue>> =
            serde_urlencoded::from_str(&q).unwrap();

        // Now pull the information out of that, including the "last_seen"
        // marker.  This should match `itemlast_marker`.  That will tell us that
        // the results page was properly generated.
        assert_eq!(S::from_query(&p1).unwrap(), scan);
        if let WhichPage::Next(PageSelector { ref last_seen, .. }) = p1.page {
            assert_eq!(last_seen, itemlast_marker);
        } else {
            panic!("expected WhichPage::Next");
        }

        // Return these two sets of pagination parameters to the caller for more
        // testing.
        (p0, p1)
    }

    #[test]
    fn test_scan_by_name() {
        // Start with the common battery of tests.
        let scan = ScanByName { sort_by: NameSortMode::NameAscending };

        let list = list_of_things();
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=name_ascending",
            &"thing0".parse().unwrap(),
            &"thing19".parse().unwrap(),
            &scan,
            &marker_for_name,
        );
        assert_eq!(scan.direction(), PaginationOrder::Ascending);

        // Verify data pages based on the query params.
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker.unwrap().as_str(), "thing19");
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        // Test from_query(): error case.
        let error = serde_urlencoded::from_str::<PaginatedByName>(
            "sort_by=name_descending",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unknown variant `name_descending`, expected `name_ascending`"
        );
    }

    #[test]
    fn test_scan_by_id() {
        // Start with the common battery of tests.
        let scan = ScanById { sort_by: IdSortMode::IdAscending, selector: () };

        let list = list_of_things();
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=id_ascending",
            &list[0].identity.id,
            &list[list.len() - 1].identity.id,
            &scan,
            &marker_for_id,
        );
        assert_eq!(scan.direction(), PaginationOrder::Ascending);

        // Verify data pages based on the query params.
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker, Some(&list[19].identity.id));
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        // Test from_query(): error case.
        let error = serde_urlencoded::from_str::<PaginatedById>(
            "sort_by=id_descending",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unknown variant `id_descending`, expected `id_ascending`"
        );
    }

    #[test]
    fn test_scan_by_nameid_generic() {
        // Test from_query(): error case.
        let error = serde_urlencoded::from_str::<PaginatedByNameOrId>(
            "sort_by=id_descending",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unknown variant `id_descending`, expected one of \
             `name_ascending`, `name_descending`, `id_ascending`"
        );

        // TODO-coverage It'd be nice to exercise the from_query() error cases
        // where the scan params doesn't match the last_seen value kind.
        // However, we can't easily generate these, either directly or by
        // causing Dropshot to parse a querystring.  In the latter case, it
        // would have to be a page token that Dropshot generated, but by
        // design we can't get Dropshot to construct such a token.
    }

    #[test]
    fn test_scan_by_nameid_name() {
        // Start with the common battery of tests.
        let scan = ScanByNameOrId {
            sort_by: NameOrIdSortMode::NameDescending,
            selector: (),
        };
        assert_eq!(scan.direction(), PaginationOrder::Descending);

        let list = list_of_things();
        let thing0_marker = NameOrId::Name("thing0".parse().unwrap());
        let thinglast_name: Name = "thing19".parse().unwrap();
        let thinglast_marker = NameOrId::Name(thinglast_name.clone());
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=name_descending",
            &thing0_marker,
            &thinglast_marker,
            &ScanByNameOrId {
                sort_by: NameOrIdSortMode::NameAscending,
                selector: (),
            },
            &marker_for_name_or_id,
        );

        // Verify data pages based on the query params.
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        let data_page = match name_or_id_pagination(&data_page, &scan) {
            Ok(PaginatedBy::Name(params, ..)) => params,
            _ => {
                panic!("Expected Name pagination, got Id pagination")
            }
        };
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Descending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        let data_page = match name_or_id_pagination(&data_page, &scan) {
            Ok(PaginatedBy::Name(params, ..)) => params,
            _ => {
                panic!("Expected Name pagination, got Id pagination")
            }
        };
        assert_eq!(data_page.marker, Some(&thinglast_name));
        assert_eq!(data_page.direction, PaginationOrder::Descending);
        assert_eq!(data_page.limit, limit);
    }

    #[test]
    fn test_scan_by_nameid_id() {
        // Start with the common battery of tests.
        let scan = ScanByNameOrId {
            sort_by: NameOrIdSortMode::IdAscending,
            selector: (),
        };
        assert_eq!(scan.direction(), PaginationOrder::Ascending);

        let list = list_of_things();
        let thing0_marker = NameOrId::Id(list[0].identity.id);
        let thinglast_id = list[list.len() - 1].identity.id;
        let thinglast_marker = NameOrId::Id(list[list.len() - 1].identity.id);

        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=id_ascending",
            &thing0_marker,
            &thinglast_marker,
            &ScanByNameOrId {
                sort_by: NameOrIdSortMode::NameAscending,
                selector: (),
            },
            &marker_for_name_or_id,
        );

        // Verify data pages based on the query params.
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        let data_page = match name_or_id_pagination(&data_page, &scan) {
            Ok(PaginatedBy::Id(params, ..)) => params,
            _ => {
                panic!("Expected id pagination, got name pagination")
            }
        };
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        let data_page = match name_or_id_pagination(&data_page, &scan) {
            Ok(PaginatedBy::Id(params, ..)) => params,
            _ => {
                panic!("Expected id pagination, got name pagination")
            }
        };
        assert_eq!(data_page.marker, Some(&thinglast_id));
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);
    }

    #[test]
    fn test_scan_by_time_and_id() {
        let scan = ScanByTimeAndId {
            sort_by: TimeAndIdSortMode::TimeAndIdAscending,
            selector: (),
        };

        let list = list_of_things();
        let item0_time = list[0].identity.time_created;
        let item0_id = list[0].identity.id;
        let item0_marker = (item0_time, item0_id);

        let last_idx = list.len() - 1;
        let item_last_time = list[last_idx].identity.time_created;
        let item_last_id = list[last_idx].identity.id;
        let item_last_marker = (item_last_time, item_last_id);

        let marker_fn =
            |_: &ScanByTimeAndId, item: &MyThing| -> (DateTime<Utc>, Uuid) {
                (item.identity.time_created, item.identity.id)
            };
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=time_and_id_ascending",
            &item0_marker,
            &item_last_marker,
            &scan,
            &marker_fn,
        );

        assert_eq!(scan.direction(), PaginationOrder::Ascending);

        // Verify data pages based on the query params.
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker, Some(&item_last_marker));
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        // test descending too, why not (it caught a mistake!)
        let scan_desc = ScanByTimeAndId {
            sort_by: TimeAndIdSortMode::TimeAndIdDescending,
            selector: (),
        };
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan_desc,
            "sort_by=time_and_id_descending",
            &item0_marker,
            &item_last_marker,
            &scan,
            &marker_fn,
        );
        assert_eq!(scan_desc.direction(), PaginationOrder::Descending);

        // Verify data pages based on the query params.
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Descending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker, Some(&item_last_marker));
        assert_eq!(data_page.direction, PaginationOrder::Descending);
        assert_eq!(data_page.limit, limit);

        // Test error case
        let error = serde_urlencoded::from_str::<PaginatedByTimeAndId>(
            "sort_by=nothing",
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "unknown variant `nothing`, expected `time_and_id_ascending` or `time_and_id_descending`"
        );
    }
}
