/*!
 * Pagination support
 *
 * All list operations in the API are paginated, meaning that there's a limit on
 * the number of objects returned in a single request and clients are expected
 * to make additional requests to fetch the next page of results until the end
 * of the list is reached or the client has found what it needs.  For any list
 * operation, objects are sorted by a particular field that is unique among
 * objects in the list (usually a UTF-8 name or a UUID).  With each response,
 * the server will return a page of objects, plus a token that can be used to
 * fetch the next page.
 *
 * See Dropshot's pagination documentation for more background on this.
 *
 * For our API, we expect that most resources will support pagination in the
 * same way, which will include:
 *
 * * definitely: sorting in ascending order of the resource's "name"
 * * maybe in the future: sorting in descending order of the resource's "name"
 * * maybe in the future: sorting in ascending order of the resource's "id"
 * * maybe in the future: sorting in descending order of the resource's "id"
 * * maybe in the future: sorting in descending order of the resource's "mtime"
 *   and then "name" or "id"
 *
 * Dropshot's pagination support requires that we define the query parameters we
 * support with the first request ("scan params"), the information we need on
 * subsequent requests to resume a scan ("page selector"), and a way to generate
 * the page selector from a given object in the collection.  We can share these
 * definitions across as many resources as we want.  Below, we provide
 * definitions for resources that implement the `ApiObjectIdentity` trait.  With
 * these definitions, any type that has identity metadata can be paginated by
 * "name" in ascending order, "id" in ascending order, or either of those (plus
 * name in descending order) without any new boilerplate for that type.
 *
 * There may be resources that can't be paginated using one of the above three
 * ways, and we can define new ways to paginate them.  As you will notice below,
 * there's a fair bit of boilerplate for each way of paginating (rather than for
 * each resource paginated that way).  Where possible, we should share code.
 */

use crate::model::ApiName;
use crate::model::ApiObjectIdentity;
use crate::model::DataPageParams;
use crate::model::PaginationOrder;
use dropshot::HttpError;
use dropshot::PaginationParams;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::WhichPage;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::sync::Arc;
use uuid::Uuid;

/*
 * General pagination infrastructure
 */

/**
 * Specifies which page of results we're on
 *
 * This type is generic over the different scan modes that we support.
 */
#[derive(Debug, Deserialize, JsonSchema, Serialize)]
pub struct ApiPageSelector<ScanParams, MarkerType> {
    /** parameters describing the scan */
    #[serde(flatten)]
    scan: ScanParams,
    /** value of the marker field last seen by the client */
    last_seen: MarkerType,
}

/**
 * Describes one of our supported scan modes
 *
 * To minimize boilerplate, we provide common functions needed by our consumers
 * (e.g., `ScanParams::results_page`) as well as the Dropshot interface (e.g.,
 * `page_selector_for`).  This trait encapsulates the functionality that differs
 * among the different scan modes that we support.  Much of the functionality
 * here isn't so much a property of the Dropshot "scan parameters" as much as it
 * is specific to a scan using those parameters.  As a result, several of these
 * are associated functions rather than methods.
 */
pub trait ScanParams:
    Clone + Debug + DeserializeOwned + JsonSchema + PartialEq + Serialize
{
    /**
     * Type of the "marker" field for this scan mode
     *
     * For example, when scanning by name, this would be `ApiName`.
     */
    type MarkerValue: Clone + Debug + DeserializeOwned + PartialEq + Serialize;

    /**
     * Return the direction of the scan
     */
    fn direction(&self) -> PaginationOrder;

    /**
     * Given an item, return the appropriate marker value
     *
     * For example, when scanning by name, this returns the "name" field of the
     * item.
     */
    fn marker_for_item<T: ApiObjectIdentity>(&self, t: &T)
        -> Self::MarkerValue;

    /**
     * Given pagination parameters, return the current scan parameters
     *
     * This can fail if the pagination parameters are not self-consistent (e.g.,
     * if the scan parameters indicate we're going in ascending order by name,
     * but the marker is an id rather than a name).
     */
    fn from_query(
        q: &PaginationParams<Self, ApiPageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError>;

    /**
     * Generate a page of results for a paginated endpoint that lists items of
     * type `T`
     *
     * `list` contains the items that should appear on the page.  It's not
     * expected that consumers would override this implementation.
     */
    fn results_page<T>(
        query: &PaginationParams<
            Self,
            ApiPageSelector<Self, Self::MarkerValue>,
        >,
        list: Vec<T>,
    ) -> Result<ResultsPage<T>, dropshot::HttpError>
    where
        T: ApiObjectIdentity + Serialize,
    {
        let scan_params = Self::from_query(query)?;
        ResultsPage::new(list, scan_params, page_selector_for)
    }
}

/**
 * See `dropshot::ResultsPage::new`
 */
fn page_selector_for<T, S, M>(
    item: &T,
    scan_params: &S,
) -> ApiPageSelector<S, M>
where
    T: ApiObjectIdentity,
    S: ScanParams<MarkerValue = M>,
    M: Clone + Debug + DeserializeOwned + PartialEq + Serialize,
{
    ApiPageSelector {
        scan: scan_params.clone(),
        last_seen: scan_params.marker_for_item(item),
    }
}

/**
 * Given a request and pagination parameters, return a [`DataPageParams`]
 * describing the current page of results to return
 *
 * This implementation is used for `ApiScanByName` and `ApiScanById`.  See
 * [`data_page_params_nameid_name`] and [`data_page_params_nameid_id`] for
 * variants that can be used for `ApiScanByNameOrId`.
 */
pub fn data_page_params_for<'a, S, C>(
    rqctx: &'a Arc<RequestContext<C>>,
    pag_params: &'a PaginationParams<S, ApiPageSelector<S, S::MarkerValue>>,
) -> Result<DataPageParams<'a, S::MarkerValue>, HttpError>
where
    S: ScanParams,
    C: dropshot::ServerContext,
{
    let limit = rqctx.page_limit(&pag_params)?;
    data_page_params_with_limit(limit, &pag_params)
}

/**
 * Provided separately from data_page_params_for() so that the test suite can
 * test the bulk of the logic without needing to cons up a Dropshot
 * `RequestContext` just to get the limit.
 */
fn data_page_params_with_limit<S>(
    limit: NonZeroU32,
    pag_params: &PaginationParams<S, ApiPageSelector<S, S::MarkerValue>>,
) -> Result<DataPageParams<S::MarkerValue>, HttpError>
where
    S: ScanParams,
{
    let marker = match &pag_params.page {
        WhichPage::First(..) => None,
        WhichPage::Next(ApiPageSelector { last_seen, .. }) => Some(last_seen),
    };
    let scan_params = S::from_query(pag_params)?;
    let direction = scan_params.direction();

    Ok(DataPageParams { marker, direction, limit })
}

/*
 * Pagination by name in ascending order only (most resources today)
 */

/** Query parameters for pagination by name only */
pub type ApiPaginatedByName =
    PaginationParams<ApiScanByName, ApiPageSelectorByName>;
/** Page selector for pagination by name only */
pub type ApiPageSelectorByName = ApiPageSelector<ApiScanByName, ApiName>;
/** Scan parameters for resources that support scanning by name only */
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ApiScanByName {
    #[serde(default = "default_name_sort_mode")]
    sort_by: ApiNameSortMode,
}
/**
 * Supported set of sort modes for scanning by name only
 *
 * Currently, we only support scanning in ascending order.
 */
#[derive(Copy, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiNameSortMode {
    /** sort in increasing order of "name" */
    NameAscending,
}

fn default_name_sort_mode() -> ApiNameSortMode {
    ApiNameSortMode::NameAscending
}

impl ScanParams for ApiScanByName {
    type MarkerValue = ApiName;
    fn direction(&self) -> PaginationOrder {
        PaginationOrder::Ascending
    }
    fn marker_for_item<T: ApiObjectIdentity>(&self, item: &T) -> ApiName {
        item.identity().name.clone()
    }
    fn from_query(
        p: &PaginationParams<Self, ApiPageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(ApiPageSelector { ref scan, .. }) => scan,
        })
    }
}

/*
 * Pagination by id in ascending order only (for some anonymous resources today)
 */

/** Query parameters for pagination by id only */
pub type ApiPaginatedById = PaginationParams<ApiScanById, ApiPageSelectorById>;
/** Page selector for pagination by name only */
pub type ApiPageSelectorById = ApiPageSelector<ApiScanById, Uuid>;
/** Scan parameters for resources that support scanning by id only */
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ApiScanById {
    #[serde(default = "default_id_sort_mode")]
    sort_by: ApiIdSortMode,
}

/**
 * Supported set of sort modes for scanning by id only.
 *
 * Currently, we only support scanning in ascending order.
 */
#[derive(Copy, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiIdSortMode {
    /** sort in increasing order of "id" */
    IdAscending,
}

fn default_id_sort_mode() -> ApiIdSortMode {
    ApiIdSortMode::IdAscending
}

impl ScanParams for ApiScanById {
    type MarkerValue = Uuid;
    fn direction(&self) -> PaginationOrder {
        PaginationOrder::Ascending
    }
    fn marker_for_item<T: ApiObjectIdentity>(&self, item: &T) -> Uuid {
        item.identity().id
    }
    fn from_query(p: &ApiPaginatedById) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(ApiPageSelector { ref scan, .. }) => scan,
        })
    }
}

/*
 * Pagination by any of: name ascending, name descending, or id ascending.
 * We include this now primarily to exercise the interface for doing so.
 */

/** Query parameters for pagination by name or id */
pub type ApiPaginatedByNameOrId =
    PaginationParams<ApiScanByNameOrId, ApiPageSelectorByNameOrId>;
/** Page selector for pagination by name or id */
pub type ApiPageSelectorByNameOrId =
    ApiPageSelector<ApiScanByNameOrId, ApiNameOrIdMarker>;
/** Scan parameters for resources that support scanning by name or id */
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ApiScanByNameOrId {
    #[serde(default = "default_nameid_sort_mode")]
    sort_by: ApiNameOrIdSortMode,
}
/** Supported set of sort modes for scanning by name or id */
#[derive(Copy, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiNameOrIdSortMode {
    /** sort in increasing order of "name" */
    NameAscending,
    /** sort in decreasing order of "name" */
    NameDescending,
    /** sort in increasing order of "id" */
    IdAscending,
}

fn default_nameid_sort_mode() -> ApiNameOrIdSortMode {
    ApiNameOrIdSortMode::NameAscending
}

/*
 * TODO-correctness It's tempting to make this a serde(untagged) enum, which
 * would clean up the format of the page selector parameter.  However, it would
 * have the side effect that if the name happened to be a valid uuid, then we'd
 * parse it as a uuid here, even if the corresponding scan parameters indicated
 * that we were doing a scan by name.  Then we'd fail later on an invalid
 * combination.  We could infer the correct variant here from the "sort_by"
 * field of the adjacent scan params, but we'd have to write our own
 * `Deserialize` to do this.  This might be worth revisiting before we commit to
 * any particular version of the API.
 */
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiNameOrIdMarker {
    Id(Uuid),
    Name(ApiName),
}

fn bad_token_error() -> HttpError {
    HttpError::for_bad_request(None, String::from("invalid page token"))
}

#[derive(Debug, PartialEq)]
pub enum ApiPagField {
    Id,
    Name,
}

pub fn pagination_field_for_scan_params(p: &ApiScanByNameOrId) -> ApiPagField {
    match p.sort_by {
        ApiNameOrIdSortMode::NameAscending => ApiPagField::Name,
        ApiNameOrIdSortMode::NameDescending => ApiPagField::Name,
        ApiNameOrIdSortMode::IdAscending => ApiPagField::Id,
    }
}

impl ScanParams for ApiScanByNameOrId {
    type MarkerValue = ApiNameOrIdMarker;

    fn direction(&self) -> PaginationOrder {
        match self.sort_by {
            ApiNameOrIdSortMode::NameAscending => PaginationOrder::Ascending,
            ApiNameOrIdSortMode::NameDescending => PaginationOrder::Descending,
            ApiNameOrIdSortMode::IdAscending => PaginationOrder::Ascending,
        }
    }

    fn marker_for_item<T: ApiObjectIdentity>(
        &self,
        item: &T,
    ) -> ApiNameOrIdMarker {
        let identity = item.identity();
        match pagination_field_for_scan_params(self) {
            ApiPagField::Name => ApiNameOrIdMarker::Name(identity.name.clone()),
            ApiPagField::Id => ApiNameOrIdMarker::Id(identity.id),
        }
    }

    fn from_query(
        p: &PaginationParams<Self, ApiPageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError> {
        match &p.page {
            WhichPage::First(scan_mode) => Ok(scan_mode),

            WhichPage::Next(ApiPageSelectorByNameOrId {
                scan,
                last_seen: ApiNameOrIdMarker::Name(_),
            }) => match scan.sort_by {
                ApiNameOrIdSortMode::NameAscending => Ok(scan),
                ApiNameOrIdSortMode::NameDescending => Ok(scan),
                ApiNameOrIdSortMode::IdAscending => Err(()),
            },

            WhichPage::Next(ApiPageSelectorByNameOrId {
                scan,
                last_seen: ApiNameOrIdMarker::Id(_),
            }) => match scan.sort_by {
                ApiNameOrIdSortMode::NameAscending => Err(()),
                ApiNameOrIdSortMode::NameDescending => Err(()),
                ApiNameOrIdSortMode::IdAscending => Ok(scan),
            },
        }
        .map_err(|_| bad_token_error())
    }
}

/**
 * Serves the same purpose as [`data_page_params_for`] for the specific case of
 * `ApiScanByNameOrId` when scanning by `name`
 *
 * Why do we need a separate function here?  Because `data_page_params_for` only
 * knows how to return the (statically-defined) marker value from the page
 * selector.  For `ApiScanByNameOrId`, this would return the enum
 * `ApiNameOrIdMarker`.  But at some point our caller needs the specific type
 * (e.g., `ApiName` for a scan by name or `Uuid` for a scan by Uuid).  They get
 * that from this function and its partner, [`data_page_params_nameid_id`].
 * These functions are where we look at the enum variant and extract the
 * specific marker value out.
 */
pub fn data_page_params_nameid_name<'a, C>(
    rqctx: &'a Arc<RequestContext<C>>,
    pag_params: &'a ApiPaginatedByNameOrId,
) -> Result<DataPageParams<'a, ApiName>, HttpError>
where
    C: dropshot::ServerContext,
{
    let limit = rqctx.page_limit(&pag_params)?;
    data_page_params_nameid_name_limit(limit, pag_params)
}

fn data_page_params_nameid_name_limit(
    limit: NonZeroU32,
    pag_params: &ApiPaginatedByNameOrId,
) -> Result<DataPageParams<ApiName>, HttpError> {
    let data_page = data_page_params_with_limit(limit, pag_params)?;
    let direction = data_page.direction;
    let marker = match data_page.marker {
        None => None,
        Some(ApiNameOrIdMarker::Name(name)) => Some(name),
        /*
         * This should arguably be a panic or a 500 error, since the caller
         * should not have invoked this version of the function if they didn't
         * know they were looking at a name-based marker.
         */
        Some(ApiNameOrIdMarker::Id(_)) => return Err(bad_token_error()),
    };
    Ok(DataPageParams { limit, direction, marker })
}

/**
 * See [`data_page_params_nameid_name`].
 */
pub fn data_page_params_nameid_id<'a, C>(
    rqctx: &'a Arc<RequestContext<C>>,
    pag_params: &'a ApiPaginatedByNameOrId,
) -> Result<DataPageParams<'a, Uuid>, HttpError>
where
    C: dropshot::ServerContext,
{
    let limit = rqctx.page_limit(&pag_params)?;
    data_page_params_nameid_id_limit(limit, pag_params)
}

fn data_page_params_nameid_id_limit(
    limit: NonZeroU32,
    pag_params: &ApiPaginatedByNameOrId,
) -> Result<DataPageParams<Uuid>, HttpError> {
    let data_page = data_page_params_with_limit(limit, pag_params)?;
    let direction = data_page.direction;
    let marker = match data_page.marker {
        None => None,
        Some(ApiNameOrIdMarker::Id(id)) => Some(id),
        /*
         * This should arguably be a panic or a 500 error, since the caller
         * should not have invoked this version of the function if they didn't
         * know they were looking at an id-based marker.
         */
        Some(ApiNameOrIdMarker::Name(_)) => return Err(bad_token_error()),
    };
    Ok(DataPageParams { limit, direction, marker })
}

#[cfg(test)]
mod test {
    use super::data_page_params_nameid_id_limit;
    use super::data_page_params_nameid_name_limit;
    use super::data_page_params_with_limit;
    use super::page_selector_for;
    use super::pagination_field_for_scan_params;
    use super::ApiIdSortMode;
    use super::ApiName;
    use super::ApiNameOrIdMarker;
    use super::ApiNameOrIdSortMode;
    use super::ApiNameSortMode;
    use super::ApiPagField;
    use super::ApiPageSelector;
    use super::ApiPageSelectorById;
    use super::ApiPageSelectorByName;
    use super::ApiPageSelectorByNameOrId;
    use super::ApiPaginatedById;
    use super::ApiPaginatedByName;
    use super::ApiPaginatedByNameOrId;
    use super::ApiScanById;
    use super::ApiScanByName;
    use super::ApiScanByNameOrId;
    use super::ScanParams;
    use crate::api_model::ApiIdentityMetadata;
    use crate::api_model::ApiObjectIdentity;
    use api_identity::ApiObjectIdentity;
    use chrono::Utc;
    use dropshot::PaginationOrder;
    use dropshot::PaginationParams;
    use dropshot::WhichPage;
    use expectorate::assert_contents;
    use http::StatusCode;
    use schemars::schema_for;
    use serde::Serialize;
    use serde_json::to_string_pretty;
    use std::convert::TryFrom;
    use std::num::NonZeroU32;
    use uuid::Uuid;

    /*
     * It's important to verify the schema for the page selectors because this
     * is a part of our interface that does not appear in the OpenAPI spec
     * because it's obscured by Dropshot's automatic encoding of the page
     * selector.
     *
     * Below, we also check the schema for the scan parameters because it's easy
     * to do and useful to have the examples there.  We may want to remove this
     * if/when we add a test case that checks the entire OpenAPI schema for our
     * various APIs, since this will then be redundant.
     */
    #[test]
    fn test_pagination_schemas() {
        let schemas = vec![
            ("scan parameters, scan by name only", schema_for!(ApiScanByName)),
            ("scan parameters, scan by id only", schema_for!(ApiScanById)),
            (
                "scan parameters, scan by name or id",
                schema_for!(ApiScanByNameOrId),
            ),
            (
                "page selector, scan by name only",
                schema_for!(ApiPageSelectorByName),
            ),
            (
                "page selector, scan by id only",
                schema_for!(ApiPageSelectorById),
            ),
            (
                "page selector, scan by name or id",
                schema_for!(ApiPageSelectorByNameOrId),
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

    /*
     * As much for illustration as anything, we check examples of the scan
     * parameters and page selectors here.
     */
    #[test]
    fn test_pagination_examples() {
        let scan_by_id = ApiScanById { sort_by: ApiIdSortMode::IdAscending };
        let scan_by_name =
            ApiScanByName { sort_by: ApiNameSortMode::NameAscending };
        let scan_by_nameid_name =
            ApiScanByNameOrId { sort_by: ApiNameOrIdSortMode::NameAscending };
        let scan_by_nameid_id =
            ApiScanByNameOrId { sort_by: ApiNameOrIdSortMode::IdAscending };
        let id: Uuid = "61a78113-d3c6-4b35-a410-23e9eae64328".parse().unwrap();
        let name = ApiName::try_from(String::from("bort")).unwrap();
        let examples = vec![
            /* scan parameters only */
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
            /* page selectors */
            (
                "page selector: by id ascending",
                to_string_pretty(&ApiPageSelectorById {
                    scan: scan_by_id,
                    last_seen: id,
                })
                .unwrap(),
            ),
            (
                "page selector: by name ascending",
                to_string_pretty(&ApiPageSelectorByName {
                    scan: scan_by_name,
                    last_seen: name.clone(),
                })
                .unwrap(),
            ),
            (
                "page selector: by name or id, using id ascending",
                to_string_pretty(&ApiPageSelectorByNameOrId {
                    scan: scan_by_nameid_id,
                    last_seen: ApiNameOrIdMarker::Id(id),
                })
                .unwrap(),
            ),
            (
                "page selector: by name or id, using id ascending",
                to_string_pretty(&ApiPageSelectorByNameOrId {
                    scan: scan_by_nameid_name,
                    last_seen: ApiNameOrIdMarker::Name(name.clone()),
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

    #[derive(ApiObjectIdentity, Clone, Debug, PartialEq, Serialize)]
    struct MyThing {
        identity: ApiIdentityMetadata,
    }

    fn list_of_things() -> Vec<MyThing> {
        (0..20)
            .map(|i| {
                let name = ApiName::try_from(format!("thing{}", i)).unwrap();
                let now = Utc::now();
                MyThing {
                    identity: ApiIdentityMetadata {
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

    /**
     * Function for running a bunch of tests on a ScanParams type.
     */
    fn test_scan_param_common<S>(
        list: &Vec<MyThing>,
        scan: &S,
        querystring: &str,
        item0_marker: &S::MarkerValue,
        itemlast_marker: &S::MarkerValue,
        scan_default: &S,
    ) -> (
        PaginationParams<S, ApiPageSelector<S, S::MarkerValue>>,
        PaginationParams<S, ApiPageSelector<S, S::MarkerValue>>,
    )
    where
        S: ScanParams,
    {
        let li = list.len() - 1;

        /* Test basic parts of ScanParams interface. */
        assert_eq!(&scan.marker_for_item(&list[0]), item0_marker);
        assert_eq!(&scan.marker_for_item(&list[li]), itemlast_marker);

        /* Test page_selector_for(). */
        let page_selector = page_selector_for(&list[0], scan);
        assert_eq!(&page_selector.scan, scan);
        assert_eq!(&page_selector.last_seen, item0_marker);

        let page_selector = page_selector_for(&list[li], scan);
        assert_eq!(&page_selector.scan, scan);
        assert_eq!(&page_selector.last_seen, itemlast_marker);

        /* Test from_query() with the default scan parameters. */
        let p: PaginationParams<S, ApiPageSelector<S, S::MarkerValue>> =
            serde_urlencoded::from_str("").unwrap();
        assert_eq!(S::from_query(&p).unwrap(), scan_default);

        /*
         * Test from_query() based on an explicit querystring corresponding to
         * the first page in a scan with "scan" as the scan parameters.
         */
        let p0: PaginationParams<S, ApiPageSelector<S, S::MarkerValue>> =
            serde_urlencoded::from_str(querystring).unwrap();
        assert_eq!(S::from_query(&p0).unwrap(), scan);

        /*
         * Generate a results page from that, verify it, pull the token out, and
         * use it to generate pagination parameters for a NextPage request.
         */
        let page = S::results_page(&p0, list.clone()).unwrap();
        assert_eq!(&page.items, list);
        assert!(page.next_page.is_some());
        let q = format!("page_token={}", page.next_page.unwrap());
        let p1: PaginationParams<S, ApiPageSelector<S, S::MarkerValue>> =
            serde_urlencoded::from_str(&q).unwrap();

        /*
         * Now pull the information out of that, including the "last_seen"
         * marker.  This should match `itemlast_marker`.  That will tell us that
         * the results page was properly generated.
         */
        assert_eq!(S::from_query(&p1).unwrap(), scan);
        if let WhichPage::Next(ApiPageSelector { ref last_seen, .. }) = p1.page
        {
            assert_eq!(last_seen, itemlast_marker);
        } else {
            panic!("expected WhichPage::Next");
        }

        /*
         * Return these two sets of pagination parameters to the caller for more
         * testing.
         */
        (p0, p1)
    }

    #[test]
    fn test_scan_by_name() {
        /* Start with the common battery of tests. */
        let scan = ApiScanByName { sort_by: ApiNameSortMode::NameAscending };

        let list = list_of_things();
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=name-ascending",
            &ApiName::try_from(String::from("thing0")).unwrap(),
            &ApiName::try_from(String::from("thing19")).unwrap(),
            &scan,
        );
        assert_eq!(scan.direction(), PaginationOrder::Ascending);

        /* Verify data pages based on the query params. */
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker.unwrap().as_str(), "thing19");
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        /* Test from_query(): error case. */
        let error = serde_urlencoded::from_str::<ApiPaginatedByName>(
            "sort_by=name-descending",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unknown variant `name-descending`, expected `name-ascending`"
        );
    }

    #[test]
    fn test_scan_by_id() {
        /* Start with the common battery of tests. */
        let scan = ApiScanById { sort_by: ApiIdSortMode::IdAscending };

        let list = list_of_things();
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=id-ascending",
            &list[0].identity.id,
            &list[list.len() - 1].identity.id,
            &scan,
        );
        assert_eq!(scan.direction(), PaginationOrder::Ascending);

        /* Verify data pages based on the query params. */
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_with_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_with_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker, Some(&list[19].identity.id));
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        /* Test from_query(): error case. */
        let error = serde_urlencoded::from_str::<ApiPaginatedById>(
            "sort_by=id-descending",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unknown variant `id-descending`, expected `id-ascending`"
        );
    }

    #[test]
    fn test_scan_by_nameid_generic() {
        /* Test from_query(): error case. */
        let error = serde_urlencoded::from_str::<ApiPaginatedByNameOrId>(
            "sort_by=id-descending",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unknown variant `id-descending`, expected one of \
             `name-ascending`, `name-descending`, `id-ascending`"
        );

        /*
         * TODO-coverage It'd be nice to exercise the from_query() error cases
         * where the scan params doesn't match the last_seen value kind.
         * However, we can't easily generate these, either directly or by
         * causing Dropshot to parse a querystring.  In the latter case, it
         * would have to be a page token that Dropshot generated, but by
         * design we can't get Dropshot to construct such a token.
         */
    }

    #[test]
    fn test_scan_by_nameid_name() {
        /* Start with the common battery of tests. */
        let scan =
            ApiScanByNameOrId { sort_by: ApiNameOrIdSortMode::NameDescending };
        assert_eq!(pagination_field_for_scan_params(&scan), ApiPagField::Name);
        assert_eq!(scan.direction(), PaginationOrder::Descending);

        let list = list_of_things();
        let thing0_marker = ApiNameOrIdMarker::Name(
            ApiName::try_from(String::from("thing0")).unwrap(),
        );
        let thinglast_name =
            ApiName::try_from(String::from("thing19")).unwrap();
        let thinglast_marker = ApiNameOrIdMarker::Name(thinglast_name.clone());
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=name-descending",
            &thing0_marker,
            &thinglast_marker,
            &ApiScanByNameOrId { sort_by: ApiNameOrIdSortMode::NameAscending },
        );

        /* Verify data pages based on the query params. */
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_nameid_name_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Descending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_nameid_name_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker, Some(&thinglast_name));
        assert_eq!(data_page.direction, PaginationOrder::Descending);
        assert_eq!(data_page.limit, limit);

        let error = data_page_params_nameid_id_limit(limit, &p1).unwrap_err();
        assert_eq!(error.status_code, StatusCode::BAD_REQUEST);
        assert_eq!(error.external_message, "invalid page token");
    }

    #[test]
    fn test_scan_by_nameid_id() {
        /* Start with the common battery of tests. */
        let scan =
            ApiScanByNameOrId { sort_by: ApiNameOrIdSortMode::IdAscending };
        assert_eq!(pagination_field_for_scan_params(&scan), ApiPagField::Id);
        assert_eq!(scan.direction(), PaginationOrder::Ascending);

        let list = list_of_things();
        let thing0_marker = ApiNameOrIdMarker::Id(list[0].identity.id);
        let thinglast_id = list[list.len() - 1].identity.id;
        let thinglast_marker =
            ApiNameOrIdMarker::Id(list[list.len() - 1].identity.id);
        let (p0, p1) = test_scan_param_common(
            &list,
            &scan,
            "sort_by=id-ascending",
            &thing0_marker,
            &thinglast_marker,
            &ApiScanByNameOrId { sort_by: ApiNameOrIdSortMode::NameAscending },
        );

        /* Verify data pages based on the query params. */
        let limit = NonZeroU32::new(123).unwrap();
        let data_page = data_page_params_nameid_id_limit(limit, &p0).unwrap();
        assert_eq!(data_page.marker, None);
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let data_page = data_page_params_nameid_id_limit(limit, &p1).unwrap();
        assert_eq!(data_page.marker, Some(&thinglast_id));
        assert_eq!(data_page.direction, PaginationOrder::Ascending);
        assert_eq!(data_page.limit, limit);

        let error = data_page_params_nameid_name_limit(limit, &p1).unwrap_err();
        assert_eq!(error.status_code, StatusCode::BAD_REQUEST);
        assert_eq!(error.external_message, "invalid page token");
    }
}
