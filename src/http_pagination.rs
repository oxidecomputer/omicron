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

use crate::api_model::ApiName;
use crate::api_model::ApiObjectIdentity;
use crate::api_model::DataPageParams;
use crate::api_model::PaginationOrder;
use dropshot::HttpError;
use dropshot::PaginationParams;
use dropshot::RequestContext;
use dropshot::WhichPage;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

/*
 * For our own purposes, we define a ScanParams trait that we can use to
 * implement some common functions.
 */
pub trait ScanParams:
    Clone + DeserializeOwned + JsonSchema + Serialize
{
    type MarkerValue: Clone + DeserializeOwned + Serialize;
    fn marker_for<T: ApiObjectIdentity>(&self, t: &T) -> Self::MarkerValue;
    fn from_query(
        q: &PaginationParams<Self, ApiPageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError>;
}

fn page_selector_for<T, S, M>(
    item: &T,
    scan_params: &S,
) -> ApiPageSelector<S, M>
where
    T: ApiObjectIdentity,
    S: ScanParams<MarkerValue = M>,
    M: Clone + DeserializeOwned + Serialize,
{
    ApiPageSelector {
        scan: scan_params.clone(),
        last_seen: scan_params.marker_for(item).clone(),
    }
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
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
pub struct ApiScanByName {
    #[serde(default = "default_name_sort_mode")]
    sort_by: ApiNameSortMode,
}

impl ScanParams for ApiScanByName {
    type MarkerValue = ApiName;
    fn marker_for<T: ApiObjectIdentity>(&self, item: &T) -> ApiName {
        item.identity().name.clone()
    }
    fn from_query(
        p: &PaginationParams<Self, ApiPageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(ApiPageSelector {
                ref scan, ..
            }) => scan,
        })
    }
}

/**
 * Supported set of sort modes for scanning by name only
 *
 * Currently, we only support scanning in ascending order.
 */
#[derive(Copy, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiNameSortMode {
    /** sort in increasing order of "name" */
    NameAscending,
}

fn default_name_sort_mode() -> ApiNameSortMode {
    ApiNameSortMode::NameAscending
}

/**
 * Given the request context `rqctx` and Dropshot pagination parameters
 * `pag_params` for a name-based scan, construct a `DataPageParams` describing
 * the page of data that we want.
 */
pub fn data_page_params_name<'a>(
    rqctx: &'a Arc<RequestContext>,
    pag_params: &'a ApiPaginatedByName,
) -> Result<DataPageParams<'a, ApiName>, HttpError> {
    let limit = rqctx.page_limit(&pag_params)?;
    let marker = match &pag_params.page {
        WhichPage::First(..) => None,
        WhichPage::Next(ApiPageSelector {
            scan:
                ApiScanByName {
                    sort_by: ApiNameSortMode::NameAscending,
                },
            last_seen,
        }) => Some(last_seen),
    };

    Ok(DataPageParams {
        marker,
        direction: PaginationOrder::Ascending,
        limit,
    })
}

/*
 * Pagination by id in ascending order only (for some anonymous resources today)
 */

/** Query parameters for pagination by id only */
pub type ApiPaginatedById = PaginationParams<ApiScanById, ApiPageSelectorById>;

/** Page selector for pagination by name only */
pub type ApiPageSelectorById = ApiPageSelector<ApiScanById, Uuid>;

/** Scan parameters for resources that support scanning by id only */
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
pub struct ApiScanById {
    #[serde(default = "default_id_sort_mode")]
    sort_by: ApiIdSortMode,
}

impl ScanParams for ApiScanById {
    type MarkerValue = Uuid;
    fn marker_for<T: ApiObjectIdentity>(&self, item: &T) -> Uuid {
        item.identity().id.clone()
    }
    fn from_query(
        p: &PaginationParams<Self, ApiPageSelector<Self, Self::MarkerValue>>,
    ) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(ApiPageSelector {
                ref scan, ..
            }) => scan,
        })
    }
}

/**
 * Supported set of sort modes for scanning by id only.
 *
 * Currently, we only support scanning in ascending order.
 */
#[derive(Copy, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiIdSortMode {
    /** sort in increasing order of "id" */
    IdAscending,
}

fn default_id_sort_mode() -> ApiIdSortMode {
    ApiIdSortMode::IdAscending
}

/**
 * Given the request context `rqctx` and Dropshot pagination parameters
 * `pag_params` for an id-based scan, construct a `DataPageParams` describing
 * the page of data that we want.
 */
pub fn data_page_params_id<'a>(
    rqctx: &'a Arc<RequestContext>,
    pag_params: &'a ApiPaginatedById,
) -> Result<DataPageParams<'a, Uuid>, HttpError> {
    let limit = rqctx.page_limit(&pag_params)?;
    let marker = match &pag_params.page {
        WhichPage::First(..) => None,
        WhichPage::Next(ApiPageSelectorById {
            scan:
                ApiScanById {
                    sort_by: ApiIdSortMode::IdAscending,
                },
            last_seen,
        }) => Some(last_seen),
    };

    Ok(DataPageParams {
        marker,
        direction: PaginationOrder::Ascending,
        limit,
    })
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
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
pub struct ApiScanByNameOrId {
    #[serde(default = "default_nameid_sort_mode")]
    sort_by: ApiNameOrIdSortMode,
}

impl ScanParams for ApiScanByNameOrId {
    type MarkerValue = ApiNameOrIdMarker;
    fn marker_for<T: ApiObjectIdentity>(&self, item: &T) -> ApiNameOrIdMarker {
        let identity = item.identity();
        match pagination_field_for_scan_params(self) {
            ApiPagField::Name => ApiNameOrIdMarker::Name(identity.name.clone()),
            ApiPagField::Id => ApiNameOrIdMarker::Id(identity.id.clone()),
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

/** Supported set of sort modes for scanning by name or id */
#[derive(Copy, Clone, Deserialize, JsonSchema, Serialize)]
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

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiNameOrIdMarker {
    Id(Uuid),
    Name(ApiName),
}

pub enum ApiPagField {
    Id,
    Name,
}

fn bad_token_error() -> HttpError {
    HttpError::for_bad_request(None, String::from("invalid page token"))
}

pub fn pagination_field_for_scan_params(p: &ApiScanByNameOrId) -> ApiPagField {
    match p.sort_by {
        ApiNameOrIdSortMode::NameAscending => ApiPagField::Name,
        ApiNameOrIdSortMode::NameDescending => ApiPagField::Name,
        ApiNameOrIdSortMode::IdAscending => ApiPagField::Id,
    }
}

/**
 * Given the request context `rqctx` and Dropshot pagination parameters
 * `pag_params` for a name-based scan, construct a `DataPageParams` describing
 * the page of data that we want.
 */
pub fn data_page_params_nameid_name<'a>(
    rqctx: &'a Arc<RequestContext>,
    pag_params: &'a ApiPaginatedByNameOrId,
) -> Result<DataPageParams<'a, ApiName>, HttpError> {
    let limit = rqctx.page_limit(&pag_params)?;
    let scan_params = ApiScanByNameOrId::from_query(pag_params)?;

    let direction = match &scan_params.sort_by {
        ApiNameOrIdSortMode::NameAscending => PaginationOrder::Ascending,
        ApiNameOrIdSortMode::NameDescending => PaginationOrder::Descending,
        _ => return Err(bad_token_error()),
    };

    let marker = match &pag_params.page {
        WhichPage::First(..) => None,
        WhichPage::Next(ApiPageSelectorByNameOrId {
            last_seen: ApiNameOrIdMarker::Name(name),
            ..
        }) => Some(name),
        WhichPage::Next(ApiPageSelectorByNameOrId {
            last_seen: ApiNameOrIdMarker::Id(_),
            ..
        }) => return Err(bad_token_error()),
    };

    Ok(DataPageParams {
        marker,
        direction,
        limit,
    })
}

/**
 * Given the request context `rqctx` and Dropshot pagination parameters
 * `pag_params` for a name-based scan, construct a `DataPageParams` describing
 * the page of data that we want.
 */
pub fn data_page_params_nameid_id<'a>(
    rqctx: &'a Arc<RequestContext>,
    pag_params: &'a ApiPaginatedByNameOrId,
) -> Result<DataPageParams<'a, Uuid>, HttpError> {
    let limit = rqctx.page_limit(&pag_params)?;
    let marker = match &pag_params.page {
        WhichPage::First(..) => None,
        WhichPage::Next(ApiPageSelectorByNameOrId {
            last_seen: ApiNameOrIdMarker::Id(id),
            ..
        }) => Some(id),
        WhichPage::Next(ApiPageSelectorByNameOrId {
            last_seen: ApiNameOrIdMarker::Name(_),
            ..
        }) => return Err(bad_token_error()),
    };

    Ok(DataPageParams {
        marker,
        direction: PaginationOrder::Ascending,
        limit,
    })
}

/*
 * General pagination infrastructure
 */

/**
 * Specifies which page of results we're on
 */
#[derive(Deserialize, JsonSchema, Serialize)]
pub struct ApiPageSelector<ScanParams, MarkerType> {
    /** parameters describing the scan */
    #[serde(flatten)]
    scan: ScanParams,
    /** value of the marker field last seen by the client */
    last_seen: MarkerType,
}

/**
 * A page of results in the API
 */
/*
 * This wraps the corresponding Dropshot type to provide slightly more
 * convenient constructors for our common case.
 */
#[derive(Serialize, JsonSchema)]
pub struct ApiResultsPage<T> {
    #[serde(flatten)]
    results: dropshot::ResultsPage<T>,
}

impl<T: ApiObjectIdentity + Serialize> ApiResultsPage<T> {
    /**
     * Generate a page of results for a paginated endpoint that lists items of
     * type `T`
     *
     * `list` contains the items that should appear on the page.
     */
    pub fn new<S, P>(
        query: &PaginationParams<S, ApiPageSelector<S, P>>,
        list: Vec<T>,
    ) -> Result<Self, dropshot::HttpError>
    where
        S: ScanParams<MarkerValue = P>,
        P: Clone + DeserializeOwned + Serialize,
    {
        let scan_params = S::from_query(query)?;
        Ok(ApiResultsPage {
            results: dropshot::ResultsPage::new(
                list,
                scan_params,
                page_selector_for,
            )?,
        })
    }
}
