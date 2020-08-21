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
use dropshot::ResultsPage;
use dropshot::WhichPage;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
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
#[derive(Deserialize, JsonSchema, Serialize)]
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
    Clone + DeserializeOwned + JsonSchema + Serialize
{
    /**
     * Type of the "marker" field for this scan mode
     *
     * For example, when scanning by name, this would be `ApiName`.
     */
    type MarkerValue: Clone + DeserializeOwned + Serialize;

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
    M: Clone + DeserializeOwned + Serialize,
{
    ApiPageSelector {
        scan: scan_params.clone(),
        last_seen: scan_params.marker_for_item(item).clone(),
    }
}

/**
 * Given a request and pagination parameters, return a [`DataPageParams`]
 * describing the current page of results to return
 *
 * This implementation is used for `ApiScanByName` and `ApiScanById`.  See
 * [`data_page_params_for_nameid_name`] and [`data_page_params_for_nameid_id`]
 * for variants that can be used for `ApiScanByNameOrId`.
 */
pub fn data_page_params_for<'a, S>(
    rqctx: &'a Arc<RequestContext>,
    pag_params: &'a PaginationParams<S, ApiPageSelector<S, S::MarkerValue>>,
) -> Result<DataPageParams<'a, S::MarkerValue>, HttpError>
where
    S: ScanParams,
{
    let limit = rqctx.page_limit(&pag_params)?;
    let marker = match &pag_params.page {
        WhichPage::First(..) => None,
        WhichPage::Next(ApiPageSelector {
            last_seen, ..
        }) => Some(last_seen),
    };
    let scan_params = S::from_query(pag_params)?;
    let direction = scan_params.direction();

    Ok(DataPageParams {
        marker,
        direction,
        limit,
    })
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
            WhichPage::Next(ApiPageSelector {
                ref scan, ..
            }) => scan,
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
        item.identity().id.clone()
    }
    fn from_query(p: &ApiPaginatedById) -> Result<&Self, HttpError> {
        Ok(match p.page {
            WhichPage::First(ref scan_params) => scan_params,
            WhichPage::Next(ApiPageSelector {
                ref scan, ..
            }) => scan,
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
pub fn data_page_params_nameid_name<'a>(
    rqctx: &'a Arc<RequestContext>,
    pag_params: &'a ApiPaginatedByNameOrId,
) -> Result<DataPageParams<'a, ApiName>, HttpError> {
    let data_page = data_page_params_for(rqctx, pag_params)?;
    let limit = data_page.limit;
    let direction = data_page.direction;
    let marker = match data_page.marker {
        None => None,
        Some(ApiNameOrIdMarker::Name(name)) => Some(name),
        Some(ApiNameOrIdMarker::Id(_)) => return Err(bad_token_error()),
    };
    Ok(DataPageParams {
        limit,
        direction,
        marker,
    })
}

/**
 * See [`data_page_params_nameid_name`].
 */
pub fn data_page_params_nameid_id<'a>(
    rqctx: &'a Arc<RequestContext>,
    pag_params: &'a ApiPaginatedByNameOrId,
) -> Result<DataPageParams<'a, Uuid>, HttpError> {
    let data_page = data_page_params_for(rqctx, pag_params)?;
    let limit = data_page.limit;
    let direction = data_page.direction;
    let marker = match data_page.marker {
        None => None,
        Some(ApiNameOrIdMarker::Id(id)) => Some(id),
        Some(ApiNameOrIdMarker::Name(_)) => return Err(bad_token_error()),
    };
    Ok(DataPageParams {
        limit,
        direction,
        marker,
    })
}

#[cfg(test)]
mod test {
    use super::page_selector_for;
    use super::ApiIdSortMode;
    use super::ApiName;
    use super::ApiNameOrIdMarker;
    use super::ApiNameOrIdSortMode;
    use super::ApiNameSortMode;
    use super::ApiPageSelectorById;
    use super::ApiPageSelectorByName;
    use super::ApiPageSelectorByNameOrId;
    use super::ApiScanById;
    use super::ApiScanByName;
    use super::ApiScanByNameOrId;
    use super::ScanParams;
    use crate::api_model::ApiIdentityMetadata;
    use crate::api_model::ApiObjectIdentity;
    use api_identity::ApiObjectIdentity;
    use chrono::Utc;
    use dropshot::PaginationOrder;
    use expectorate::assert_contents;
    use schemars::schema_for;
    use serde::Serialize;
    use serde_json::to_string_pretty;
    use std::convert::TryFrom;
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
        let scan_by_id = ApiScanById {
            sort_by: ApiIdSortMode::IdAscending,
        };
        let scan_by_name = ApiScanByName {
            sort_by: ApiNameSortMode::NameAscending,
        };
        let scan_by_nameid_name = ApiScanByNameOrId {
            sort_by: ApiNameOrIdSortMode::NameAscending,
        };
        let scan_by_nameid_id = ApiScanByNameOrId {
            sort_by: ApiNameOrIdSortMode::IdAscending,
        };
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

    #[derive(ApiObjectIdentity, Clone, Serialize)]
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

    #[test]
    fn test_scan_by_name() {
        let scan = ApiScanByName {
            sort_by: ApiNameSortMode::NameAscending,
        };

        let list = list_of_things();

        /*
         * TODO-coverage it'd be nice to test data_page_params_for(), but that
         * requires a RequestContext for getting the page limit.  At least we
         * can test some of the underlying pieces.
         */
        assert_eq!(scan.direction(), PaginationOrder::Ascending);
        assert_eq!(scan.marker_for_item(&list[0]), "thing0");
        assert_eq!(scan.marker_for_item(&list[6]), "thing6");

        /* Test page_selector_for(). */
        let page_selector = page_selector_for(&list[0], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(String::from(page_selector.last_seen).as_str(), "thing0");
        let page_selector = page_selector_for(&list[6], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(String::from(page_selector.last_seen).as_str(), "thing6");

        /*
         * TODO-coverage it'd be nice to test from_query() and results_page()
         * but we can't construct a PaginationParams outside of Dropshot.
         */
    }

    #[test]
    fn test_scan_by_id() {
        let scan = ApiScanById {
            sort_by: ApiIdSortMode::IdAscending,
        };

        let list = list_of_things();

        /* TODO-coverage See test_scan_by_name(). */
        assert_eq!(scan.direction(), PaginationOrder::Ascending);
        assert_eq!(scan.marker_for_item(&list[0]), list[0].identity.id);
        assert_eq!(scan.marker_for_item(&list[6]), list[6].identity.id);

        /* Test page_selector_for(). */
        let page_selector = page_selector_for(&list[0], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(page_selector.last_seen, list[0].identity.id);
        let page_selector = page_selector_for(&list[6], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(page_selector.last_seen, list[6].identity.id);

        /* TODO-coverage See test_scan_by_name(). */
    }

    #[test]
    fn test_scan_by_nameid_name() {
        let scan = ApiScanByNameOrId {
            sort_by: ApiNameOrIdSortMode::NameDescending,
        };

        let list = list_of_things();
        let thing0_marker = ApiNameOrIdMarker::Name(
            ApiName::try_from(String::from("thing0")).unwrap(),
        );
        let thing6_marker = ApiNameOrIdMarker::Name(
            ApiName::try_from(String::from("thing6")).unwrap(),
        );

        /* TODO-coverage See test_scan_by_name(). */
        assert_eq!(scan.direction(), PaginationOrder::Descending);
        assert_eq!(scan.marker_for_item(&list[0]), thing0_marker);
        assert_eq!(scan.marker_for_item(&list[6]), thing6_marker);

        /* Test page_selector_for(). */
        let page_selector = page_selector_for(&list[0], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(page_selector.last_seen, thing0_marker);
        let page_selector = page_selector_for(&list[6], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(page_selector.last_seen, thing6_marker);

        /* TODO-coverage See test_scan_by_name(). */
    }

    #[test]
    fn test_scan_by_nameid_id() {
        let scan = ApiScanByNameOrId {
            sort_by: ApiNameOrIdSortMode::IdAscending,
        };

        let list = list_of_things();
        let thing0_marker = ApiNameOrIdMarker::Id(list[0].identity.id);
        let thing6_marker = ApiNameOrIdMarker::Id(list[6].identity.id);

        /* TODO-coverage See test_scan_by_name(). */
        assert_eq!(scan.direction(), PaginationOrder::Ascending);
        assert_eq!(scan.marker_for_item(&list[0]), thing0_marker);
        assert_eq!(scan.marker_for_item(&list[6]), thing6_marker);

        /* Test page_selector_for(). */
        let page_selector = page_selector_for(&list[0], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(page_selector.last_seen, thing0_marker);
        let page_selector = page_selector_for(&list[6], &scan);
        assert_eq!(page_selector.scan, scan);
        assert_eq!(page_selector.last_seen, thing6_marker);

        /* TODO-coverage See test_scan_by_name(). */
    }
}
