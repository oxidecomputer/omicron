/*!
 * Functions for iterating pages from a collection
 */

use crate::api::DataPageParams;
use crate::api::Error;
use crate::api::ListResult;
use crate::api::PaginationOrder::Ascending;
use crate::api::PaginationOrder::Descending;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::ops::Bound;
use std::sync::Arc;

/**
 * List a page of items from a collection `search_tree` that maps lookup keys
 * directly to the actual objects
 */
pub fn collection_page<KeyType, ValueType>(
    search_tree: &BTreeMap<KeyType, Arc<ValueType>>,
    pagparams: &DataPageParams<'_, KeyType>,
) -> ListResult<Arc<ValueType>>
where
    KeyType: std::cmp::Ord,
    ValueType: Send + Sync + 'static,
{
    /*
     * We assemble the list of results that we're going to return now.  If the
     * caller is holding a lock, they'll be able to release it right away.  This
     * also makes the lifetime of the return value much easier.
     */
    let list = collection_page_as_iter(search_tree, pagparams)
        .map(|(_, v)| Ok(Arc::clone(v)))
        .collect::<Vec<Result<Arc<ValueType>, Error>>>();
    Ok(futures::stream::iter(list).boxed())
}

/**
 * Returns a page of items from a collection `search_tree` as an iterator
 */
pub fn collection_page_as_iter<'a, 'b, KeyType, ValueType>(
    search_tree: &'a BTreeMap<KeyType, ValueType>,
    pagparams: &'b DataPageParams<'_, KeyType>,
) -> Box<dyn Iterator<Item = (&'a KeyType, &'a ValueType)> + 'a>
where
    KeyType: std::cmp::Ord,
{
    /*
     * Convert the 32-bit limit to a "usize".  This can in principle fail, but
     * not in any context in which we ever expect this code to run.
     */
    let limit = usize::try_from(pagparams.limit.get()).unwrap();
    match (pagparams.direction, &pagparams.marker) {
        (Ascending, None) => Box::new(search_tree.iter().take(limit)),
        (Descending, None) => Box::new(search_tree.iter().rev().take(limit)),
        (Ascending, Some(start_value)) => Box::new(
            search_tree
                .range((Bound::Excluded(*start_value), Bound::Unbounded))
                .take(limit),
        ),
        (Descending, Some(start_value)) => Box::new(
            search_tree
                .range((Bound::Unbounded, Bound::Excluded(*start_value)))
                .rev()
                .take(limit),
        ),
    }
}
