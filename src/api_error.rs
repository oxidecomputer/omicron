/*!
 * API error handling facilities
 */

/*
 * XXX need to take a closer look at what error handling looks like.  What makes
 * this a little complicated is that it looks like each API function needs to
 * return a Result<_, E> where E must be an actual struct that implements the
 * actix_web::error::ResponseError trait.  That is, it can't be "dyn
 * actix_web::error::ResponseError", nor can it be a Box of that.
 *
 * We should probably look at the "fail"/"failure" crate, as that seems
 * widely used and there's some support for it in Actix.  There's also an Error
 * struct within Actix we could potentially use.
 *
 * We'll want to carefully design what our errors look like.  In principle:
 *
 * - One subtype of errors are associated with failed responses.  These all
 *   ought to have an HTTP status code.  Should there also be a hierarchy
 *   descending from ClientError and ServerError?
 * - There may be some errors that aren't associated with failed responses
 *   (e.g., errors serializing a particular row from a database, which may not
 *   result in a response error)
 * - All user-visible errors should probably have a string code (separate from
 *   the HTTP status code) and string description.
 * - It would be nice if some errors could include additional information (e.g.,
 *   validation errors could indicate which property was invalid)
 */
#[derive(Debug)]
pub struct ApiError {
}
impl actix_web::error::ResponseError for ApiError {
}
impl std::fmt::Display for ApiError {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // XXX What is this used for?  Should this emit JSON?
        // (We have to implement it in order to implement the
        // actix_web::error::ResponseError trait.)
        Ok(())
    }
}

impl From<serde_json::error::Error> for ApiError {
    fn from(_error: serde_json::error::Error)
        -> Self
    {
        // XXX
        ApiError {}
    }
}
