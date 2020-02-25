/*!
 * facilities related to the HTTP layer of the API
 */

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures::stream::StreamExt;
use http::status::StatusCode;
use hyper::Body;
use hyper::Response;
use serde::Serialize;
use std::sync::Arc;

use crate::api_error::ApiError;
use crate::api_error::ApiHttpError;
use crate::api_model::ApiObject;
use crate::api_model::ObjectStream;

/**
 * Given a `Result` representing an object in the API, serialize the object to
 * JSON bytes.  This function is expected to be used in the body of an API
 * request handler that's serializing a stream of API objects.  In particular,
 * if the `Result` is actually an error, that object is simply skipped, as
 * we can't fail the HTTP request in this case.  See comments below for details.
 *
 * This function currently always serializes to JSON.  If it becomes important
 * to support other formats (e.g., XML), this function could accept some context
 * associated with the HTTP request (or response) to indicate what format to use
 * and then use the right one.
 */
pub fn api_http_serialize_for_stream<T: Serialize>(
    maybe_object: &Result<T, ApiError>)
    -> Result<Bytes, ApiError>
{
    /*
     * This function is invoked for each item in a stream.  Each item is a
     * Result.  In the simple case of an ok result, we serialize the object via
     * serde and return the resulting bytes.  But what if we have an error?
     * This function is generally used in a context in which we're sending a
     * streaming HTTP response body to a client.  Critically, we've already
     * emitted a successful HTTP status code.  We have no good way to provide
     * the client with an error.  The only thing we could do is terminate the
     * response prematurely.  Indeed, if we were to emit an Err from this
     * function, Actix _will_ terminate the response.  A careful client will
     * know that something bad happened (because the content-length or chunk
     * lengths will not match what's expected) and will likely throw out the
     * whole response.  A less careful client may not realize that its response
     * was truncated and believe it has all of the data.  Either way, this isn't
     * a great situation.
     *
     * How could we get here?  This could happen if, for example, we were
     * streaming rows back from a database and one of the rows didn't pass a
     * validation check.  For now, we make the executive decision to simply skip
     * this item.  This isn't great, but again, it's not clear what a better
     * option is here.  At the very least, we ought to bump a counter and log a
     * warning (potentially throttled to avoid spamming our logs if we keep
     * hitting the same bad object).  None of this infrastructure exists yet.
     * TODO Make sure to add counters and log messages for this case.
     *
     * Note: one alternative would be to abandon streaming APIs altogether in
     * favor of bounded-size responses with pagination.  After all, we already
     * apply tight bounds to responses and support pagination.  If we buffer
     * everything up ahead of time, then we could potentially send a proper
     * error response for this case.  But this doesn't really solve the problem,
     * since we still have to decide whether to omit the problematic item or
     * fail the request.  And streaming responses is still useful for memory
     * usage and liveness.  So we settle for this approach.
     */
    let mut object_json_bytes = match maybe_object {
        Ok(object) => match serde_json::to_vec(object) {
            Ok(json_bytes) => json_bytes,
            Err(_) => vec![]
        },
        Err(_) => vec![]
    };

    /*
     * Append a newline after each object that we emit.
     * TODO We currently do this even in the error cases above because if we try
     * to pass a zero-byte buffer to Actix, it interprets that as the end of the
     * response.
     */
    object_json_bytes.push(b'\n');
    Ok(object_json_bytes.into())
}

/*
 * Helper functions for returning HTTP responses.
 */

/**
 * Return an HTTP response appropriate for having successfully created a
 * resource.  The status code is 201 "Created" and the body describes the given
 * ApiObject.
 */
pub fn api_http_create<T>(object: Arc<T>)
    -> Result<Response<Body>, ApiHttpError>
    where
        T: ApiObject
{
    let serialized = api_http_serialize_for_stream(&Ok(object.to_view()))?;
    // XXX need way to convert this stream to a real body
    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(serialized.into())?)
}

/**
 * Return an HTTP response appropriate for having successfully deleted a
 * resource.  This returns an empty 204 "No Content" response.
 */
pub fn api_http_delete()
    -> Result<Response<Body>, ApiHttpError>
{
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())?)
}

/**
 * Returns an HTTP response appropriate for fetching a single resource.  This
 * returns a 200 "OK" response whose body describes the given ApiObject.
 */
pub fn api_http_emit_one<T>(object: Arc<T>)
    -> Result<Response<Body>, ApiHttpError>
    where T: ApiObject
{
    let serialized = api_http_serialize_for_stream(&Ok(object.to_view()))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(serialized.into())?)
}

/**
 * Returns an HTTP response appropriate for streaming a sequence of resources
 * represented by `object_stream`.  This returns a 200 "OK" response whose body
 * contains the list of resources, newline-separated.  These are streamed out
 * asynchronously.
 * TODO It's weird that this is async and the other ones are not.  This is like
 * half-buffered and half-streaming.
 */
pub async fn api_http_emit_stream<T: 'static>(object_stream: ObjectStream<T>)
    -> Result<Response<Body>, ApiHttpError>
    where T: ApiObject
{
    let byte_stream = object_stream
        .map(|maybe_object| maybe_object.map(|object| object.to_view()))
        .map(|maybe_object| api_http_serialize_for_stream(&maybe_object));

    /*
     * TODO here's where the half-async, half-buffering rears its head...
     */
    let bufvec: Vec<Result<Bytes, ApiError>> = byte_stream.collect().await;
    let mut bytebuf = BytesMut::new();
    for result in bufvec {
        let bytes = result?;
        bytebuf.put(bytes);
    }

    /*
     * TODO Figure out if this is the right format (newline-separated JSON) and
     * if so whether it's a good content-type for this.
     * Is it important to be able to support different formats later?  (or
     * useful to factor the code so that we could?)
     * Source: https://github.com/ndjson/ndjson-spec#33-mediatype-and-file-extensions
     * used by cockroachdb, for example, and eleastic-search
     * TODO is it better if this is streaming all the way to the client?  That
     * would be nice, but is it more trouble than it's worth?
     */
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/x-ndjson")
        .body(bytebuf.freeze().into())?)
}
