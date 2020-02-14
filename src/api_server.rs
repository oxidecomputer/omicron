/*!
 * server-wide state and facilities
 */

use actix_http::Request;
use actix_http::Response;
use actix_service::IntoServiceFactory;
use actix_service::ServiceFactory;
use actix_web::App;
use actix_web::dev::AppConfig;
use actix_web::dev::MessageBody;
use actix_web::dev::Service;
use actix_web::error::Error;
use actix_web::web::Data;
use actix_web::web::ServiceConfig;
use core::fmt::Debug;
use std::sync::Arc;

use crate::api_http_entrypoints;
use crate::api_model;
use crate::sim;
use sim::SimulatorBuilder;

/**
 * Stores shared state used by API endpoints
 */
pub struct ApiServerState {
    /** the API backend to use for servicing requests */
    pub backend: Arc<dyn api_model::ApiBackend>
}

/**
 * Set up initial server-wide shared state.
 */
static mut SERVER_STATE: Option<Data<ApiServerState>> = None;

pub fn init_server_state()
{
    let mut simbuilder = SimulatorBuilder::new();
    simbuilder.project_create("simproject1");
    simbuilder.project_create("simproject2");
    simbuilder.project_create("simproject3");

    unsafe { SERVER_STATE = Some(Data::new(ApiServerState {
        backend: Arc::new(simbuilder.build())
    })); }
}

pub fn configure_app(cfg: &mut ServiceConfig)
{
    let state = unsafe { SERVER_STATE.as_ref().unwrap().clone(); };
    cfg.data(state);
    api_http_entrypoints::register_api_entrypoints(cfg);
}

///**
// * Returns an Actix ServiceFactory, which encapsulates the application itself
// * (e.g., routes, application data, etc.)
// */
//pub fn setup_app<F, I, S, B>()
////    -> F
////    where
////        F: Fn() -> I + Send + Clone + 'static,
////        I: IntoServiceFactory<S>,
////        S: ServiceFactory<Config = AppConfig, Request = Request>,
////        S::Error: Into<Error> + 'static,
////        S::InitError: Debug,
////        S::Response: Into<Response<B>> + 'static,
////        <S::Service as Service>::Future: 'static,
////        B: MessageBody + 'static,
//    -> impl Fn() -> actix_web::app::App<actix_service::AppEntry, actix_http::body::Body>
//{
//    let app_state = setup_server_state();
//
//    move || {
//        App::new()
//            .app_data(app_state.clone())
//            .configure(api_http_entrypoints::register_api_entrypoints)
//    }
//}

// pub fn api_configure_app(cfg: &mut ServiceConfig)
// {
//     let app_state = setup_server_state();
// 
//     cfg.app_data(app_state.clone())
//         .configure(api_http_entrypoints::register_api_entrypoints)
// }

// use actix_service::ServiceFactory;
use actix_web::dev::{ServiceRequest, ServiceResponse};
// use actix_web::{web, App, Error, HttpResponse};

pub fn api_create_app(app_state: Data<ApiServerState>) -> App<
    impl ServiceFactory<
        Config = (),
        Request = ServiceRequest,
        Response = ServiceResponse<impl MessageBody>,
        Error = Error,
    >,
    impl MessageBody,
>
{
    App::new()
        .app_data(app_state.clone())
        .configure(api_http_entrypoints::register_api_entrypoints)
}
