// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Alert/webhook types for version INITIAL.

use api_identity::ObjectIdentity;
use chrono::{DateTime, Utc};
use omicron_common::api::external::{IdentityMetadata, ObjectIdentity};
use omicron_uuid_kinds::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct AlertSubscription(pub(crate) String);

impl AlertSubscription {
    pub(crate) const PATTERN: &str =
        r"^([a-zA-Z0-9_]+|\*|\*\*)(\.([a-zA-Z0-9_]+|\*|\*\*))*$";
}

impl schemars::JsonSchema for AlertSubscription {
    fn schema_name() -> String {
        "AlertSubscription".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                title: Some("A webhook event class subscription".to_string()),
                description: Some(
                    "A webhook event class subscription matches either a single event class exactly, or a glob pattern including wildcards that may match multiple event classes"
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: None,
                min_length: None,
                pattern: Some(AlertSubscription::PATTERN.to_string()),
            })),
            ..Default::default()
        }
        .into()
    }
}

// ALERTS

/// An alert class.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertClass {
    /// The name of the alert class.
    pub name: String,

    /// A description of what this alert class represents.
    pub description: String,
}

/// The configuration for an alert receiver.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct AlertReceiver {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The list of alert classes to which this receiver is subscribed.
    pub subscriptions: Vec<AlertSubscription>,

    /// Configuration specific to the kind of alert receiver that this is.
    pub kind: AlertReceiverKind,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertSubscriptionCreated {
    /// The new subscription added to the receiver.
    pub subscription: AlertSubscription,
}

/// The possible alert delivery mechanisms for an alert receiver.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AlertReceiverKind {
    Webhook(WebhookReceiverConfig),
}

/// The configuration for a webhook alert receiver.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq,
)]
pub struct WebhookReceiver {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The list of alert classes to which this receiver is subscribed.
    pub subscriptions: Vec<AlertSubscription>,

    #[serde(flatten)]
    pub config: WebhookReceiverConfig,
}

impl From<WebhookReceiver> for AlertReceiver {
    fn from(
        WebhookReceiver { identity, subscriptions, config }: WebhookReceiver,
    ) -> Self {
        Self {
            identity,
            subscriptions,
            kind: AlertReceiverKind::Webhook(config),
        }
    }
}

impl PartialEq<WebhookReceiver> for AlertReceiver {
    fn eq(&self, other: &WebhookReceiver) -> bool {
        // Will become refutable if/when more variants are added...
        #[allow(irrefutable_let_patterns)]
        let AlertReceiverKind::Webhook(ref config) = self.kind else {
            return false;
        };
        self.identity == other.identity
            && self.subscriptions == other.subscriptions
            && config == &other.config
    }
}

impl PartialEq<AlertReceiver> for WebhookReceiver {
    fn eq(&self, other: &AlertReceiver) -> bool {
        // Will become refutable if/when more variants are added...
        #[allow(irrefutable_let_patterns)]
        let AlertReceiverKind::Webhook(ref config) = other.kind else {
            return false;
        };
        self.identity == other.identity
            && self.subscriptions == other.subscriptions
            && &self.config == config
    }
}

/// Webhook-specific alert receiver configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WebhookReceiverConfig {
    /// The URL that webhook notification requests are sent to.
    pub endpoint: Url,
    // A list containing the IDs of the secret keys used to sign payloads sent
    // to this receiver.
    pub secrets: Vec<WebhookSecret>,
}

/// A list of the IDs of secrets associated with a webhook receiver.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookSecrets {
    pub secrets: Vec<WebhookSecret>,
}

/// A view of a shared secret key assigned to a webhook receiver.
///
/// Once a secret is created, the value of the secret is not available in the
/// API, as it must remain secret. Instead, secrets are referenced by their
/// unique IDs assigned when they are created.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WebhookSecret {
    /// The public unique ID of the secret.
    pub id: Uuid,

    /// The UTC timestamp at which this secret was created.
    pub time_created: DateTime<Utc>,
}

/// A delivery of a webhook event.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct AlertDelivery {
    /// The UUID of this delivery attempt.
    pub id: Uuid,

    /// The UUID of the alert receiver that this event was delivered to.
    #[schemars(with = "Uuid")]
    pub receiver_id: AlertReceiverUuid,

    /// The event class.
    pub alert_class: String,

    /// The UUID of the event.
    #[schemars(with = "Uuid")]
    pub alert_id: AlertUuid,

    /// The state of this delivery.
    pub state: AlertDeliveryState,

    /// Why this delivery was performed.
    pub trigger: AlertDeliveryTrigger,

    /// Individual attempts to deliver this webhook event, and their outcomes.
    pub attempts: AlertDeliveryAttempts,

    /// The time at which this delivery began (i.e. the event was dispatched to
    /// the receiver).
    pub time_started: DateTime<Utc>,
}

/// The state of a webhook delivery attempt.
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Deserialize,
    Serialize,
    JsonSchema,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryState {
    /// The webhook event has not yet been delivered successfully.
    ///
    /// Either no delivery attempts have yet been performed, or the delivery has
    /// failed at least once but has retries remaining.
    Pending,
    /// The webhook event has been delivered successfully.
    Delivered,
    /// The webhook delivery attempt has failed permanently and will not be
    /// retried again.
    Failed,
}

/// The reason an alert was delivered
#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Deserialize,
    Serialize,
    JsonSchema,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryTrigger {
    /// Delivery was triggered by the alert itself.
    Alert,
    /// Delivery was triggered by a request to resend the alert.
    Resend,
    /// This delivery is a liveness probe.
    Probe,
}

/// A list of attempts to deliver an alert to a receiver.
///
/// The type of the delivery attempt model depends on the receiver type, as it
/// may contain information specific to that delivery mechanism. For example,
/// webhook delivery attempts contain the HTTP status code of the webhook
/// request.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryAttempts {
    /// A list of attempts to deliver an alert to a webhook receiver.
    Webhook(Vec<WebhookDeliveryAttempt>),
}

/// An individual delivery attempt for a webhook event.
///
/// This represents a single HTTP request that was sent to the receiver, and its
/// outcome.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct WebhookDeliveryAttempt {
    /// The time at which the webhook delivery was attempted.
    pub time_sent: DateTime<Utc>,

    /// The attempt number.
    pub attempt: usize,

    /// The outcome of this delivery attempt: either the event was delivered
    /// successfully, or the request failed for one of several reasons.
    pub result: WebhookDeliveryAttemptResult,

    pub response: Option<WebhookDeliveryResponse>,
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
pub enum WebhookDeliveryAttemptResult {
    /// The webhook event has been delivered successfully.
    Succeeded,
    /// A webhook request was sent to the endpoint, and it
    /// returned a HTTP error status code indicating an error.
    FailedHttpError,
    /// The webhook request could not be sent to the receiver endpoint.
    FailedUnreachable,
    /// A connection to the receiver endpoint was successfully established, but
    /// no response was received within the delivery timeout.
    FailedTimeout,
}

/// The response received from a webhook receiver endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct WebhookDeliveryResponse {
    /// The HTTP status code returned from the webhook endpoint.
    pub status: u16,
    /// The response time of the webhook endpoint, in milliseconds.
    pub duration_ms: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct AlertDeliveryId {
    pub delivery_id: Uuid,
}

/// Data describing the result of an alert receiver liveness probe attempt.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct AlertProbeResult {
    /// The outcome of the probe delivery.
    pub probe: AlertDelivery,
    /// If the probe request succeeded, and resending failed deliveries on
    /// success was requested, the number of new delivery attempts started.
    /// Otherwise, if the probe did not succeed, or resending failed deliveries
    /// was not requested, this is null.
    ///
    /// Note that this may be 0, if there were no events found which had not
    /// been delivered successfully to this receiver.
    pub resends_started: Option<usize>,
}

// ALERT PARAMS

use omicron_common::api::external::{
    IdentityMetadataCreateParams, IdentityMetadataUpdateParams, NameOrId,
};

/// Query params for listing alert classes.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertClassFilter {
    /// An optional glob pattern for filtering alert class names.
    ///
    /// If provided, only alert classes which match this glob pattern will be
    /// included in the response.
    pub filter: Option<AlertSubscription>,
}

#[derive(Deserialize, JsonSchema)]
pub struct AlertSelector {
    /// UUID of the alert
    pub alert_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertSubscriptionSelector {
    /// The webhook receiver that the subscription is attached to.
    #[serde(flatten)]
    pub receiver: AlertReceiverSelector,
    /// The event class subscription itself.
    pub subscription: AlertSubscription,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertClassPage {
    /// The last webhook event class returned by a previous page.
    pub last_seen: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertReceiverSelector {
    /// The name or ID of the webhook receiver.
    pub receiver: NameOrId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The URL that webhook notification requests should be sent to
    pub endpoint: Url,

    /// A non-empty list of secret keys used to sign webhook payloads.
    pub secrets: Vec<String>,

    /// A list of webhook event class subscriptions.
    ///
    /// If this list is empty or is not included in the request body, the
    /// webhook will not be subscribed to any events.
    #[serde(default)]
    pub subscriptions: Vec<AlertSubscription>,
}

/// Parameters to update a webhook configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookReceiverUpdate {
    #[serde(flatten)]
    pub identity: IdentityMetadataUpdateParams,

    /// The URL that webhook notification requests should be sent to
    pub endpoint: Option<Url>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertSubscriptionCreate {
    /// The event class pattern to subscribe to.
    pub subscription: AlertSubscription,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookSecretCreate {
    /// The value of the shared secret key.
    pub secret: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct WebhookSecretSelector {
    /// ID of the secret.
    pub secret_id: Uuid,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertDeliveryStateFilter {
    /// If true, include deliveries which are currently in progress.
    ///
    /// If any of the "pending", "failed", or "delivered" query parameters are
    /// set to true, only deliveries matching those state(s) will be included in
    /// the response. If NO state filter parameters are set, then all deliveries
    /// are included.
    ///
    /// A delivery is considered "pending" if it has not yet been sent at all,
    /// or if a delivery attempt has failed but the delivery has retries
    /// remaining.
    pub pending: Option<bool>,
    /// If true, include deliveries which have failed permanently.
    ///
    /// If any of the "pending", "failed", or "delivered" query parameters are
    /// set to true, only deliveries matching those state(s) will be included in
    /// the response. If NO state filter parameters are set, then all deliveries
    /// are included.
    ///
    /// A delivery fails permanently when the retry limit of three total
    /// attempts is reached without a successful delivery.
    pub failed: Option<bool>,
    /// If true, include deliveries which have succeeded.
    ///
    /// If any of the "pending", "failed", or "delivered" query parameters are
    /// set to true, only deliveries matching those state(s) will be included in
    /// the response. If NO state filter parameters are set, then all deliveries
    /// are included.
    pub delivered: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AlertReceiverProbe {
    /// If true, resend all events that have not been delivered successfully if
    /// the probe request succeeds.
    #[serde(default)]
    pub resend: bool,
}
