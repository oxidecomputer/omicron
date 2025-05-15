// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Alerts

use crate::Nexus;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::model::Alert;
use nexus_db_queries::db::model::AlertClass;
use nexus_db_queries::db::model::AlertDeliveryTrigger;
use nexus_db_queries::db::model::WebhookDelivery;
use nexus_db_queries::db::model::WebhookReceiverConfig;
use nexus_types::external_api::params;
use nexus_types::external_api::shared;
use nexus_types::external_api::views;
use nexus_types::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::NameOrId;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::AlertReceiverUuid;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::WebhookDeliveryUuid;

impl Nexus {
    /// Publish a new alert, with the provided `id`, `alert_class`, and
    /// JSON data payload.
    ///
    /// If this method returns `Ok`, the event has been durably recorded in
    /// CockroachDB.  Once the new event record is inserted into the database,
    /// the webhook dispatcher background task is activated to dispatch the
    /// event to receivers.  However, if (for whatever reason) this Nexus fails
    /// to do that, the event remains durably in the database to be dispatched
    /// and delivered by someone else.
    pub async fn alert_publish(
        &self,
        opctx: &OpContext,
        id: AlertUuid,
        class: AlertClass,
        event: serde_json::Value,
    ) -> Result<Alert, Error> {
        let alert =
            self.datastore().alert_create(opctx, id, class, event).await?;
        slog::debug!(
            &opctx.log,
            "published alert";
            "alert_id" => ?id,
            "alert_class" => %alert.class,
            "time_created" => ?alert.identity.time_created,
        );

        // Once the alert has been inserted, activate the dispatcher task to
        // ensure its propagated to receivers.
        self.background_tasks.task_alert_dispatcher.activate();

        Ok(alert)
    }

    //
    // Lookups
    //

    pub fn alert_receiver_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        rx_selector: params::AlertReceiverSelector,
    ) -> LookupResult<lookup::AlertReceiver<'a>> {
        match rx_selector.receiver {
            NameOrId::Id(id) => {
                let rx = LookupPath::new(opctx, &self.db_datastore)
                    .alert_receiver_id(AlertReceiverUuid::from_untyped_uuid(
                        id,
                    ));
                Ok(rx)
            }
            NameOrId::Name(name) => {
                let rx = LookupPath::new(opctx, &self.db_datastore)
                    .alert_receiver_name_owned(name.into());
                Ok(rx)
            }
        }
    }

    pub fn alert_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        params::AlertSelector { alert_id }: params::AlertSelector,
    ) -> LookupResult<lookup::Alert<'a>> {
        let event = LookupPath::new(opctx, &self.db_datastore)
            .alert_id(AlertUuid::from_untyped_uuid(alert_id));
        Ok(event)
    }

    //
    // Alert class API
    //
    pub async fn alert_class_list(
        &self,
        opctx: &OpContext,
        filter: params::AlertClassFilter,
        pagparams: DataPageParams<'_, params::AlertClassPage>,
    ) -> ListResultVec<views::AlertClass> {
        opctx
            .authorize(authz::Action::ListChildren, &authz::ALERT_CLASS_LIST)
            .await?;
        Self::actually_list_alert_classes(filter, pagparams)
    }

    // This is factored out to avoid having to make a whole Nexus to test it.
    fn actually_list_alert_classes(
        params::AlertClassFilter { filter }: params::AlertClassFilter,
        pagparams: DataPageParams<'_, params::AlertClassPage>,
    ) -> ListResultVec<views::AlertClass> {
        use nexus_db_model::AlertSubscriptionKind;

        let regex = if let Some(filter) = filter {
            let sub = AlertSubscriptionKind::try_from(filter)?;
            let regex_string = match sub {
                AlertSubscriptionKind::Exact(class) => class.as_str(),
                AlertSubscriptionKind::Glob(ref glob) => glob.regex.as_str(),
            };
            let re = regex::Regex::new(regex_string).map_err(|e| {
                // This oughtn't happen, provided the code for producing the
                // regex for a glob is correct.
                Error::InternalError {
                    internal_message: format!(
                        "valid alert class globs ({sub:?}) should always \
                         produce a valid regex, and yet: {e:?}"
                    ),
                }
            })?;
            Some(re)
        } else {
            None
        };

        // If we're resuming a previous scan, figure out where to start.
        let start = if let Some(params::AlertClassPage { last_seen }) =
            pagparams.marker
        {
            let start = AlertClass::ALL_CLASSES.iter().enumerate().find_map(
                |(idx, class)| {
                    if class.as_str() == last_seen { Some(idx) } else { None }
                },
            );
            match start {
                Some(start) => start + 1,
                None => return Ok(Vec::new()),
            }
        } else {
            0
        };

        // This shouldn't ever happen, but...don't panic I guess.
        if start > AlertClass::ALL_CLASSES.len() {
            return Ok(Vec::new());
        }

        let result = AlertClass::ALL_CLASSES[start..]
            .iter()
            .filter_map(|&class| {
                // Skip test classes, as they should not be used in the public
                // API, except in test builds, where we need them
                // for, you know... testing...
                if !cfg!(test) && class.is_test() {
                    return None;
                }
                if let Some(ref regex) = regex {
                    if !regex.is_match(class.as_str()) {
                        return None;
                    }
                }
                Some(class.into())
            })
            .take(pagparams.limit.get() as usize)
            .collect::<Vec<_>>();
        Ok(result)
    }

    //
    // Receiver configuration API methods
    //

    pub async fn alert_receiver_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<WebhookReceiverConfig> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.datastore().alert_rx_list(opctx, pagparams).await
    }

    pub async fn alert_receiver_config_fetch(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
    ) -> LookupResult<WebhookReceiverConfig> {
        let (authz_rx, rx) = rx.fetch().await?;
        let (subscriptions, secrets) =
            self.datastore().webhook_rx_config_fetch(opctx, &authz_rx).await?;
        Ok(WebhookReceiverConfig { rx, secrets, subscriptions })
    }

    //
    // Receiver subscription API methods
    //

    pub async fn alert_receiver_subscription_add(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
        params::AlertSubscriptionCreate { subscription}: params::AlertSubscriptionCreate,
    ) -> CreateResult<views::AlertSubscriptionCreated> {
        let (authz_rx,) = rx.lookup_for(authz::Action::Modify).await?;
        let db_subscription = nexus_db_model::AlertSubscriptionKind::try_from(
            subscription.clone(),
        )?;
        let _ = self
            .datastore()
            .alert_subscription_add(opctx, &authz_rx, db_subscription)
            .await?;
        Ok(views::AlertSubscriptionCreated { subscription })
    }

    pub async fn alert_receiver_subscription_remove(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
        subscription: shared::AlertSubscription,
    ) -> DeleteResult {
        let (authz_rx,) = rx.lookup_for(authz::Action::Modify).await?;
        let db_subscription =
            nexus_db_model::AlertSubscriptionKind::try_from(subscription)?;
        let _ = self
            .datastore()
            .alert_subscription_remove(opctx, &authz_rx, db_subscription)
            .await?;
        Ok(())
    }

    pub async fn alert_receiver_resend(
        &self,
        opctx: &OpContext,
        rx: lookup::AlertReceiver<'_>,
        event: lookup::Alert<'_>,
    ) -> CreateResult<WebhookDeliveryUuid> {
        let (authz_rx,) = rx.lookup_for(authz::Action::CreateChild).await?;
        let (authz_event, event) = event.fetch().await?;
        let datastore = self.datastore();

        let is_subscribed = datastore
            .alert_rx_is_subscribed_to_alert(opctx, &authz_rx, &authz_event)
            .await?;
        if !is_subscribed {
            return Err(Error::invalid_request(format!(
                "cannot resend alert: receiver is not subscribed to the '{}' \
                 alert class",
                event.class,
            )));
        }

        let delivery = WebhookDelivery::new(
            &event.id(),
            &authz_rx.id(),
            AlertDeliveryTrigger::Resend,
        );
        let delivery_id = delivery.id.into();

        if let Err(e) =
            datastore.webhook_delivery_create_batch(opctx, vec![delivery]).await
        {
            slog::error!(
                &opctx.log,
                "failed to create new delivery to resend webhook alert";
                "rx_id" => ?authz_rx.id(),
                "alert_id" => ?authz_event.id(),
                "alert_class" => %event.class,
                "delivery_id" => ?delivery_id,
                "error" => %e,
            );
            return Err(e);
        }

        slog::info!(
            &opctx.log,
            "resending webhook event";
            "rx_id" => ?authz_rx.id(),
            "alert_id" => ?authz_event.id(),
            "alert_class" => %event.class,
            "delivery_id" => ?delivery_id,
        );

        self.background_tasks.task_webhook_deliverator.activate();
        Ok(delivery_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Nexus;
    use std::num::NonZeroU32;

    #[test]
    fn test_alert_class_list() {
        #[track_caller]
        fn list(
            filter: Option<&str>,
            last_seen: Option<&str>,
            limit: u32,
        ) -> Vec<String> {
            let filter = params::AlertClassFilter {
                filter: dbg!(filter).map(|f| f.parse().unwrap()),
            };
            let marker = dbg!(last_seen).map(|last_seen| {
                params::AlertClassPage { last_seen: last_seen.to_string() }
            });
            let result = Nexus::actually_list_alert_classes(
                filter,
                DataPageParams {
                    marker: marker.as_ref(),
                    direction: dropshot::PaginationOrder::Ascending,
                    limit: NonZeroU32::new(dbg!(limit)).unwrap(),
                },
            );

            // Throw away the description fields
            dbg!(result)
                .unwrap()
                .into_iter()
                .map(|view| view.name)
                .collect::<Vec<_>>()
        }

        // Paginated class list, without a glob filter.
        let classes = list(None, None, 3);
        assert_eq!(classes, &["probe", "test.foo", "test.foo.bar"]);
        let classes = list(None, Some("test.foo.bar"), 3);
        assert_eq!(
            classes,
            &["test.foo.baz", "test.quux.bar", "test.quux.bar.baz"]
        );
        // Don't assert that a third list will return no more results, since
        // more events may be added in the future, and we don't have a filter.

        // Try a filter for only `test.**` events.
        let filter = Some("test.**");
        let classes = list(filter, None, 2);
        assert_eq!(classes, &["test.foo", "test.foo.bar"]);
        let classes = list(filter, Some("test.foo.bar"), 2);
        assert_eq!(classes, &["test.foo.baz", "test.quux.bar"]);
        let classes = list(filter, Some("test.quux.bar"), 2);
        assert_eq!(classes, &["test.quux.bar.baz"]);
        let classes = list(filter, Some("test.quux.bar.baz"), 2);
        assert_eq!(classes, Vec::<String>::new());
    }
}
