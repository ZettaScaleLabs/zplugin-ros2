use crate::ROS2PluginRuntime;
//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use crate::dds_discovery::*;
use crate::discovered_entities::DiscoveredEntities;
use crate::discovered_entities::ROS2DiscoveryEvent;
use crate::ros_discovery::*;
use crate::route_topic_dds_zenoh::RouteDDSZenoh;
use crate::route_topic_zenoh_dds::RouteZenohDDS;
use async_std::task;
use cyclors::dds_entity_t;
use cyclors::qos::Qos;
use flume::{unbounded, Receiver, Sender};
use futures::select;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use zenoh::prelude::keyexpr;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::OwnedKeyExpr;
use zenoh::publication::CongestionControl;
use zenoh::queryable::Query;
use zenoh::sample::Sample;
use zenoh_core::zread;
use zenoh_core::zwrite;
use zenoh_util::{TimedEvent, Timer};

use crate::ke_for_sure;
use crate::ChannelEvent;
use crate::ROS_DISCOVERY_INFO_POLL_INTERVAL_MS;

lazy_static::lazy_static!(
    static ref KE_PREFIX_ROUTE_PUBLISHER: &'static keyexpr = ke_for_sure!("route/topic/pub");
    static ref KE_PREFIX_ROUTE_SUBSCRIBER: &'static keyexpr = ke_for_sure!("route/topic/sub");
    static ref KE_PREFIX_ROUTE_SERVICE_SRV: &'static keyexpr = ke_for_sure!("route/service/srv");
    static ref KE_PREFIX_ROUTE_SERVICE_CLI: &'static keyexpr = ke_for_sure!("route/service/cli");
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum RouteStatus {
    Routed(OwnedKeyExpr), // Routing is active, with the zenoh key expression used for the route
    NotAllowed,           // Routing was not allowed per configuration
    CreationFailure(String), // The route creation failed
    _QoSConflict,         // A route was already established but with conflicting QoS
}

#[derive(Debug)]
enum RouteRef {
    PublisherRoute(OwnedKeyExpr),
    SubscriberRoute(OwnedKeyExpr),
}

pub struct RoutesMgr<'a> {
    participant: dds_entity_t,
    // maps of established routes from/to DDS (indexed by zenoh key expression)
    routes_publishers: HashMap<OwnedKeyExpr, RouteDDSZenoh<'a>>,
    routes_subscribers: HashMap<OwnedKeyExpr, RouteZenohDDS<'a>>,
    // admin space: index is the admin_keyexpr (relative to admin_prefix)
    admin_space: HashMap<OwnedKeyExpr, RouteRef>,
}

impl<'a> RoutesMgr<'a> {
    pub fn create(participant: dds_entity_t) -> RoutesMgr<'a> {
        RoutesMgr {
            participant,
            routes_publishers: HashMap::new(),
            routes_subscribers: HashMap::new(),
            admin_space: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn try_add_route_from_dds(
        &mut self,
        plugin: &ROS2PluginRuntime<'a>,
        ke: OwnedKeyExpr,
        topic_name: &str,
        topic_type: &str,
        type_info: &Option<TypeInfo>,
        keyless: bool,
        reader_qos: Qos,
        congestion_ctrl: CongestionControl,
    ) -> RouteStatus {
        if self.routes_publishers.contains_key(&ke) {
            // TODO: check if there is no QoS conflict with existing route
            log::debug!(
                "Route from DDS to resource {} already exists -- ignoring",
                ke
            );
            return RouteStatus::Routed(ke);
        }

        // create route DDS->Zenoh
        match RouteDDSZenoh::new(
            plugin,
            topic_name.into(),
            topic_type.into(),
            type_info,
            keyless,
            reader_qos,
            ke.clone(),
            congestion_ctrl,
        )
        .await
        {
            Ok(route) => {
                log::info!("{}: created with topic_type={}", route, topic_type);
                self.add_route_from_dds(ke.clone(), route);
                RouteStatus::Routed(ke)
            }
            Err(e) => {
                log::error!(
                    "Route DDS->Zenoh ({} -> {}): creation failed: {}",
                    topic_name,
                    ke,
                    e
                );
                RouteStatus::CreationFailure(e)
            }
        }
    }

    async fn try_add_route_to_dds(
        &mut self,
        plugin: &ROS2PluginRuntime<'a>,
        ke: OwnedKeyExpr,
        topic_name: &str,
        topic_type: &str,
        keyless: bool,
        is_transient: bool,
        writer_qos: Qos,
    ) -> RouteStatus {
        if let Some(route) = self.routes_subscribers.get(&ke) {
            // TODO: check if there is no type or QoS conflict with existing route
            log::debug!(
                "Route from resource {} to DDS already exists -- ignoring",
                ke
            );
            return RouteStatus::Routed(ke);
        }

        // create route Zenoh->DDS
        match RouteZenohDDS::new(
            plugin,
            ke.clone(),
            is_transient,
            topic_name.into(),
            topic_type.into(),
            keyless,
            writer_qos,
        )
        .await
        {
            Ok(route) => {
                log::info!("{}: created with topic_type={}", route, topic_type);
                self.add_route_to_dds(ke.clone(), route);
                RouteStatus::Routed(ke)
            }
            Err(e) => {
                log::error!(
                    "Route Zenoh->DDS ({} -> {}): creation failed: {}",
                    ke,
                    topic_name,
                    e
                );
                RouteStatus::CreationFailure(e)
            }
        }
    }

    fn add_route_from_dds(&mut self, ke: OwnedKeyExpr, r: RouteDDSZenoh<'a>) {
        // insert reference in admin_space
        let admin_ke = *KE_PREFIX_ROUTE_PUBLISHER / &ke;
        self.admin_space
            .insert(admin_ke, RouteRef::PublisherRoute(ke.clone()));

        // insert route in routes_publishers map
        self.routes_publishers.insert(ke, r);
    }

    fn add_route_to_dds(&mut self, ke: OwnedKeyExpr, r: RouteZenohDDS<'a>) {
        // insert reference in admin_space
        let admin_ke = *KE_PREFIX_ROUTE_SUBSCRIBER / &ke;
        self.admin_space
            .insert(admin_ke, RouteRef::SubscriberRoute(ke.clone()));

        // insert route in routes_publishers map
        self.routes_subscribers.insert(ke, r);
    }

    pub async fn treat_admin_query(&self, query: &Query, admin_keyexpr_prefix: &keyexpr) {
        let selector = query.selector();

        // get the list of sub-key expressions that will match the same stored keys than
        // the selector, if those keys had the admin_keyexpr_prefix.
        let sub_kes = selector.key_expr.strip_prefix(admin_keyexpr_prefix);
        if sub_kes.is_empty() {
            log::error!("Received query for admin space: '{}' - but it's not prefixed by admin_keyexpr_prefix='{}'", selector, admin_keyexpr_prefix);
            return;
        }

        // For all sub-key expression
        for sub_ke in sub_kes {
            if sub_ke.is_wild() {
                // iterate over all admin space to find matching keys and reply for each
                for (ke, route_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, admin_keyexpr_prefix, ke, route_ref)
                            .await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(route_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, admin_keyexpr_prefix, sub_ke, route_ref)
                        .await;
                }
            }
        }
    }

    async fn send_admin_reply(
        &self,
        query: &Query,
        admin_keyexpr_prefix: &keyexpr,
        key_expr: &keyexpr,
        route_ref: &RouteRef,
    ) {
        match self.get_entity_json_value(route_ref) {
            Ok(Some(v)) => {
                let admin_keyexpr = admin_keyexpr_prefix / &key_expr;
                if let Err(e) = query
                    .reply(Ok(Sample::new(admin_keyexpr, v)))
                    .res_async()
                    .await
                {
                    log::warn!("Error replying to admin query {:?}: {}", query, e);
                }
            }
            Ok(None) => log::error!("INTERNAL ERROR: Dangling {:?} for {}", route_ref, key_expr),
            Err(e) => {
                log::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }

    fn get_entity_json_value(
        &self,
        route_ref: &RouteRef,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match route_ref {
            RouteRef::PublisherRoute(ke) => self
                .routes_publishers
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
            RouteRef::SubscriberRoute(ke) => self
                .routes_subscribers
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
        }
    }
}
