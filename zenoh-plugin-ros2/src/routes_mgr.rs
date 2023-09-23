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
use crate::config::Config;
use crate::discovered_entities::DiscoveredEntities;
use crate::discovered_entities::ROS2DiscoveryEvent;
use crate::node_info::TopicPub;
use crate::node_info::TopicSub;
use crate::qos_helpers::adapt_reader_qos_for_writer;
use crate::qos_helpers::adapt_writer_qos_for_reader;
use crate::qos_helpers::is_transient_local;
use crate::qos_helpers::is_writer_reliable;
use crate::route_publisher::RoutePublisher;
use crate::route_subscriber::RouteSubscriber;
use cyclors::dds_entity_t;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use zenoh::liveliness::LivelinessToken;
use zenoh::prelude::keyexpr;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::OwnedKeyExpr;
use zenoh::publication::CongestionControl;
use zenoh::queryable::Query;
use zenoh::sample::Sample;
use zenoh::Session;
use zenoh_core::zread;

use crate::ke_for_sure;

lazy_static::lazy_static!(
    static ref KE_PREFIX_ROUTE_PUBLISHER: &'static keyexpr = ke_for_sure!("route/topic/pub");
    static ref KE_PREFIX_ROUTE_SUBSCRIBER: &'static keyexpr = ke_for_sure!("route/topic/sub");
    static ref KE_PREFIX_ROUTE_SERVICE_SRV: &'static keyexpr = ke_for_sure!("route/service/srv");
    static ref KE_PREFIX_ROUTE_SERVICE_CLI: &'static keyexpr = ke_for_sure!("route/service/cli");
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RouteStatus {
    Routed(OwnedKeyExpr), // Routing is active, with the zenoh key expression used for the route
    NotAllowed,           // Routing was not allowed per configuration
    CreationFailure(String), // The route creation failed
    _QoSConflict,         // A route was already established but with conflicting QoS
}

#[derive(Debug)]
enum RouteRef {
    PublisherRoute(String),
    SubscriberRoute(String),
}

pub struct RoutesMgr<'a> {
    plugin_id: OwnedKeyExpr,
    config: Arc<Config>,
    zsession: &'a Arc<Session>,
    participant: dds_entity_t,
    discovered_entities: Arc<RwLock<DiscoveredEntities>>,
    // maps of established routes - ecah map indexed by topic/service/action name
    routes_publishers: HashMap<String, RoutePublisher<'a>>,
    routes_subscribers: HashMap<String, RouteSubscriber<'a>>,
    admin_prefix: OwnedKeyExpr,
    // admin space: index is the admin_keyexpr (relative to admin_prefix)
    admin_space: HashMap<OwnedKeyExpr, RouteRef>,
}

impl<'a> RoutesMgr<'a> {
    pub fn create(
        plugin_id: OwnedKeyExpr,
        config: Arc<Config>,
        zsession: &'a Arc<Session>,
        participant: dds_entity_t,
        discovered_entities: Arc<RwLock<DiscoveredEntities>>,
        admin_prefix: OwnedKeyExpr,
    ) -> RoutesMgr<'a> {
        RoutesMgr {
            plugin_id,
            config,
            zsession,
            participant,
            discovered_entities,
            routes_publishers: HashMap::new(),
            routes_subscribers: HashMap::new(),
            admin_prefix,
            admin_space: HashMap::new(),
        }
    }

    pub async fn update(&mut self, event: ROS2DiscoveryEvent) -> Result<(), String> {
        use ROS2DiscoveryEvent::*;
        match event {
            DiscoveredTopicPub(node, iface) => {
                self.update_route_publisher(&node, &iface).await?;
            }
            UndiscoveredTopicPub(node, iface) => {
                if let Entry::Occupied(mut entry) = self.routes_publishers.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.local_nodes.remove(&node);
                    if route.is_unused() {
                        log::info!("{route} unused - remove it");
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_PUBLISHER / iface.name_as_keyexpr()));
                        entry.remove();
                    }
                }
            }
            DiscoveredTopicSub(node, iface) => {
                self.update_route_subscriber(&node, &iface).await?;
            }
            UndiscoveredTopicSub(node, iface) => {
                if let Entry::Occupied(mut entry) =
                    self.routes_subscribers.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.local_nodes.remove(&node);
                    if route.is_unused() {
                        log::info!("{route} unused - remove it");
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SUBSCRIBER / iface.name_as_keyexpr()));
                        entry.remove();
                    }
                }
            }
            DiscoveredServiceSrv(node, iface) => {
                log::info!("... TODO: create Service Server route for {}", iface.name);
            }
            UndiscoveredServiceSrv(node, iface) => {
                log::info!("... TODO: delete Service Server route for {}", iface.name);
            }
            DiscoveredServiceCli(node, iface) => {
                log::info!("... TODO: create Service Client route for {}", iface.name);
            }
            UndiscoveredServiceCli(node, iface) => {
                log::info!("... TODO: delete Service Client route for {}", iface.name);
            }
            DiscoveredActionSrv(node, iface) => {
                log::info!("... TODO: create Action Server route for {}", iface.name);
            }
            UndiscoveredActionSrv(node, iface) => {
                log::info!("... TODO: delete Action Server route for {}", iface.name);
            }
            DiscoveredActionCli(node, iface) => {
                log::info!("... TODO: create Action Client route for {}", iface.name);
            }
            UndiscoveredActionCli(node, iface) => {
                log::info!("... TODO: delete Action Client route for {}", iface.name);
            }
        }
        Ok(())
    }

    async fn update_route_publisher(&mut self, node: &str, iface: &TopicPub) -> Result<(), String> {
        if let Some(route) = self.routes_publishers.get_mut(&iface.name) {
            route.add_local_node(node.into());
            log::debug!(
                "{route} already exists, now serving nodes {:?}",
                route.local_nodes
            );
            return Ok(());
        }

        // Retrieve info on DDS Writer
        let entity = {
            let entities = zread!(self.discovered_entities);
            entities
                .get_writer(&iface.writer)
                .ok_or(format!(
                    "Failed to get DDS info for {iface} Writer {}. Already deleted ?",
                    iface.writer
                ))?
                .clone()
        };

        // Zenoh key expression to use for routing
        // TODO: remap option ?
        let ke = iface.name_as_keyexpr().to_owned();

        // create route
        let mut route = RoutePublisher::create(
            &self.config,
            &self.plugin_id,
            &self.zsession,
            self.participant,
            iface.name.clone(),
            iface.typ.clone(),
            entity,
            ke,
        )
        .await?;
        route.add_local_node(node.into());
        log::info!("{route} created");

        // insert reference in admin_space
        let admin_ke = *KE_PREFIX_ROUTE_PUBLISHER / iface.name_as_keyexpr();
        self.admin_space
            .insert(admin_ke, RouteRef::PublisherRoute(iface.name.clone()));

        // insert route in routes_publishers map
        self.routes_publishers.insert(iface.name.clone(), route);
        Ok(())
    }

    async fn update_route_subscriber(
        &mut self,
        node: &str,
        iface: &TopicSub,
    ) -> Result<(), String> {
        if let Some(route) = self.routes_subscribers.get_mut(&iface.name) {
            route.add_local_node(node.into());
            log::debug!(
                "{route} already exists, now serving nodes {:?}",
                route.local_nodes
            );
            return Ok(());
        }

        // Retrieve info on DDS Reader
        let entity = {
            let entities = zread!(self.discovered_entities);
            entities
                .get_reader(&iface.reader)
                .ok_or(format!(
                    "Failed to get DDS info for {iface} Reader {}. Already deleted ?",
                    iface.reader
                ))?
                .clone()
        };

        // Zenoh key expression to use for routing
        // TODO: remap option ?
        let ke = iface.name_as_keyexpr().to_owned();

        // create route
        let mut route = RouteSubscriber::create(
            &self.config,
            &self.plugin_id,
            &self.zsession,
            self.participant,
            iface.name.clone(),
            iface.typ.clone(),
            entity,
            ke,
        )
        .await?;
        route.add_local_node(node.into());
        log::info!("{route} created");

        // insert reference in admin_space
        let admin_ke = *KE_PREFIX_ROUTE_SUBSCRIBER / iface.name_as_keyexpr();
        self.admin_space
            .insert(admin_ke, RouteRef::SubscriberRoute(iface.name.clone()));

        // insert route in routes_publishers map
        self.routes_subscribers.insert(iface.name.clone(), route);
        Ok(())
    }

    pub async fn treat_admin_query(&self, query: &Query) {
        let selector = query.selector();

        // get the list of sub-key expressions that will match the same stored keys than
        // the selector, if those keys had the admin_keyexpr_prefix.
        let sub_kes = selector.key_expr.strip_prefix(&self.admin_prefix);
        if sub_kes.is_empty() {
            log::error!("Received query for admin space: '{}' - but it's not prefixed by admin_keyexpr_prefix='{}'", selector, &self.admin_prefix);
            return;
        }

        // For all sub-key expression
        for sub_ke in sub_kes {
            if sub_ke.is_wild() {
                // iterate over all admin space to find matching keys and reply for each
                for (ke, route_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, ke, route_ref).await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(route_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, sub_ke, route_ref).await;
                }
            }
        }
    }

    async fn send_admin_reply(&self, query: &Query, key_expr: &keyexpr, route_ref: &RouteRef) {
        match self.get_entity_json_value(route_ref) {
            Ok(Some(v)) => {
                let admin_keyexpr = &self.admin_prefix / &key_expr;
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
