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

use cyclors::qos::{HistoryKind, Qos};
use cyclors::{dds_entity_t, DDS_LENGTH_UNLIMITED};
use serde::{Serialize, Serializer};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, fmt};
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh_ext::{PublicationCache, SessionExt};

use crate::{dds_discovery::*, qos_helpers::*, Config, KE_PREFIX_PUB_CACHE};

enum ZPublisher<'a> {
    Publisher(KeyExpr<'a>),
    PublicationCache(PublicationCache<'a>),
}

impl ZPublisher<'_> {
    fn key_expr(&self) -> &KeyExpr<'_> {
        match self {
            ZPublisher::Publisher(k) => k,
            ZPublisher::PublicationCache(p) => p.key_expr(),
        }
    }
}

fn serialize_zpublisher<S>(zpub: &ZPublisher, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(zpub.key_expr().as_str())
}

// a route from DDS to Zenoh
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RoutePublisher<'a> {
    // the ROS2 Publisher name
    name: String,
    // the ROS2 type
    typ: String,
    // the local DDS Reader created to serve the route (i.e. re-publish to zenoh data coming from DDS)
    #[serde(serialize_with = "serialize_entity_guid")]
    dds_reader: dds_entity_t,
    // the zenoh publisher used to re-publish to zenoh the data received by the DDS Reader
    #[serde(serialize_with = "serialize_zpublisher")]
    zenoh_publisher: ZPublisher<'a>,
    // the list of remote routes served by this route (admin key expr)
    remote_routes: HashSet<OwnedKeyExpr>,
    // the list of nodes served by this route
    pub(crate) local_nodes: HashSet<String>,
}

impl Drop for RoutePublisher<'_> {
    fn drop(&mut self) {
        if let Err(e) = delete_dds_entity(self.dds_reader) {
            log::warn!("{}: error deleting DDS Reader:  {}", self, e);
        }
    }
}

impl fmt::Display for RoutePublisher<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Publisher ({} -> {})",
            self.name,
            self.zenoh_publisher.key_expr()
        )
    }
}

impl RoutePublisher<'_> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<'a>(
        ros2_name: String,
        ros2_type: String,
        config: &Config,
        plugin_id: &keyexpr,
        zsession: &'a Arc<Session>,
        participant: dds_entity_t,
        topic_name: String,
        topic_type: String,
        type_info: &Option<Arc<TypeInfo>>,
        keyless: bool,
        reader_qos: Qos,
        ke: OwnedKeyExpr,
        congestion_ctrl: CongestionControl,
    ) -> Result<RoutePublisher<'a>, String> {
        log::debug!("Route Publisher ({ros2_name} -> {ke}): creation with type {ros2_type}");

        // declare the zenoh key expression
        let declared_ke = zsession
            .declare_keyexpr(ke.clone())
            .res()
            .await
            .map_err(|e| {
                format!("Route Publisher ({ros2_name} -> {ke}): failed to declare KeyExpr: {e}")
            })?;

        // declare the zenoh Publisher
        let zenoh_publisher: ZPublisher<'a> = if is_transient_local(&reader_qos) {
            #[allow(non_upper_case_globals)]
            let history_qos = get_history_or_default(&reader_qos);
            let durability_service_qos = get_durability_service_or_default(&reader_qos);
            let history = match (history_qos.kind, history_qos.depth) {
                (HistoryKind::KEEP_LAST, n) => {
                    if keyless {
                        // only 1 instance => history=n
                        n as usize
                    } else if durability_service_qos.max_instances == DDS_LENGTH_UNLIMITED {
                        // No limit! => history=MAX
                        usize::MAX
                    } else if durability_service_qos.max_instances > 0 {
                        // Compute cache size as history.depth * durability_service.max_instances
                        // This makes the assumption that the frequency of publication is the same for all instances...
                        // But as we have no way to have 1 cache per-instance, there is no other choice.
                        if let Some(m) = n.checked_mul(durability_service_qos.max_instances) {
                            m as usize
                        } else {
                            usize::MAX
                        }
                    } else {
                        n as usize
                    }
                }
                (HistoryKind::KEEP_ALL, _) => usize::MAX,
            };
            log::debug!(
                "Caching publications for TRANSIENT_LOCAL Writer on resource {} with history {} (Writer uses {:?} and DurabilityService.max_instances={})",
                ke, history, reader_qos.history, durability_service_qos.max_instances
            );
            let pub_cache = zsession
                .declare_publication_cache(&declared_ke)
                .history(history)
                .queryable_prefix(*KE_PREFIX_PUB_CACHE / plugin_id)
                .queryable_allowed_origin(Locality::Remote) // Note: don't reply to queries from local QueryingSubscribers
                .res()
                .await
                .map_err(|e| {
                    format!("Failed create PublicationCache for key {ke} (rid={declared_ke}): {e}")
                })?;
            ZPublisher::PublicationCache(pub_cache)
        } else {
            if let Err(e) = zsession.declare_publisher(declared_ke.clone()).res().await {
                log::warn!(
                    "Failed to declare publisher for key {} (rid={}): {}",
                    ke,
                    declared_ke,
                    e
                );
            }
            ZPublisher::Publisher(declared_ke.clone())
        };

        let read_period = get_read_period(&config, &ke);

        // create matching DDS Writer that forwards data coming from zenoh
        let dds_reader = create_forwarding_dds_reader(
            participant,
            topic_name.clone(),
            topic_type.clone(),
            type_info,
            keyless,
            reader_qos,
            declared_ke,
            zsession.clone(),
            read_period,
            congestion_ctrl,
        )?;

        Ok(RoutePublisher {
            name: ros2_name,
            typ: ros2_type,
            dds_reader,
            zenoh_publisher,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    #[inline]
    pub fn dds_reader_guid(&self) -> Result<String, String> {
        get_guid(&self.dds_reader)
    }

    #[inline]
    pub fn add_remote_route(&mut self, admin_ke: OwnedKeyExpr) {
        self.remote_routes.insert(admin_ke);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, admin_ke: &keyexpr) {
        self.remote_routes.remove(admin_ke);
    }

    /// Remove all routes reference with admin keyexpr containing "sub_ke"
    #[inline]
    pub fn remove_remote_routes(&mut self, sub_ke: &str) {
        self.remote_routes.retain(|s| !s.contains(sub_ke));
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub fn add_local_node(&mut self, node: String) {
        self.local_nodes.insert(node);
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        self.local_nodes.remove(node);
    }

    #[inline]
    pub fn is_serving_local_node(&self) -> bool {
        !self.local_nodes.is_empty()
    }

    #[inline]
    pub fn is_unused(&self) -> bool {
        !self.is_serving_local_node() && !self.is_serving_remote_route()
    }
}

// Return the read period if keyexpr matches one of the --dds-periodic-topics option
fn get_read_period(config: &Config, ke: &keyexpr) -> Option<Duration> {
    // for (re, freq) in &config.max_frequencies {
    //     if re.is_match(ke) {
    //         return Some(Duration::from_secs_f32(1f32 / freq));
    //     }
    // }
    None
}
