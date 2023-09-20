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
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, fmt};
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh_ext::{PublicationCache, SessionExt};

use crate::gid::Gid;
use crate::{dds_discovery::*, qos_helpers::*, Config, ROS2PluginRuntime, KE_PREFIX_PUB_CACHE};

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

// a route from DDS to Zenoh
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RoutePublisher<'a> {
    // the local DDS Reader created to serve the route (i.e. re-publish to zenoh data coming from DDS)
    #[serde(serialize_with = "serialize_entity_guid")]
    dds_reader: dds_entity_t,
    // the DDS topic name to read on
    topic_name: String,
    // the DDS topic type
    topic_type: String,
    // is DDS topic keyess
    keyless: bool,
    // the zenoh publisher used to re-publish to zenoh the data received by the DDS Reader
    #[serde(skip)]
    zenoh_publisher: ZPublisher<'a>,
    // the list of remote writers served by this route (admin key expr)
    remote_routed_readers: HashSet<OwnedKeyExpr>,
    // the list of local readers served by this route
    local_routed_writers: HashSet<Gid>,
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
            "Route DDS->Zenoh ({} -> {})",
            self.topic_name,
            self.zenoh_publisher.key_expr()
        )
    }
}

impl RoutePublisher<'_> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<'a>(
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
        log::debug!(
            "Route DDS->Zenoh ({} -> {}): creation with topic_type={}",
            topic_name,
            ke,
            topic_type
        );

        // declare the zenoh key expression
        let declared_ke = zsession
            .declare_keyexpr(ke.clone())
            .res()
            .await
            .map_err(|e| {
                format!("Route Zenoh->DDS ({topic_name} -> {ke}): failed to declare KeyExpr: {e}")
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
            dds_reader,
            topic_name,
            topic_type,
            keyless,
            zenoh_publisher,
            remote_routed_readers: HashSet::new(),
            local_routed_writers: HashSet::new(),
        })
    }

    pub fn dds_reader_guid(&self) -> Result<String, String> {
        get_guid(&self.dds_reader)
    }

    pub fn add_remote_routed_reader(&mut self, admin_ke: OwnedKeyExpr) {
        self.remote_routed_readers.insert(admin_ke);
    }

    pub fn remove_remote_routed_reader(&mut self, admin_ke: &keyexpr) {
        self.remote_routed_readers.remove(admin_ke);
    }

    /// Remove all Readers reference with admin keyexpr containing "sub_ke"
    pub fn remove_remote_routed_readers_containing(&mut self, sub_ke: &str) {
        self.remote_routed_readers.retain(|s| !s.contains(sub_ke));
    }

    pub fn has_remote_routed_reader(&self) -> bool {
        !self.remote_routed_readers.is_empty()
    }

    pub fn is_routing_remote_reader(&self, entity_key: &str) -> bool {
        self.remote_routed_readers
            .iter()
            .any(|s| s.contains(entity_key))
    }

    pub fn add_local_routed_writer(&mut self, entity_key: Gid) {
        self.local_routed_writers.insert(entity_key);
    }

    pub fn remove_local_routed_writer(&mut self, entity_key: &Gid) {
        self.local_routed_writers.remove(entity_key);
    }

    pub fn has_local_routed_writer(&self) -> bool {
        !self.local_routed_writers.is_empty()
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
