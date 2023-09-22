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
use async_trait::async_trait;
use cyclors::*;
use flume::{unbounded, Receiver, Sender};
use futures::select;
use git_version::git_version;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use std::env;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use zenoh::liveliness::LivelinessToken;
use zenoh::plugins::{Plugin, RunningPluginTrait, Runtime, ZenohPlugin};
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::queryable::Query;
use zenoh::Result as ZResult;
use zenoh::Session;
use zenoh_core::{bail, zerror};
use zenoh_ext::SubscriberBuilderExt;
use zenoh_util::Timed;

pub mod config;
mod dds_discovery;
mod discovered_entities;
mod discovery_mgr;
mod gid;
mod node_info;
mod qos_helpers;
mod ros_discovery;
mod route_publisher;
mod route_subscriber;
mod routes_mgr;
use config::Config;
use dds_discovery::*;

use crate::discovered_entities::ROS2DiscoveryEvent;
use crate::discovery_mgr::DiscoveryMgr;
use crate::routes_mgr::RoutesMgr;

pub const GIT_VERSION: &str = git_version!(prefix = "v", cargo_prefix = "v");

#[macro_export]
macro_rules! ke_for_sure {
    ($val:expr) => {
        unsafe { keyexpr::from_str_unchecked($val) }
    };
}

lazy_static::lazy_static!(
    pub static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
    pub static ref VERSION_JSON_VALUE: Value =
        serde_json::Value::String(LONG_VERSION.clone()).into();
    static ref LOG_PAYLOAD: bool = std::env::var("Z_LOG_PAYLOAD").is_ok();

    static ref KE_ANY_1_SEGMENT: &'static keyexpr = ke_for_sure!("*");
    static ref KE_ANY_N_SEGMENT: &'static keyexpr = ke_for_sure!("**");

    static ref KE_PREFIX_PUB_CACHE: &'static keyexpr = ke_for_sure!("@ros2_pub_cache");
);

zenoh::kedefine!(
    pub ke_admin_version: "${plugin_status_key:**}/__version__",
    pub ke_admin_prefix: "@ros2/${id:*}/",
    pub ke_liveliness_plugin: "@ros2/${plugin_id:**}",
);

// CycloneDDS' localhost-only: set network interface address (shortened form of config would be
// possible, too, but I think it is clearer to spell it out completely).
// Empty configuration fragments are ignored, so it is safe to unconditionally append a comma.
const CYCLONEDDS_CONFIG_LOCALHOST_ONLY: &str = r#"<CycloneDDS><Domain><General><Interfaces><NetworkInterface address="127.0.0.1" multicast="true"/></Interfaces></General></Domain></CycloneDDS>,"#;

// CycloneDDS' enable-shm: enable usage of Iceoryx shared memory
#[cfg(feature = "dds_shm")]
const CYCLONEDDS_CONFIG_ENABLE_SHM: &str = r#"<CycloneDDS><Domain><SharedMemory><Enable>true</Enable></SharedMemory></Domain></CycloneDDS>,"#;

const ROS_DISCOVERY_INFO_POLL_INTERVAL_MS: u64 = 100;

zenoh_plugin_trait::declare_plugin!(ROS2Plugin);

#[allow(clippy::upper_case_acronyms)]
pub struct ROS2Plugin;

impl ZenohPlugin for ROS2Plugin {}
impl Plugin for ROS2Plugin {
    type StartArgs = Runtime;
    type RunningPlugin = zenoh::plugins::RunningPlugin;

    const STATIC_NAME: &'static str = "zenoh-plugin-ros2";

    fn start(name: &str, runtime: &Self::StartArgs) -> ZResult<zenoh::plugins::RunningPlugin> {
        // Try to initiate login.
        // Required in case of dynamic lib, otherwise no logs.
        // But cannot be done twice in case of static link.
        let _ = env_logger::try_init();

        let runtime_conf = runtime.config.lock();
        let plugin_conf = runtime_conf
            .plugin(name)
            .ok_or_else(|| zerror!("Plugin `{}`: missing config", name))?;
        let config: Config = serde_json::from_value(plugin_conf.clone())
            .map_err(|e| zerror!("Plugin `{}` configuration error: {}", name, e))?;
        async_std::task::spawn(run(runtime.clone(), config));
        Ok(Box::new(ROS2Plugin))
    }
}
impl RunningPluginTrait for ROS2Plugin {
    fn config_checker(&self) -> zenoh::plugins::ValidationFunction {
        Arc::new(|_, _, _| bail!("ROS2Plugin does not support hot configuration changes."))
    }

    fn adminspace_getter<'a>(
        &'a self,
        selector: &'a Selector<'a>,
        plugin_status_key: &str,
    ) -> ZResult<Vec<zenoh::plugins::Response>> {
        let mut responses = Vec::new();
        let version_key = [plugin_status_key, "/__version__"].concat();
        if selector.key_expr.intersects(ke_for_sure!(&version_key)) {
            responses.push(zenoh::plugins::Response::new(
                version_key,
                GIT_VERSION.into(),
            ));
        }
        Ok(responses)
    }
}

pub async fn run(runtime: Runtime, config: Config) {
    // Try to initiate login.
    // Required in case of dynamic lib, otherwise no logs.
    // But cannot be done twice in case of static link.
    let _ = env_logger::try_init();
    log::debug!("ROS2 plugin {}", LONG_VERSION.as_str());
    log::info!("ROS2 plugin {:?}", config);

    // open zenoh-net Session
    let zsession = match zenoh::init(runtime).res_async().await {
        Ok(session) => Arc::new(session),
        Err(e) => {
            log::error!("Unable to init zenoh session for DDS plugin : {:?}", e);
            return;
        }
    };

    let plugin_id = zsession.zid();
    let ke_liveliness = zenoh::keformat!(ke_liveliness_plugin::formatter(), plugin_id).unwrap();
    let member = match zsession
        .liveliness()
        .declare_token(ke_liveliness)
        .res_async()
        .await
    {
        Ok(member) => member,
        Err(e) => {
            log::error!(
                "Unable to declare liveliness token for DDS plugin : {:?}",
                e
            );
            return;
        }
    };

    // if "ros_localhost_only" is set, configure CycloneDDS to use only localhost interface
    if config.ros_localhost_only {
        env::set_var(
            "CYCLONEDDS_URI",
            format!(
                "{}{}",
                CYCLONEDDS_CONFIG_LOCALHOST_ONLY,
                env::var("CYCLONEDDS_URI").unwrap_or_default()
            ),
        );
    }

    // if "enable_shm" is set, configure CycloneDDS to use Iceoryx shared memory
    #[cfg(feature = "dds_shm")]
    {
        if config.shm_enabled {
            env::set_var(
                "CYCLONEDDS_URI",
                format!(
                    "{}{}",
                    CYCLONEDDS_CONFIG_ENABLE_SHM,
                    env::var("CYCLONEDDS_URI").unwrap_or_default()
                ),
            );
            if config.forward_discovery {
                warn!("DDS shared memory support enabled but will not be used as forward discovery mode is active.");
            }
        }
    }

    // create DDS Participant
    log::debug!(
        "Create DDS Participant on domain {} with CYCLONEDDS_URI='{}'",
        config.domain,
        env::var("CYCLONEDDS_URI").unwrap_or_default()
    );
    let participant =
        unsafe { dds_create_participant(config.domain, std::ptr::null(), std::ptr::null()) };
    log::debug!(
        "ROS2 plugin {} using DDS Participant {} created",
        plugin_id,
        get_guid(&participant).unwrap()
    );

    let mut ros2_plugin = ROS2PluginRuntime {
        config: Arc::new(config),
        zsession: &zsession,
        participant,
        _member: member,
        plugin_id: plugin_id.into(),
        admin_space: HashMap::<OwnedKeyExpr, AdminRef>::new(),
    };

    ros2_plugin.run().await;
}

pub struct ROS2PluginRuntime<'a> {
    config: Arc<Config>,
    // Note: &'a Arc<Session> here to keep the ownership of Session outside this struct
    // and be able to store the publishers/subscribers it creates in this same struct.
    zsession: &'a Arc<Session>,
    participant: dds_entity_t,
    _member: LivelinessToken<'a>,
    plugin_id: OwnedKeyExpr,
    // admin space: index is the admin_keyexpr (relative to admin_prefix)
    // value is the JSon string to return to queries.
    admin_space: HashMap<OwnedKeyExpr, AdminRef>,
}

impl Serialize for ROS2PluginRuntime<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // return the plugin's config as a JSON struct
        let mut s = serializer.serialize_struct("dds", 3)?;
        s.serialize_field("domain", &self.config.domain)?;
        s.end()
    }
}

// An reference used in admin space to point to a struct (DdsEntity or Route) stored in another map
#[derive(Debug)]
enum AdminRef {
    Config,
    Version,
}

impl<'a> ROS2PluginRuntime<'a> {
    async fn run(&mut self) {
        // Subscribe to all liveliness info from other ROS2 plugins
        let ke_liveliness_all =
            zenoh::keformat!(ke_liveliness_plugin::formatter(), plugin_id = "**").unwrap();
        let group_subscriber = self
            .zsession
            .liveliness()
            .declare_subscriber(ke_liveliness_all)
            .querying()
            .with(flume::unbounded())
            .res_async()
            .await
            .expect("Failed to create Liveliness Subscriber");

        // Create and start DiscoveryManager
        let (tx, discovery_rcv): (Sender<ROS2DiscoveryEvent>, Receiver<ROS2DiscoveryEvent>) =
            unbounded();
        let mut discovery_mgr = DiscoveryMgr::create(self.participant);
        discovery_mgr.run(tx).await;

        // create RoutesManager
        let mut routes_mgr = RoutesMgr::create(
            self.plugin_id.clone(),
            self.config.clone(),
            self.zsession,
            self.participant,
            discovery_mgr.discovered_entities.clone(),
        );
        DiscoveryMgr::create(self.participant);

        // declare admin space queryable
        let admin_keyexpr_prefix = if let Some(ref id) = self.config.id {
            zenoh::keformat!(ke_admin_prefix::formatter(), id = id).unwrap()
        } else {
            zenoh::keformat!(ke_admin_prefix::formatter(), id = self.zsession.zid()).unwrap()
        };
        let admin_keyexpr_expr = (&admin_keyexpr_prefix) / *KE_ANY_N_SEGMENT;
        log::debug!("Declare admin space on {}", admin_keyexpr_expr);
        let admin_queryable = self
            .zsession
            .declare_queryable(admin_keyexpr_expr)
            .res_async()
            .await
            .expect("Failed to create AdminSpace queryable");

        // add plugin's config and version in admin space
        self.admin_space.insert(
            &admin_keyexpr_prefix / ke_for_sure!("config"),
            AdminRef::Config,
        );
        self.admin_space.insert(
            &admin_keyexpr_prefix / ke_for_sure!("version"),
            AdminRef::Version,
        );

        loop {
            select!(
                evt = discovery_rcv.recv_async() => {
                    match evt {
                        Ok(evt) => {
                            if self.is_allowed(&evt) {
                                log::info!("{evt} - Allowed");
                                if let Err(e) = routes_mgr.update(evt).await {
                                    log::warn!("Error updating route: {e}");
                                }
                            } else {
                                log::info!("{evt} - Denied per config");
                            }
                        }
                        Err(e) => log::error!("Internal Error: received from DiscoveryMgr: {e}")
                    }
                },

                group_event = group_subscriber.recv_async() => {
                    match group_event
                    {
                        Ok(evt) => {
                            let ke_parsed = ke_liveliness_plugin::parse(evt.key_expr.as_keyexpr());
                            let plugin_id = ke_parsed.map(|p| p.plugin_id().map(ToOwned::to_owned));
                            match (plugin_id, evt.kind) {
                                (Ok(Some(plugin_id)), SampleKind::Put) if plugin_id != self.plugin_id => {
                                    log::info!("New zenoh_ros2_plugin detected: {}", plugin_id);
                                }
                                (Ok(Some(plugin_id)), SampleKind::Delete) if plugin_id != self.plugin_id => {
                                    log::debug!("Remote zenoh_ros2_plugin left: {}", plugin_id);
                                }
                                (Ok(Some(_)), _) => (),
                                (Ok(None), _) | (Err(_), _) =>
                                log::warn!("Error receiving GroupEvent: invalid keyexpr '{}'", evt.key_expr)
                            }
                        },
                        Err(e) => log::warn!("Error receiving GroupEvent: {}", e)
                    }
                },

                get_request = admin_queryable.recv_async() => {
                    if let Ok(query) = get_request {
                        self.treat_admin_query(&query).await;
                        // pass query to discovery_mgr
                        discovery_mgr.treat_admin_query(&query, &admin_keyexpr_prefix);
                        // pass query to discovery_mgr
                        routes_mgr.treat_admin_query(&query, &admin_keyexpr_prefix).await;
                    } else {
                        log::warn!("AdminSpace queryable was closed!");
                    }
                }
            )
        }
    }

    fn is_allowed(&self, evt: &ROS2DiscoveryEvent) -> bool {
        if let Some(allowance) = &self.config.allowance {
            use ROS2DiscoveryEvent::*;
            match evt {
                DiscoveredTopicPub(_, iface) => allowance.is_publisher_allowed(&iface.name),
                DiscoveredTopicSub(_, iface) => allowance.is_subscriber_allowed(&iface.name),
                DiscoveredServiceSrv(_, iface) => allowance.is_service_srv_allowed(&iface.name),
                DiscoveredServiceCli(_, iface) => allowance.is_service_cli_allowed(&iface.name),
                DiscoveredActionSrv(_, iface) => allowance.is_action_srv_allowed(&iface.name),
                DiscoveredActionCli(_, iface) => allowance.is_action_cli_allowed(&iface.name),
                _ => true, // only Undiscovered events remain - always allow them (in case dynamic change of config is supported)
            }
        } else {
            // no allow/deny configured => allow all
            true
        }
    }

    async fn treat_admin_query(&self, query: &Query) {
        let query_ke = query.selector().key_expr;
        if query_ke.is_wild() {
            // iterate over all admin space to find matching keys and reply for each
            for (ke, admin_ref) in self.admin_space.iter() {
                if query_ke.intersects(ke) {
                    self.send_admin_reply(query, ke, admin_ref).await;
                }
            }
        } else {
            // sub_ke correspond to 1 key - just get it and reply
            let own_ke: OwnedKeyExpr = query_ke.into();
            if let Some(admin_ref) = self.admin_space.get(&own_ke) {
                self.send_admin_reply(query, &own_ke, admin_ref).await;
            }
        }
    }

    async fn send_admin_reply(&self, query: &Query, key_expr: &keyexpr, admin_ref: &AdminRef) {
        let value: Value = match admin_ref {
            AdminRef::Version => VERSION_JSON_VALUE.clone(),
            AdminRef::Config => match serde_json::to_value(self) {
                Ok(v) => v.into(),
                Err(e) => {
                    log::error!("INTERNAL ERROR serializing config as JSON: {}", e);
                    return;
                }
            },
        };
        if let Err(e) = query
            .reply(Ok(Sample::new(key_expr.to_owned(), value)))
            .res_async()
            .await
        {
            log::warn!("Error replying to admin query {:?}: {}", query, e);
        }
    }
}

//TODO replace when stable https://github.com/rust-lang/rust/issues/65816
#[inline]
pub fn vec_into_raw_parts<T>(v: Vec<T>) -> (*mut T, usize, usize) {
    let mut me = ManuallyDrop::new(v);
    (me.as_mut_ptr(), me.len(), me.capacity())
}

struct ChannelEvent {
    tx: Sender<()>,
}

#[async_trait]
impl Timed for ChannelEvent {
    async fn run(&mut self) {
        if self.tx.send(()).is_err() {
            log::warn!("Error sending periodic timer notification on channel");
        };
    }
}
