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

use cyclors::{
    dds_entity_t, dds_get_entity_sertype, dds_strretcode, dds_writecdr, ddsi_serdata_from_ser_iov,
    ddsi_serdata_kind_SDK_DATA, ddsi_sertype, ddsrt_iov_len_t, ddsrt_iovec_t,
};
use serde::{Serialize, Serializer};
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;
use std::{ffi::CStr, fmt, time::Duration};
use zenoh::liveliness::LivelinessToken;
use zenoh::prelude::*;
use zenoh::query::ReplyKeyExpr;
use zenoh::{prelude::r#async::AsyncResolve, subscriber::Subscriber};
use zenoh_ext::{FetchingSubscriber, SubscriberBuilderExt};

use crate::qos_helpers::{adapt_reader_qos_for_writer, is_transient_local, qos_to_key_expr};
use crate::{
    dds_discovery::*, ke_liveliness_sub, qos::Qos, vec_into_raw_parts, Config, KE_ANY_1_SEGMENT,
    KE_PREFIX_PUB_CACHE, LOG_PAYLOAD,
};

enum ZSubscriber<'a> {
    Subscriber(Subscriber<'a, ()>),
    FetchingSubscriber(FetchingSubscriber<'a, ()>),
}

impl ZSubscriber<'_> {
    fn key_expr(&self) -> &KeyExpr<'static> {
        match self {
            ZSubscriber::Subscriber(s) => s.key_expr(),
            ZSubscriber::FetchingSubscriber(s) => s.key_expr(),
        }
    }
}

fn serialize_zsubscriber<S>(zsub: &ZSubscriber, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(zsub.key_expr().as_str())
}

// a route from Zenoh to DDS
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteSubscriber<'a> {
    // the ROS2 Subscriber name
    name: String,
    // the ROS2 type
    typ: String,
    // the zenoh session
    #[serde(skip)]
    zenoh_session: &'a Arc<Session>,
    // the zenoh subscriber receiving data to be re-published by the DDS Writer
    #[serde(serialize_with = "serialize_zsubscriber")]
    zenoh_subscriber: ZSubscriber<'a>,
    // the local DDS Writer created to serve the route (i.e. re-publish to DDS data coming from zenoh)
    #[serde(serialize_with = "serialize_entity_guid")]
    dds_writer: dds_entity_t,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken<'a>>,
    // the list of remote routes served by this route (admin key expr)
    remote_routes: HashSet<OwnedKeyExpr>,
    // the list of nodes served by this route
    pub(crate) local_nodes: HashSet<String>,
}

impl Drop for RouteSubscriber<'_> {
    fn drop(&mut self) {
        if let Err(e) = delete_dds_entity(self.dds_writer) {
            log::warn!("{}: error deleting DDS Reader:  {}", self, e);
        }
    }
}

impl fmt::Display for RouteSubscriber<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Subscriber ({} -> {})",
            self.zenoh_subscriber.key_expr(),
            self.name
        )
    }
}

impl RouteSubscriber<'_> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<'a, 'b>(
        config: &Config,
        plugin_id: &keyexpr,
        zsession: &'a Arc<Session>,
        participant: dds_entity_t,
        ros2_name: String,
        ros2_type: String,
        reader: DdsEntity,
        ke: OwnedKeyExpr,
    ) -> Result<RouteSubscriber<'a>, String> {
        // If reader is transient_local, use a QueryingSubscriber
        let querying_subscriber = is_transient_local(&reader.qos);
        log::debug!("Route Subscriber ({ke} -> {ros2_name}): creation with type {ros2_type} (querying_subscriber:{querying_subscriber})");

        let dds_writer = create_forwarding_dds_writer(
            participant,
            reader.topic_name.clone(),
            reader.type_name.clone(),
            reader.keyless,
            adapt_reader_qos_for_writer(&reader.qos),
        )?;

        // Callback routing data received by Zenoh subscriber to DDS Writer (if set)
        let ton = reader.topic_name.clone();
        let subscriber_callback = move |s: Sample| {
            do_route_data(s, &ton, dds_writer);
        };

        // create zenoh subscriber
        let zenoh_subscriber = if querying_subscriber {
            // query all PublicationCaches on "<KE_PREFIX_PUB_CACHE>/*/<routing_keyexpr>"
            let query_selector: Selector = (*KE_PREFIX_PUB_CACHE / *KE_ANY_1_SEGMENT / &ke).into();
            log::debug!(
                    "Route Subscriber ({} -> {}): query historical data from everybody for TRANSIENT_LOCAL Reader on {}",
                    ke,
                    ros2_name,
                    query_selector
                );

            let sub = zsession
                .declare_subscriber(ke.clone())
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .reliable()
                .querying()
                .query_timeout(config.queries_timeout)
                .query_selector(query_selector)
                .query_accept_replies(ReplyKeyExpr::Any)
                .res()
                .await
                .map_err(|e| {
                    format!(
                        "Route Subscriber ({ke} -> {ros2_name}): failed to create FetchingSubscriber: {e}"
                    )
                })?;
            ZSubscriber::FetchingSubscriber(sub)
        } else {
            let sub = zsession
                .declare_subscriber(ke.clone())
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .reliable()
                .res()
                .await
                .map_err(|e| {
                    format!(
                        "Route Subscriber ({ke} -> {ros2_name}): failed to create Subscriber: {e}"
                    )
                })?;
            ZSubscriber::Subscriber(sub)
        };

        // create associated LivelinessToken
        let qos_ke = qos_to_key_expr(reader.keyless, &reader.qos);
        let token = zsession
            .liveliness()
            .declare_token(
                zenoh::keformat!(
                    ke_liveliness_sub::formatter(),
                    plugin_id,
                    ke,
                    typ = reader.type_name,
                    qos_ke
                )
                .unwrap(),
            )
            .res()
            .await
            .map_err(|e| {
                format!(
                    "Failed create LivelinessToken associated to route for Publisher {ros2_name}"
                )
            })?;

        Ok(RouteSubscriber {
            name: ros2_name,
            typ: ros2_type,
            zenoh_session: zsession,
            zenoh_subscriber,
            dds_writer,
            liveliness_token: Some(token),
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    /// If this route uses a FetchingSubscriber, query for historical publications
    /// using the specified Selector. Otherwise, do nothing.
    pub async fn query_historical_publications<'a, F>(
        &mut self,
        selector: F,
        query_timeout: Duration,
    ) where
        F: Fn() -> Selector<'a>,
    {
        if let ZSubscriber::FetchingSubscriber(sub) = &mut self.zenoh_subscriber {
            let s = selector();
            log::debug!(
                "Route Subscriber ({} -> {}): query historical publications from {}",
                sub.key_expr(),
                self.name,
                s
            );
            if let Err(e) = sub
                .fetch({
                    let session = &self.zenoh_session;
                    let s = s.clone();
                    move |cb| {
                        use zenoh_core::SyncResolve;
                        session
                            .get(&s)
                            .target(QueryTarget::All)
                            .consolidation(ConsolidationMode::None)
                            .accept_replies(ReplyKeyExpr::Any)
                            .timeout(query_timeout)
                            .callback(cb)
                            .res_sync()
                    }
                })
                .res()
                .await
            {
                log::warn!(
                    "{}: query for historical publications on {} failed: {}",
                    self,
                    s,
                    e
                );
            }
        }
    }

    #[inline]
    pub fn dds_writer_guid(&self) -> Result<String, String> {
        get_guid(&self.dds_writer)
    }

    #[inline]
    pub fn add_remote_route(&mut self, admin_ke: OwnedKeyExpr) {
        self.remote_routes.insert(admin_ke);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, admin_ke: &keyexpr) {
        self.remote_routes.remove(admin_ke);
    }

    /// Remove all Writers reference with admin keyexpr containing "sub_ke"
    #[inline]
    pub fn remove_remote_routes(&mut self, sub_ke: &str) {
        self.remote_routes.retain(|s| !s.contains(sub_ke));
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub fn add_local_node(&mut self, entity_key: String) {
        self.local_nodes.insert(entity_key);
    }

    #[inline]
    pub fn remove_local_node(&mut self, entity_key: &str) {
        self.local_nodes.remove(entity_key);
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

fn do_route_data(s: Sample, topic_name: &str, data_writer: dds_entity_t) {
    if *LOG_PAYLOAD {
        log::trace!(
            "Route Subscriber ({} -> {}): routing data - payload: {:?}",
            s.key_expr,
            &topic_name,
            s.value.payload
        );
    } else {
        log::trace!(
            "Route Subscriber ({} -> {}): routing data",
            s.key_expr,
            &topic_name
        );
    }

    unsafe {
        let bs = s.value.payload.contiguous().into_owned();
        // As per the Vec documentation (see https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_raw_parts)
        // the only way to correctly releasing it is to create a vec using from_raw_parts
        // and then have its destructor do the cleanup.
        // Thus, while tempting to just pass the raw pointer to cyclone and then free it from C,
        // that is not necessarily safe or guaranteed to be leak free.
        // TODO replace when stable https://github.com/rust-lang/rust/issues/65816
        let (ptr, len, capacity) = vec_into_raw_parts(bs);
        let size: ddsrt_iov_len_t = match len.try_into() {
            Ok(s) => s,
            Err(_) => {
                log::warn!(
                    "Route Subscriber ({} -> {}): can't route data; excessive payload size ({})",
                    s.key_expr,
                    topic_name,
                    len
                );
                return;
            }
        };

        let data_out = ddsrt_iovec_t {
            iov_base: ptr as *mut std::ffi::c_void,
            iov_len: size,
        };

        let mut sertype_ptr: *const ddsi_sertype = std::ptr::null_mut();
        let ret = dds_get_entity_sertype(data_writer, &mut sertype_ptr);
        if ret < 0 {
            log::warn!(
                "Route Subscriber ({} -> {}): can't route data; sertype lookup failed ({})",
                s.key_expr,
                topic_name,
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            );
            return;
        }

        let fwdp = ddsi_serdata_from_ser_iov(
            sertype_ptr,
            ddsi_serdata_kind_SDK_DATA,
            1,
            &data_out,
            size as usize,
        );

        dds_writecdr(data_writer, fwdp);
        drop(Vec::from_raw_parts(ptr, len, capacity));
    }
}
