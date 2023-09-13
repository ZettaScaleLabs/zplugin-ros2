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
use async_std::task;
use cyclors::dds_entity_t;
use flume::{unbounded, Receiver, Sender};
use futures::select;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use zenoh::prelude::keyexpr;
use zenoh::queryable::Query;
use zenoh_core::zread;
use zenoh_core::zwrite;
use zenoh_util::{TimedEvent, Timer};

use crate::ChannelEvent;
use crate::ROS_DISCOVERY_INFO_POLL_INTERVAL_MS;

pub struct DiscoveryMgr {
    participant: dds_entity_t,
    discovered_entities: Arc<RwLock<DiscoveredEntities>>,
}

impl DiscoveryMgr {
    pub fn create(participant: dds_entity_t) -> Result<DiscoveryMgr, String> {
        Ok(DiscoveryMgr {
            participant,
            discovered_entities: Arc::new(RwLock::new(Default::default())),
        })
    }

    pub async fn run(&mut self, evt_sender: Sender<ROS2DiscoveryEvent>) {
        // run DDS discovery
        let (dds_disco_snd, dds_disco_rcv): (
            Sender<DDSDiscoveryEvent>,
            Receiver<DDSDiscoveryEvent>,
        ) = unbounded();
        run_discovery(self.participant, dds_disco_snd);

        // run ROS2 discovery (periodic polling)
        let ros_disco_mgr = RosDiscoveryInfoMgr::create(self.participant)
            .expect("Failed to create RosDiscoveryInfoMgr");

        let discovered_entities = self.discovered_entities.clone();

        task::spawn(async move {
            // Timer for periodic read of "ros_discovery_info" topic
            let timer = Timer::default();
            let (tx, ros_disco_timer_rcv): (Sender<()>, Receiver<()>) = unbounded();
            let ros_disco_timer_event = TimedEvent::periodic(
                Duration::from_millis(ROS_DISCOVERY_INFO_POLL_INTERVAL_MS),
                ChannelEvent { tx },
            );
            timer.add_async(ros_disco_timer_event).await;

            loop {
                select!(
                    evt = dds_disco_rcv.recv_async() => {
                        match evt.unwrap() {
                            DDSDiscoveryEvent::DiscoveredParticipant {entity} => {
                                zwrite!(discovered_entities).add_participant(entity);
                            },
                            DDSDiscoveryEvent::UndiscoveredParticipant {key} => {
                                zwrite!(discovered_entities).remove_participant(&key);
                            },
                            DDSDiscoveryEvent::DiscoveredPublication{entity} => {
                                zwrite!(discovered_entities).add_writer(entity);
                            },
                            DDSDiscoveryEvent::UndiscoveredPublication{key} => {
                                zwrite!(discovered_entities).remove_writer(&key);
                            },
                            DDSDiscoveryEvent::DiscoveredSubscription {entity} => {
                                zwrite!(discovered_entities).add_reader(entity);
                            },
                            DDSDiscoveryEvent::UndiscoveredSubscription {key} => {
                                zwrite!(discovered_entities).remove_reader(&key);
                            },
                        }
                    }

                    _ = ros_disco_timer_rcv.recv_async() => {
                        let infos = ros_disco_mgr.read();
                        for part_info in infos {
                            log::info!("Received ros_discovery_info from {}", part_info);
                            zwrite!(discovered_entities).update_participant_info(part_info);
                        }
                    }
                )
            }
        });
    }

    pub fn treat_admin_query(&self, query: &Query, admin_keyexpr_prefix: &keyexpr) {
        // pass query to discovered_entities
        let discovered_entities = zread!(self.discovered_entities);
        // TODO: find a better solution than block_on()
        async_std::task::block_on(
            discovered_entities.treat_admin_query(query, admin_keyexpr_prefix),
        );
    }
}
