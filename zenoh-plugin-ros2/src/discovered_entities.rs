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

use std::collections::HashMap;
use std::fmt;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::{prelude::*, queryable::Query};

use crate::ros_discovery::NodeEntitiesInfo;
use crate::{
    dds_discovery::{DdsEntity, DdsParticipant},
    gid::Gid,
    node_info::*,
    ros_discovery::ParticipantEntitiesInfo,
};

#[derive(Default)]
pub struct DiscoveredEntities {
    participants: HashMap<Gid, DdsParticipant>,
    writers: HashMap<Gid, DdsEntity>,
    readers: HashMap<Gid, DdsEntity>,
    ros_participant_info: HashMap<Gid, ParticipantEntitiesInfo>,
    nodes_info: HashMap<Gid, HashMap<String, NodeInfo>>,
    admin_space: HashMap<OwnedKeyExpr, EntityRef>,
}

#[derive(Debug)]
enum EntityRef {
    Participant(Gid),
    Writer(Gid),
    Reader(Gid),
    Node(Gid, String),
}

zenoh::kedefine!(
    pub(crate) ke_admin_participant: "dds/${pgid:*}",
    pub(crate) ke_admin_writer: "dds/${pgid:*}/writer/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_reader: "dds/${pgid:*}/reader/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_node: "node/${pgid:*}/${fullname:**}",
);

impl DiscoveredEntities {
    #[inline]
    pub fn add_participant(&mut self, participant: DdsParticipant) {
        self.admin_space.insert(
            zenoh::keformat!(ke_admin_participant::formatter(), pgid = participant.key).unwrap(),
            EntityRef::Participant(participant.key),
        );
        self.participants.insert(participant.key, participant);
    }

    #[inline]
    pub fn remove_participant(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        // Remove Participant from participants list and from admin_space
        self.participants.remove(gid);
        self.admin_space
            .remove(&zenoh::keformat!(ke_admin_participant::formatter(), pgid = gid).unwrap());
        // Remove associated NodeInfos
        if let Some(nodes) = self.nodes_info.remove(gid) {
            for (name, mut node) in nodes {
                log::info!("Undiscovered ROS2 Node {}", name);
                self.admin_space.remove(
                    &zenoh::keformat!(
                        ke_admin_node::formatter(),
                        pgid = gid,
                        fullname = &name[1..],
                    )
                    .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
            }
        }
        events
    }

    #[inline]
    pub fn add_writer(&mut self, writer: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            zenoh::keformat!(
                ke_admin_writer::formatter(),
                pgid = writer.participant_key,
                wgid = writer.key,
                topic = &writer.topic_name,
            )
            .unwrap(),
            EntityRef::Writer(writer.key),
        );

        // Check if this Writer is present in some NodeInfo.undiscovered_writer list
        let mut event: Option<ROS2DiscoveryEvent> = None;
        for (_, nodes_map) in &mut self.nodes_info {
            for (_, node) in nodes_map {
                if let Some(i) = node
                    .undiscovered_writer
                    .iter()
                    .position(|gid| gid == &writer.key)
                {
                    // update the NodeInfo with this Writer's info
                    node.undiscovered_writer.remove(i);
                    event = node.update_with_writer(&writer);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Writers list
        self.writers.insert(writer.key, writer);
        event
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(writer) = self.writers.remove(gid) {
            self.admin_space.remove(
                &zenoh::keformat!(
                    ke_admin_writer::formatter(),
                    pgid = writer.participant_key,
                    wgid = writer.key,
                    topic = &writer.topic_name,
                )
                .unwrap(),
            );

            // Remove the Writer from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for (_, nodes_map) in &mut self.nodes_info {
                for (_, node) in nodes_map {
                    if let Some(e) = node.remove_writer(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            zenoh::keformat!(
                ke_admin_reader::formatter(),
                pgid = reader.participant_key,
                wgid = reader.key,
                topic = &reader.topic_name,
            )
            .unwrap(),
            EntityRef::Reader(reader.key),
        );

        // Check if this Reader is present in some NodeInfo.undiscovered_reader list
        let mut event = None;
        for (_, nodes_map) in &mut self.nodes_info {
            for (_, node) in nodes_map {
                if let Some(i) = node
                    .undiscovered_reader
                    .iter()
                    .position(|gid| gid == &reader.key)
                {
                    // update the NodeInfo with this Reader's info
                    node.undiscovered_reader.remove(i);
                    event = node.update_with_writer(&reader);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Readers list
        self.readers.insert(reader.key, reader);
        event
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(reader) = self.readers.remove(gid) {
            self.admin_space.remove(
                &zenoh::keformat!(
                    ke_admin_reader::formatter(),
                    pgid = reader.participant_key,
                    wgid = reader.key,
                    topic = &reader.topic_name,
                )
                .unwrap(),
            );

            // Remove the Reader from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for (_, nodes_map) in &mut self.nodes_info {
                for (_, node) in nodes_map {
                    if let Some(e) = node.remove_reader(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    pub fn update_participant_info(
        &mut self,
        ros_info: ParticipantEntitiesInfo,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        let Self {
            writers,
            readers,
            nodes_info,
            admin_space,
            ..
        } = self;
        let nodes_map = nodes_info.entry(ros_info.gid).or_insert_with(HashMap::new);

        // Remove nodes that are no longer present in ParticipantEntitiesInfo
        nodes_map.retain(|name, node| {
            if !ros_info.node_entities_info_seq.contains_key(name) {
                log::info!("Undiscovered ROS2 Node {}", name);
                admin_space.remove(
                    &zenoh::keformat!(
                        ke_admin_node::formatter(),
                        pgid = ros_info.gid,
                        fullname = &name[1..],
                    )
                    .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
                false
            } else {
                true
            }
        });

        // For each declared node in this ros_node_info
        for (name, ros_node_info) in &ros_info.node_entities_info_seq {
            // Get the corresponding NodeInfo, or create it if not existing
            let node: &mut NodeInfo = nodes_map.entry(name.into()).or_insert_with(|| {
                log::info!("Discovered ROS2 Node {}", name);
                self.admin_space.insert(
                    zenoh::keformat!(
                        ke_admin_node::formatter(),
                        pgid = ros_info.gid,
                        fullname = &name[1..],
                    )
                    .unwrap(),
                    EntityRef::Node(ros_info.gid, name.clone()),
                );
                NodeInfo::create(
                    ros_node_info.node_namespace.clone(),
                    ros_node_info.node_name.clone(),
                    ros_info.gid,
                )
            });
            // Update NodeInfo, adding resulting events to the list
            events.append(&mut Self::update_node_info(
                node,
                ros_node_info,
                readers,
                writers,
            ));
        }

        // Save ParticipantEntitiesInfo
        self.ros_participant_info.insert(ros_info.gid, ros_info);
        events
    }

    pub fn update_node_info(
        node: &mut NodeInfo,
        ros_node_info: &NodeEntitiesInfo,
        readers: &mut HashMap<Gid, DdsEntity>,
        writers: &mut HashMap<Gid, DdsEntity>,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events = Vec::new();
        // For each declared Reader
        for rgid in &ros_node_info.reader_gid_seq {
            if let Some(entity) = readers.get(rgid) {
                log::debug!(
                    "ROS2 Node {ros_node_info} declares Reader on {}",
                    entity.topic_name
                );
                let event: Option<ROS2DiscoveryEvent> = node.update_with_reader(entity);
                if let Some(e) = event {
                    log::info!("ROS2 Node {ros_node_info} declares {e}");
                    events.push(e);
                }
            } else {
                log::debug!(
                    "ROS2 Node {ros_node_info} declares a not yet discovered DDS Reader: {rgid}"
                );
                node.undiscovered_reader.push(*rgid);
            }
        }
        // For each declared Writer
        for wgid in &ros_node_info.writer_gid_seq {
            if let Some(entity) = writers.get(wgid) {
                log::debug!(
                    "ROS2 Node {ros_node_info} declares Writer on {}",
                    entity.topic_name
                );
                let event: Option<ROS2DiscoveryEvent> = node.update_with_writer(entity);
                if let Some(e) = event {
                    log::info!("ROS2 Node {ros_node_info} declares {e}");
                    events.push(e);
                }
            } else {
                log::debug!(
                    "ROS2 Node {ros_node_info} declares a not yet discovered DDS Writer: {wgid}"
                );
                node.undiscovered_writer.push(*wgid);
            }
        }
        events
    }

    fn get_entity_json_value(
        &self,
        entity_ref: &EntityRef,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match entity_ref {
            EntityRef::Participant(gid) => self
                .participants
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Writer(gid) => self
                .writers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Reader(gid) => self
                .readers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Node(gid, name) => self
                .nodes_info
                .get(gid)
                .map(|map| map.get(name))
                .flatten()
                .map(serde_json::to_value)
                .transpose(),
        }
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
                for (ke, entity_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, admin_keyexpr_prefix, ke, entity_ref)
                            .await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(entity_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, admin_keyexpr_prefix, sub_ke, entity_ref)
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
        entity_ref: &EntityRef,
    ) {
        match self.get_entity_json_value(entity_ref) {
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
            Ok(None) => log::error!("INTERNAL ERROR: Dangling {:?} for {}", entity_ref, key_expr),
            Err(e) => {
                log::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }
}

#[derive(Debug)]
pub enum ROS2DiscoveryEvent {
    DiscoveredTopicPub(TopicPub),
    UndiscoveredTopicPub(TopicPub),
    DiscoveredTopicSub(TopicSub),
    UndiscoveredTopicSub(TopicSub),
    DiscoveredServiceSrv(ServiceSrv),
    UndiscoveredServiceSrv(ServiceSrv),
    DiscoveredServiceCli(ServiceCli),
    UndiscoveredServiceCli(ServiceCli),
    DiscoveredActionSrv(ActionSrv),
    UndiscoveredActionSrv(ActionSrv),
    DiscoveredActionCli(ActionCli),
    UndiscoveredActionCli(ActionCli),
}

impl fmt::Display for ROS2DiscoveryEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ROS2DiscoveryEvent::*;
        match self {
            DiscoveredTopicPub(v) => write!(f, "Publisher {}: {}", v.name, v.typ),
            UndiscoveredTopicPub(n) => write!(f, "Publisher {}", n),
            DiscoveredTopicSub(v) => write!(f, "Subscriber {}: {}", v.name, v.typ),
            UndiscoveredTopicSub(n) => write!(f, "Subscriber {}", n),
            DiscoveredServiceSrv(v) => write!(f, "Service Server {}: {}", v.name, v.typ),
            UndiscoveredServiceSrv(n) => write!(f, "Service Server {}", n),
            DiscoveredServiceCli(v) => write!(f, "Service Client {}: {}", v.name, v.typ),
            UndiscoveredServiceCli(n) => write!(f, "Service Client {}", n),
            DiscoveredActionSrv(v) => write!(f, "Action Server {}: {}", v.name, v.typ),
            UndiscoveredActionSrv(n) => write!(f, "Action Server {}", n),
            DiscoveredActionCli(v) => write!(f, "Action Client {}: {}", v.name, v.typ),
            UndiscoveredActionCli(n) => write!(f, "Action Client {}", n),
        }
    }
}

// Remove any null QoS values from a serde_json::Value
fn remove_null_qos_values(
    value: Result<serde_json::Value, serde_json::Error>,
) -> Result<serde_json::Value, serde_json::Error> {
    match value {
        Ok(value) => match value {
            serde_json::Value::Object(mut obj) => {
                let qos = obj.get_mut("qos");
                if let Some(qos) = qos {
                    if qos.is_object() {
                        qos.as_object_mut().unwrap().retain(|_, v| !v.is_null());
                    }
                }
                Ok(serde_json::Value::Object(obj))
            }
            _ => Ok(value),
        },
        Err(error) => Err(error),
    }
}
