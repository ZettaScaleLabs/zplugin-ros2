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
    pub fn remove_participant(&mut self, gid: &Gid) {
        self.participants.remove(gid);
        // cleanup associated NodeInfos
        if let Some(nodes) = self.nodes_info.remove(gid) {
            for (name, _) in nodes {
                self.admin_space.remove(
                    &zenoh::keformat!(
                        ke_admin_node::formatter(),
                        pgid = gid,
                        fullname = &name[1..],
                    )
                    .unwrap(),
                );
            }
        }
        self.admin_space
            .remove(&zenoh::keformat!(ke_admin_participant::formatter(), pgid = gid).unwrap());
    }

    #[inline]
    pub fn add_writer(&mut self, writer: DdsEntity) {
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
        self.writers.insert(writer.key, writer);
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) {
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
        }
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) {
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
        self.readers.insert(reader.key, reader);
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) {
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
        }
    }

    pub fn update_participant_info(&mut self, ros_info: ParticipantEntitiesInfo) {
        let Self {
            writers,
            readers,
            nodes_info,
            admin_space,
            ..
        } = self;
        let nodes_map = nodes_info.entry(ros_info.gid).or_insert_with(HashMap::new);

        // Remove nodes that are no longer present in ParticipantEntitiesInfo
        nodes_map.retain(|name, _| {
            if !ros_info.node_entities_info_seq.contains_key(name) {
                log::warn!("=== REMOVING NODE {}", name);
                admin_space.remove(
                    &zenoh::keformat!(
                        ke_admin_node::formatter(),
                        pgid = ros_info.gid,
                        fullname = &name[1..],
                    )
                    .unwrap(),
                );
                false
            } else {
                true
            }
        });

        // Update (or add) each node
        for (name, ros_node_info) in &ros_info.node_entities_info_seq {
            log::warn!("=== UPDATING NODE {}", name);
            let node: &mut NodeInfo = nodes_map.entry(name.into()).or_insert_with(|| {
                NodeInfo::create(
                    ros_node_info.node_namespace.clone(),
                    ros_node_info.node_name.clone(),
                    ros_info.gid,
                )
            });
            Self::update_node_info(node, ros_node_info, readers, writers);
            self.admin_space.insert(
                zenoh::keformat!(
                    ke_admin_node::formatter(),
                    pgid = ros_info.gid,
                    fullname = &name[1..],
                )
                .unwrap(),
                EntityRef::Node(ros_info.gid, name.clone()),
            );
        }

        // Save ParticipantEntitiesInfo
        self.ros_participant_info.insert(ros_info.gid, ros_info);
    }

    pub fn update_node_info(
        node: &mut NodeInfo,
        ros_node_info: &NodeEntitiesInfo,
        readers: &mut HashMap<Gid, DdsEntity>,
        writers: &mut HashMap<Gid, DdsEntity>,
    ) {
        for rgid in &ros_node_info.reader_gid_seq {
            if let Some(entity) = readers.get(rgid) {
                log::info!("{ros_node_info} declares Reader on {}", entity.topic_name);
                let (topic_prefix, topic_suffix) = entity.topic_name.split_at(3);
                let event = match topic_prefix {
                    "rt/" if topic_suffix.ends_with("/_action/status") => node
                        .update_action_cli_status_reader(
                            &topic_suffix[..topic_suffix.len() - 15],
                            &entity.type_name,
                            rgid,
                        ),
                    "rt/" if topic_suffix.ends_with("/_action/feedback") => node
                        .update_action_cli_feedback_reader(
                            &topic_suffix[..topic_suffix.len() - 17],
                            &entity.type_name,
                            rgid,
                        ),
                    "rt/" => node.update_topic_sub(topic_suffix, &entity.type_name, rgid),
                    "rq/" if topic_suffix.ends_with("/_action/send_goalRequest") => node
                        .update_action_srv_send_req_reader(
                            &topic_suffix[..topic_suffix.len() - 25],
                            &entity.type_name,
                            rgid,
                        ),
                    "rq/" if topic_suffix.ends_with("/_action/cancel_goalRequest") => node
                        .update_action_srv_cancel_req_reader(
                            &topic_suffix[..topic_suffix.len() - 27],
                            &entity.type_name,
                            rgid,
                        ),
                    "rq/" if topic_suffix.ends_with("/_action/get_resultRequest") => node
                        .update_action_srv_result_req_reader(
                            &topic_suffix[..topic_suffix.len() - 26],
                            &entity.type_name,
                            rgid,
                        ),
                    "rq/" if topic_suffix.ends_with("Request") => node
                        .update_service_srv_req_reader(
                            &topic_suffix[..topic_suffix.len() - 7],
                            &entity.type_name,
                            rgid,
                        ),
                    "rr/" if topic_suffix.ends_with("/_action/send_goalReply") => node
                        .update_action_cli_send_rep_reader(
                            &topic_suffix[..topic_suffix.len() - 23],
                            &entity.type_name,
                            rgid,
                        ),
                    "rr/" if topic_suffix.ends_with("/_action/cancel_goalReply") => node
                        .update_action_cli_cancel_rep_reader(
                            &topic_suffix[..topic_suffix.len() - 25],
                            &entity.type_name,
                            rgid,
                        ),
                    "rr/" if topic_suffix.ends_with("/_action/get_resultReply") => node
                        .update_action_cli_result_rep_reader(
                            &topic_suffix[..topic_suffix.len() - 24],
                            &entity.type_name,
                            rgid,
                        ),
                    "rr/" if topic_suffix.ends_with("Reply") => node.update_service_cli_rep_reader(
                        &topic_suffix[..topic_suffix.len() - 5],
                        &entity.type_name,
                        rgid,
                    ),
                    _ => {
                        log::error!("{ros_node_info} NON-ROS2 Reader: {}", entity.topic_name);
                        None
                    }
                };
                if let Some(e) = event {
                    log::warn!("{ros_node_info} declares {e:?}");
                }
            } else {
                log::warn!("{ros_node_info} declares an undiscovered Reader: {rgid}");
                node.undiscovered_reader.push(*rgid);
            }
        }

        for wgid in &ros_node_info.writer_gid_seq {
            if let Some(entity) = writers.get(wgid) {
                log::info!("{ros_node_info} declares Writer on {}", entity.topic_name);
                let (topic_prefix, topic_suffix) = entity.topic_name.split_at(3);
                let event = match topic_prefix {
                    "rt/" if topic_suffix.ends_with("/_action/status") => node
                        .update_action_srv_status_writer(
                            &topic_suffix[..topic_suffix.len() - 15],
                            &entity.type_name,
                            wgid,
                        ),
                    "rt/" if topic_suffix.ends_with("/_action/feedback") => node
                        .update_action_srv_feedback_writer(
                            &topic_suffix[..topic_suffix.len() - 17],
                            &entity.type_name,
                            wgid,
                        ),
                    "rt/" => node.update_topic_pub(topic_suffix, &entity.type_name, wgid),
                    "rq/" if topic_suffix.ends_with("/_action/send_goalRequest") => node
                        .update_action_cli_send_req_writer(
                            &topic_suffix[..topic_suffix.len() - 25],
                            &entity.type_name,
                            wgid,
                        ),
                    "rq/" if topic_suffix.ends_with("/_action/cancel_goalRequest") => node
                        .update_action_cli_cancel_req_writer(
                            &topic_suffix[..topic_suffix.len() - 27],
                            &entity.type_name,
                            wgid,
                        ),
                    "rq/" if topic_suffix.ends_with("/_action/get_resultRequest") => node
                        .update_action_cli_result_req_writer(
                            &topic_suffix[..topic_suffix.len() - 26],
                            &entity.type_name,
                            wgid,
                        ),
                    "rq/" if topic_suffix.ends_with("Request") => node
                        .update_service_cli_req_writer(
                            &topic_suffix[..topic_suffix.len() - 7],
                            &entity.type_name,
                            wgid,
                        ),
                    "rr/" if topic_suffix.ends_with("/_action/send_goalReply") => node
                        .update_action_srv_send_rep_writer(
                            &topic_suffix[..topic_suffix.len() - 23],
                            &entity.type_name,
                            wgid,
                        ),
                    "rr/" if topic_suffix.ends_with("/_action/cancel_goalReply") => node
                        .update_action_srv_cancel_rep_writer(
                            &topic_suffix[..topic_suffix.len() - 25],
                            &entity.type_name,
                            wgid,
                        ),
                    "rr/" if topic_suffix.ends_with("/_action/get_resultReply") => node
                        .update_action_srv_result_rep_writer(
                            &topic_suffix[..topic_suffix.len() - 24],
                            &entity.type_name,
                            wgid,
                        ),
                    "rr/" if topic_suffix.ends_with("Reply") => node.update_service_srv_rep_writer(
                        &topic_suffix[..topic_suffix.len() - 5],
                        &entity.type_name,
                        wgid,
                    ),
                    _ => {
                        log::error!("{ros_node_info} NON-ROS2 Reader: {}", entity.topic_name);
                        None
                    }
                };
                if let Some(e) = event {
                    log::warn!("{ros_node_info} declares {e:?}");
                }
            } else {
                log::warn!("{ros_node_info} declares an undiscovered Writer: {wgid}");
                node.undiscovered_writer.push(*wgid);
            }
        }
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
    UndiscoveredTopicPub(String),
    DiscoveredTopicSub(TopicSub),
    UndiscoveredTopicSub(String),
    DiscoveredServiceSrv(ServiceSrv),
    UndiscoveredServiceSrv(String),
    DiscoveredServiceCli(ServiceCli),
    UndiscoveredServiceCli(String),
    DiscoveredActionSrv(ActionSrv),
    UndiscoveredActionSrv(String),
    DiscoveredActionCli(ActionCli),
    UndiscoveredActionCli(String),
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
