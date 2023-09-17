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
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use crate::dds_discovery::DdsEntity;
use crate::discovered_entities::ROS2DiscoveryEvent;
use crate::gid::Gid;

#[derive(Clone, Debug, Serialize)]
pub struct TopicPub {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub writer: Gid,
}

impl TopicPub {
    pub fn create(name: String, typ: String, writer: Gid) -> TopicPub {
        TopicPub { name, typ, writer }
    }
}

impl std::fmt::Display for TopicPub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Publisher {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TopicSub {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub reader: Gid,
}

impl TopicSub {
    pub fn create(name: String, typ: String, reader: Gid) -> TopicSub {
        TopicSub { name, typ, reader }
    }
}

impl std::fmt::Display for TopicSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscriber {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ServiceSrvEntities {
    pub req_reader: Gid,
    pub rep_writer: Gid,
}

impl ServiceSrvEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.req_reader != Gid::NOT_DISCOVERED && self.rep_writer != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ServiceSrvEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{reqR:{:?}, repW:{:?}}}",
            self.req_reader, self.rep_writer
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceSrv {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ServiceSrvEntities,
}

impl ServiceSrv {
    pub fn create(name: String, typ: String) -> ServiceSrv {
        ServiceSrv {
            name,
            typ,
            entities: ServiceSrvEntities::default(),
        }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ServiceSrv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Service Server {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ServiceCliEntities {
    pub req_writer: Gid,
    pub rep_reader: Gid,
}

impl ServiceCliEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.rep_reader != Gid::NOT_DISCOVERED && self.req_writer != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ServiceCliEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{reqW:{:?}, repR:{:?}}}",
            self.req_writer, self.rep_reader
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceCli {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ServiceCliEntities,
}

impl ServiceCli {
    pub fn create(name: String, typ: String) -> ServiceCli {
        ServiceCli {
            name,
            typ,
            entities: ServiceCliEntities::default(),
        }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ServiceCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Service Client {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ActionSrvEntities {
    pub send_goal: ServiceSrvEntities,
    pub cancel_goal: ServiceSrvEntities,
    pub get_result: ServiceSrvEntities,
    pub status_writer: Gid,
    pub feedback_writer: Gid,
}

impl ActionSrvEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.send_goal.is_complete()
            && self.cancel_goal.is_complete()
            && self.get_result.is_complete()
            && self.status_writer != Gid::NOT_DISCOVERED
            && self.feedback_writer != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ActionSrvEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{send_goal{:?}, cancel_goal{:?}, get_result{:?}, statusW:{:?}, feedbackW:{:?}}}",
            self.send_goal,
            self.cancel_goal,
            self.get_result,
            self.status_writer,
            self.feedback_writer,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ActionSrv {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ActionSrvEntities,
}

impl ActionSrv {
    pub fn create(name: String, typ: String) -> ActionSrv {
        ActionSrv {
            name,
            typ,
            entities: ActionSrvEntities::default(),
        }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ActionSrv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Action Server {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ActionCliEntities {
    pub send_goal: ServiceCliEntities,
    pub cancel_goal: ServiceCliEntities,
    pub get_result: ServiceCliEntities,
    pub status_reader: Gid,
    pub feedback_reader: Gid,
}

impl ActionCliEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.send_goal.is_complete()
            && self.cancel_goal.is_complete()
            && self.get_result.is_complete()
            && self.status_reader != Gid::NOT_DISCOVERED
            && self.feedback_reader != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ActionCliEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{send_goal{:?}, cancel_goal{:?}, get_result{:?}, statusR:{:?}, feedbackR:{:?}}}",
            self.send_goal,
            self.cancel_goal,
            self.get_result,
            self.status_reader,
            self.feedback_reader,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ActionCli {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ActionCliEntities,
}

impl ActionCli {
    pub fn create(name: String, typ: String) -> ActionCli {
        ActionCli {
            name,
            typ,
            entities: ActionCliEntities::default(),
        }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ActionCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Action Client {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct NodeInfo {
    pub fullname: String,
    #[serde(skip)]
    node_name_idx: usize,
    #[serde(skip)]
    pub participant: Gid,
    #[serde(rename = "publishers", serialize_with = "serialize_hashmap_values")]
    pub topic_pub: HashMap<String, TopicPub>,
    #[serde(rename = "subscribers", serialize_with = "serialize_hashmap_values")]
    pub topic_sub: HashMap<String, TopicSub>,
    #[serde(
        rename = "service_servers",
        serialize_with = "serialize_hashmap_values"
    )]
    pub service_srv: HashMap<String, ServiceSrv>,
    #[serde(
        rename = "service_clients",
        serialize_with = "serialize_hashmap_values"
    )]
    pub service_cli: HashMap<String, ServiceCli>,
    #[serde(rename = "action_servers", serialize_with = "serialize_hashmap_values")]
    pub action_srv: HashMap<String, ActionSrv>,
    #[serde(rename = "action_clients", serialize_with = "serialize_hashmap_values")]
    pub action_cli: HashMap<String, ActionCli>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub undiscovered_reader: Vec<Gid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub undiscovered_writer: Vec<Gid>,
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}  (namespace={}, name={})",
            self.fullname,
            self.namespace(),
            self.name()
        )
    }
}

impl NodeInfo {
    pub fn create(namespace: String, node_name: String, participant: Gid) -> NodeInfo {
        // if
        let (fullname, node_name_idx) = if namespace == "/" {
            (format!("/{node_name}"), 1)
        } else {
            (format!("{namespace}/{node_name}"), namespace.len() + 1)
        };

        NodeInfo {
            fullname,
            node_name_idx,
            participant,
            topic_pub: HashMap::new(),
            topic_sub: HashMap::new(),
            service_srv: HashMap::new(),
            service_cli: HashMap::new(),
            action_srv: HashMap::new(),
            action_cli: HashMap::new(),
            undiscovered_reader: Vec::new(),
            undiscovered_writer: Vec::new(),
        }
    }

    pub fn namespace(&self) -> &str {
        if self.node_name_idx == 1 {
            // namespace is only "/"
            "/"
        } else {
            // don't include last "/" separator in namespace
            &self.fullname[..self.node_name_idx - 1]
        }
    }

    pub fn name(&self) -> &str {
        &self.fullname[self.node_name_idx..]
    }

    pub fn update_with_reader(&mut self, entity: &DdsEntity) -> Option<ROS2DiscoveryEvent> {
        let topic_prefix = &entity.topic_name[..3];
        let topic_suffix = &entity.topic_name[2..];
        match topic_prefix {
            "rt/" if topic_suffix.ends_with("/_action/status") => self
                .update_action_cli_status_reader(
                    &topic_suffix[..topic_suffix.len() - 15],
                    &entity.key,
                ),
            "rt/" if topic_suffix.ends_with("/_action/feedback") => self
                .update_action_cli_feedback_reader(
                    &topic_suffix[..topic_suffix.len() - 17],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rt/" => self.update_topic_sub(
                topic_suffix,
                dds_pubsub_topic_to_ros(&entity.type_name),
                &entity.key,
            ),
            "rq/" if topic_suffix.ends_with("/_action/send_goalRequest") => self
                .update_action_srv_send_req_reader(
                    &topic_suffix[..topic_suffix.len() - 25],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/cancel_goalRequest") => self
                .update_action_srv_cancel_req_reader(
                    &topic_suffix[..topic_suffix.len() - 27],
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/get_resultRequest") => self
                .update_action_srv_result_req_reader(
                    &topic_suffix[..topic_suffix.len() - 26],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("Request") => self.update_service_srv_req_reader(
                &topic_suffix[..topic_suffix.len() - 7],
                dds_service_topic_to_ros(&entity.type_name),
                &entity.key,
            ),
            "rr/" if topic_suffix.ends_with("/_action/send_goalReply") => self
                .update_action_cli_send_rep_reader(
                    &topic_suffix[..topic_suffix.len() - 23],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/cancel_goalReply") => self
                .update_action_cli_cancel_rep_reader(
                    &topic_suffix[..topic_suffix.len() - 25],
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/get_resultReply") => self
                .update_action_cli_result_rep_reader(
                    &topic_suffix[..topic_suffix.len() - 24],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("Reply") => self.update_service_cli_rep_reader(
                &topic_suffix[..topic_suffix.len() - 5],
                dds_service_topic_to_ros(&entity.type_name),
                &entity.key,
            ),
            _ => {
                log::warn!(
                    r#"ROS2 Node {self} uses unexpected DDS topic "{}" - ignored"#,
                    entity.topic_name
                );
                None
            }
        }
    }

    pub fn update_with_writer(&mut self, entity: &DdsEntity) -> Option<ROS2DiscoveryEvent> {
        let topic_prefix = &entity.topic_name[..3];
        let topic_suffix = &entity.topic_name[2..];
        match topic_prefix {
            "rt/" if topic_suffix.ends_with("/_action/status") => self
                .update_action_srv_status_writer(
                    &topic_suffix[..topic_suffix.len() - 15],
                    &entity.key,
                ),
            "rt/" if topic_suffix.ends_with("/_action/feedback") => self
                .update_action_srv_feedback_writer(
                    &topic_suffix[..topic_suffix.len() - 17],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rt/" => self.update_topic_pub(
                topic_suffix,
                dds_pubsub_topic_to_ros(&entity.type_name),
                &entity.key,
            ),
            "rq/" if topic_suffix.ends_with("/_action/send_goalRequest") => self
                .update_action_cli_send_req_writer(
                    &topic_suffix[..topic_suffix.len() - 25],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/cancel_goalRequest") => self
                .update_action_cli_cancel_req_writer(
                    &topic_suffix[..topic_suffix.len() - 27],
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/get_resultRequest") => self
                .update_action_cli_result_req_writer(
                    &topic_suffix[..topic_suffix.len() - 26],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("Request") => self.update_service_cli_req_writer(
                &topic_suffix[..topic_suffix.len() - 7],
                dds_service_topic_to_ros(&entity.type_name),
                &entity.key,
            ),
            "rr/" if topic_suffix.ends_with("/_action/send_goalReply") => self
                .update_action_srv_send_rep_writer(
                    &topic_suffix[..topic_suffix.len() - 23],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/cancel_goalReply") => self
                .update_action_srv_cancel_rep_writer(
                    &topic_suffix[..topic_suffix.len() - 25],
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/get_resultReply") => self
                .update_action_srv_result_rep_writer(
                    &topic_suffix[..topic_suffix.len() - 24],
                    dds_action_topic_to_ros(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("Reply") => self.update_service_srv_rep_writer(
                &topic_suffix[..topic_suffix.len() - 5],
                dds_service_topic_to_ros(&entity.type_name),
                &entity.key,
            ),
            _ => {
                log::warn!(
                    r#"ROS2 Node {self} uses unexpected DDS topic "{}" - ignored"#,
                    entity.topic_name
                );
                None
            }
        }
    }

    // Update TopicPub, returing a ROS2DiscoveryEvent::DiscoveredTopicSub if new or changed
    fn update_topic_pub(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredTopicPub;
        match self.topic_pub.entry(name.into()) {
            Entry::Vacant(e) => {
                let tpub = e.insert(TopicPub {
                    name: name.into(),
                    typ: typ,
                    writer: *writer,
                });
                Some(DiscoveredTopicPub(self.fullname.clone(), tpub.clone()))
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Publisher "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    result = Some(DiscoveredTopicPub(self.fullname.clone(), v.clone()));
                }
                if v.writer != *writer {
                    log::debug!(
                        r#"ROS declaration of Publisher "{v}" changed it's DDS Writer's GID from {} to {writer}"#,
                        v.writer
                    );
                    v.writer = *writer;
                    result = Some(DiscoveredTopicPub(self.fullname.clone(), v.clone()));
                }
                result
            }
        }
    }

    // Update TopicSub, returing a ROS2DiscoveryEvent::DiscoveredTopicSub if new or changed
    fn update_topic_sub(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredTopicSub;
        match self.topic_sub.entry(name.into()) {
            Entry::Vacant(e) => {
                let tsub = e.insert(TopicSub {
                    name: name.into(),
                    typ: typ,
                    reader: *reader,
                });
                Some(DiscoveredTopicSub(self.fullname.clone(), tsub.clone()))
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Subscriber "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    result = Some(DiscoveredTopicSub(self.fullname.clone(), v.clone()));
                }
                if v.reader != *reader {
                    log::debug!(
                        r#"ROS declaration of Subscriber "{v}" changed it's DDS Writer's GID from {} to {reader}"#,
                        v.reader
                    );
                    v.reader = *reader;
                    result = Some(DiscoveredTopicSub(self.fullname.clone(), v.clone()));
                }
                result
            }
        }
    }

    // Update ServiceSrv, returing a ROS2DiscoveryEvent::DiscoveredServiceSrv if new and complete or changed
    fn update_service_srv_req_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceSrv;
        match self.service_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ServiceSrv::create(name.into(), typ));
                v.entities.req_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Service Server "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.req_reader != *reader {
                    if v.entities.req_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Service Server "{v}" changed it's Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.req_reader
                        );
                    }
                    v.entities.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceSrv, returing a ROS2DiscoveryEvent::DiscoveredServiceSrv if new and complete or changed
    fn update_service_srv_rep_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceSrv;
        match self.service_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ServiceSrv::create(name.into(), typ));
                v.entities.rep_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Service Server "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.rep_writer != *writer {
                    if v.entities.rep_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Service Server "{v}" changed it's Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.rep_writer
                        );
                    }
                    v.entities.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceCli, returing a ROS2DiscoveryEvent::DiscoveredServiceCli if new and complete or changed
    fn update_service_cli_rep_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceCli;
        match self.service_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ServiceCli::create(name.into(), typ));
                v.entities.rep_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Service Client "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.rep_reader != *reader {
                    if v.entities.rep_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Service Client "{v}" changed it's Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.rep_reader
                        );
                    }
                    v.entities.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceCli, returing a ROS2DiscoveryEvent::DiscoveredServiceCli if new and complete or changed
    fn update_service_cli_req_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceCli;
        match self.service_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ServiceCli::create(name.into(), typ));
                v.entities.req_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Service Server "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.req_writer != *writer {
                    if v.entities.req_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Service Server "{v}" changed it's Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.req_writer
                        );
                    }
                    v.entities.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_send_req_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), typ));
                v.entities.send_goal.req_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.req_reader != *reader {
                    if v.entities.send_goal.req_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's send_goal Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.send_goal.req_reader
                        );
                    }
                    v.entities.send_goal.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_send_rep_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), typ));
                v.entities.send_goal.rep_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.rep_writer != *writer {
                    if v.entities.send_goal.rep_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's send_goal Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.send_goal.rep_writer
                        );
                    }
                    v.entities.send_goal.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_srv_cancel_req_reader(
        &mut self,
        name: &str,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), String::new()));
                v.entities.cancel_goal.req_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.req_reader != *reader {
                    if v.entities.cancel_goal.req_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's cancel_goal Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.cancel_goal.req_reader
                        );
                    }
                    v.entities.cancel_goal.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_srv_cancel_rep_writer(
        &mut self,
        name: &str,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), String::new()));
                v.entities.cancel_goal.rep_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.rep_writer != *writer {
                    if v.entities.cancel_goal.rep_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's cancel_goal Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.cancel_goal.rep_writer
                        );
                    }
                    v.entities.cancel_goal.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_result_req_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), typ));
                v.entities.get_result.req_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.req_reader != *reader {
                    if v.entities.get_result.req_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's get_result Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.get_result.req_reader
                        );
                    }
                    v.entities.get_result.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_result_rep_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), typ));
                v.entities.get_result.rep_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.rep_writer != *writer {
                    if v.entities.get_result.rep_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's get_result Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.get_result.rep_writer
                        );
                    }
                    v.entities.get_result.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of Status topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_srv_status_writer(
        &mut self,
        name: &str,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), String::new()));
                v.entities.status_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.status_writer != *writer {
                    if v.entities.status_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's status DDS Writer's GID from {} to {writer}"#,
                            v.entities.status_writer
                        );
                    }
                    v.entities.status_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_feedback_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionSrv::create(name.into(), typ));
                v.entities.feedback_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.feedback_writer != *writer {
                    if v.entities.feedback_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's status DDS Writer's GID from {} to {writer}"#,
                            v.entities.feedback_writer
                        );
                    }
                    v.entities.feedback_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_send_rep_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), typ));
                v.entities.send_goal.rep_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.rep_reader != *reader {
                    if v.entities.send_goal.rep_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's send_goal Reply DDS Reader's GID from {} to {reader}"#,
                            v.entities.send_goal.rep_reader
                        );
                    }
                    v.entities.send_goal.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_send_req_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), typ));
                v.entities.send_goal.req_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.req_writer != *writer {
                    if v.entities.send_goal.req_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's send_goal Request DDS Writer's GID from {} to {writer}"#,
                            v.entities.send_goal.req_writer
                        );
                    }
                    v.entities.send_goal.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_cli_cancel_rep_reader(
        &mut self,
        name: &str,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), String::new()));
                v.entities.cancel_goal.rep_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.rep_reader != *reader {
                    if v.entities.cancel_goal.rep_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's cancel_goal Reply DDS Reader's GID from {} to {reader}"#,
                            v.entities.cancel_goal.rep_reader
                        );
                    }
                    v.entities.cancel_goal.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_cli_cancel_req_writer(
        &mut self,
        name: &str,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), String::new()));
                v.entities.cancel_goal.req_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.req_writer != *writer {
                    if v.entities.cancel_goal.req_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's cancel_goal Request DDS Writer's GID from {} to {writer}"#,
                            v.entities.cancel_goal.req_writer
                        );
                    }
                    v.entities.cancel_goal.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_result_rep_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), typ));
                v.entities.get_result.rep_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.rep_reader != *reader {
                    if v.entities.get_result.rep_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's get_result Reply DDS Reader's GID from {} to {reader}"#,
                            v.entities.get_result.rep_reader
                        );
                    }
                    v.entities.get_result.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_result_req_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), typ));
                v.entities.get_result.req_writer = *writer;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.req_writer != *writer {
                    if v.entities.get_result.req_writer != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's get_result Request DDS Writer's GID from {} to {writer}"#,
                            v.entities.get_result.req_writer
                        );
                    }
                    v.entities.get_result.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of Status topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_cli_status_reader(
        &mut self,
        name: &str,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), String::new()));
                v.entities.status_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.status_reader != *reader {
                    if v.entities.status_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's status DDS Reader's GID from {} to {reader}"#,
                            v.entities.status_reader
                        );
                    }
                    v.entities.status_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_feedback_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                let v = e.insert(ActionCli::create(name.into(), typ));
                v.entities.feedback_reader = *reader;
                None
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        log::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                if v.entities.feedback_reader != *reader {
                    if v.entities.feedback_reader != Gid::NOT_DISCOVERED {
                        log::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's status DDS Reader's GID from {} to {reader}"#,
                            v.entities.feedback_reader
                        );
                    }
                    v.entities.feedback_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(self.fullname.clone(), v.clone()))
                    };
                }
                result
            }
        }
    }

    //
    pub fn remove_all_entities(&mut self) -> Vec<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::*;
        let mut events = Vec::new();

        for (_, v) in self.topic_pub.drain() {
            events.push(UndiscoveredTopicPub(self.fullname.clone(), v))
        }
        for (_, v) in self.topic_sub.drain() {
            events.push(UndiscoveredTopicSub(self.fullname.clone(), v))
        }
        for (_, v) in self.service_srv.drain() {
            events.push(UndiscoveredServiceSrv(self.fullname.clone(), v))
        }
        for (_, v) in self.service_cli.drain() {
            events.push(UndiscoveredServiceCli(self.fullname.clone(), v))
        }
        for (_, v) in self.action_srv.drain() {
            events.push(UndiscoveredActionSrv(self.fullname.clone(), v))
        }
        for (_, v) in self.action_cli.drain() {
            events.push(UndiscoveredActionCli(self.fullname.clone(), v))
        }
        self.undiscovered_reader.resize(0, Gid::NOT_DISCOVERED);
        self.undiscovered_writer.resize(0, Gid::NOT_DISCOVERED);

        events
    }

    // Remove a DDS Reader possibly used by this node, and returns an UndiscoveredX event if
    // this Reader was used by some Subscription, Service or Action
    pub fn remove_reader(&mut self, reader: &Gid) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::*;
        if let Some((name, _)) = self.topic_sub.iter().find(|(_, v)| v.reader == *reader) {
            return Some(UndiscoveredTopicSub(
                self.fullname.clone(),
                self.topic_sub.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_srv
            .iter()
            .find(|(_, v)| v.entities.req_reader == *reader)
        {
            return Some(UndiscoveredServiceSrv(
                self.fullname.clone(),
                self.service_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_cli
            .iter()
            .find(|(_, v)| v.entities.rep_reader == *reader)
        {
            return Some(UndiscoveredServiceCli(
                self.fullname.clone(),
                self.service_cli.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_srv.iter().find(|(_, v)| {
            v.entities.send_goal.req_reader == *reader
                || v.entities.cancel_goal.req_reader == *reader
                || v.entities.get_result.req_reader == *reader
        }) {
            return Some(UndiscoveredActionSrv(
                self.fullname.clone(),
                self.action_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_cli.iter().find(|(_, v)| {
            v.entities.send_goal.rep_reader == *reader
                || v.entities.cancel_goal.rep_reader == *reader
                || v.entities.get_result.rep_reader == *reader
                || v.entities.status_reader == *reader
                || v.entities.feedback_reader == *reader
        }) {
            return Some(UndiscoveredActionCli(
                self.fullname.clone(),
                self.action_cli.remove(&name.clone()).unwrap(),
            ));
        }
        self.undiscovered_reader.retain(|gid| gid != reader);
        None
    }

    // Remove a DDS Writer possibly used by this node, and returns an UndiscoveredX event if
    // this Writer was used by some Subscription, Service or Action
    pub fn remove_writer(&mut self, writer: &Gid) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::*;
        if let Some((name, _)) = self.topic_pub.iter().find(|(_, v)| v.writer == *writer) {
            return Some(UndiscoveredTopicPub(
                self.fullname.clone(),
                self.topic_pub.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_srv
            .iter()
            .find(|(_, v)| v.entities.rep_writer == *writer)
        {
            return Some(UndiscoveredServiceSrv(
                self.fullname.clone(),
                self.service_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_cli
            .iter()
            .find(|(_, v)| v.entities.req_writer == *writer)
        {
            return Some(UndiscoveredServiceCli(
                self.fullname.clone(),
                self.service_cli.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_srv.iter().find(|(_, v)| {
            v.entities.send_goal.rep_writer == *writer
                || v.entities.cancel_goal.rep_writer == *writer
                || v.entities.get_result.rep_writer == *writer
                || v.entities.status_writer == *writer
                || v.entities.feedback_writer == *writer
        }) {
            return Some(UndiscoveredActionSrv(
                self.fullname.clone(),
                self.action_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_cli.iter().find(|(_, v)| {
            v.entities.send_goal.req_writer == *writer
                || v.entities.cancel_goal.req_writer == *writer
                || v.entities.get_result.req_writer == *writer
        }) {
            return Some(UndiscoveredActionCli(
                self.fullname.clone(),
                self.action_cli.remove(&name.clone()).unwrap(),
            ));
        }
        self.undiscovered_writer.retain(|gid| gid != writer);
        None
    }
}

fn serialize_hashmap_values<S, T: Serialize>(
    map: &HashMap<String, T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq: <S as Serializer>::SerializeSeq = serializer.serialize_seq(Some(map.len()))?;
    for x in map.values() {
        seq.serialize_element(x)?;
    }
    seq.end()
}
// Convert DDS Topic for pub/sub to ROS2 topic
fn dds_pubsub_topic_to_ros(dds_topic: &str) -> String {
    let result = dds_topic.replace("::dds_::", "::").replace("::", "/");
    if result.ends_with('_') {
        result[..result.len() - 1].into()
    } else {
        result
    }
}

// Convert DDS Topic for ROS2 Service to ROS2 topic
fn dds_service_topic_to_ros(dds_topic: &str) -> String {
    dds_pubsub_topic_to_ros(
        dds_topic
            .strip_suffix("_Request_")
            .or(dds_topic.strip_suffix("_Response_"))
            .unwrap_or(dds_topic),
    )
}

// Convert DDS Topic for ROS2 Action to ROS2 topic
// Warning: can't work for "rt/.../_action/status" topic, since its type is generic
fn dds_action_topic_to_ros(dds_topic: &str) -> String {
    dds_pubsub_topic_to_ros(
        dds_topic
            .strip_suffix("_SendGoal_Request_")
            .or(dds_topic.strip_suffix("_SendGoal_Response_"))
            .or(dds_topic.strip_suffix("_GetResult_Request_"))
            .or(dds_topic.strip_suffix("_GetResult_Response_"))
            .or(dds_topic.strip_suffix("_FeedbackMessage_"))
            .unwrap_or(dds_topic),
    )
}
