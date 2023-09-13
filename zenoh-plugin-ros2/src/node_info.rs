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

use crate::discovered_entities::ROS2DiscoveryEvent;
use crate::gid::Gid;

#[derive(Clone, Serialize)]
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
        write!(f, "{}: {}", self.name, self.typ,)?;
        Ok(())
    }
}

impl std::fmt::Debug for TopicPub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TopicPub ({} - W:{:?})", self, self.writer,)?;
        Ok(())
    }
}

#[derive(Clone, Serialize)]
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
        write!(f, "{}: {}", self.name, self.typ,)?;
        Ok(())
    }
}

impl std::fmt::Debug for TopicSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TopicSub({} - R:{:?})", self, self.reader,)?;
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
        write!(f, "reqR:{:?}, repW:{:?}", self.req_reader, self.rep_writer,)?;
        Ok(())
    }
}

#[derive(Clone, Serialize)]
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
        write!(f, "{}: {}", self.name, self.typ,)?;
        Ok(())
    }
}

impl std::fmt::Debug for ServiceSrv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ServiceSrv({} - {:?})", self, self.entities,)?;
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
        write!(f, "reqW:{}, repR:{}", self.req_writer, self.rep_reader,)?;
        Ok(())
    }
}

#[derive(Clone, Serialize)]
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
        write!(f, "{}: {}", self.name, self.typ,)?;
        Ok(())
    }
}

impl std::fmt::Debug for ServiceCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ServiceCli({} - {:?})", self, self.entities,)?;
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
            "send_goal({:?}),cancel_goal({:?}), get_result({:?}), statusW:{:?}, feedbackW:{:?}",
            self.send_goal,
            self.cancel_goal,
            self.get_result,
            self.status_writer,
            self.feedback_writer,
        )?;
        Ok(())
    }
}

#[derive(Clone, Serialize)]
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
        write!(f, "{}: {}", self.name, self.typ,)?;
        Ok(())
    }
}

impl std::fmt::Debug for ActionSrv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActionSrv({} - {:?})", self, self.entities,)?;
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
            "send_goal({:?}),cancel_goal({:?}, get_result({:?}, statusR:{:?}, feedbackR:{:?}",
            self.send_goal,
            self.cancel_goal,
            self.get_result,
            self.status_reader,
            self.feedback_reader,
        )?;
        Ok(())
    }
}

#[derive(Clone, Serialize)]
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
        write!(f, "{}: {}", self.name, self.typ,)?;
        Ok(())
    }
}

impl std::fmt::Debug for ActionCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActionCli({} - {:?})", self, self.entities,)?;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct NodeInfo {
    pub namespace: String,
    pub name: String,
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
    pub undiscovered_reader: Vec<Gid>,
    pub undiscovered_writer: Vec<Gid>,
}

impl NodeInfo {
    pub fn create(namespace: String, name: String, participant: Gid) -> NodeInfo {
        NodeInfo {
            namespace,
            name,
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

    // Update TopicPub, returing a ROS2DiscoveryEvent::DiscoveredTopicSub if new or changed
    pub fn update_topic_pub(
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
                Some(DiscoveredTopicPub(tpub.clone()))
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Publisher "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    result = Some(DiscoveredTopicPub(v.clone()));
                }
                if v.writer != *writer {
                    log::debug!(
                        r#"ROS declaration of Publisher "{v}" changed it's DDS Writer's GID from {} to {writer}"#,
                        v.writer
                    );
                    v.writer = *writer;
                    result = Some(DiscoveredTopicPub(v.clone()));
                }
                result
            }
        }
    }

    // Update TopicSub, returing a ROS2DiscoveryEvent::DiscoveredTopicSub if new or changed
    pub fn update_topic_sub(
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
                Some(DiscoveredTopicSub(tsub.clone()))
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    log::warn!(
                        r#"ROS declaration of Subscriber "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    result = Some(DiscoveredTopicSub(v.clone()));
                }
                if v.reader != *reader {
                    log::debug!(
                        r#"ROS declaration of Subscriber "{v}" changed it's DDS Writer's GID from {} to {reader}"#,
                        v.reader
                    );
                    v.reader = *reader;
                    result = Some(DiscoveredTopicSub(v.clone()));
                }
                result
            }
        }
    }

    // Update ServiceSrv, returing a ROS2DiscoveryEvent::DiscoveredServiceSrv if new and complete or changed
    pub fn update_service_srv_req_reader(
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
                        result = Some(DiscoveredServiceSrv(v.clone()))
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
                        result = Some(DiscoveredServiceSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceSrv, returing a ROS2DiscoveryEvent::DiscoveredServiceSrv if new and complete or changed
    pub fn update_service_srv_rep_writer(
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
                        result = Some(DiscoveredServiceSrv(v.clone()))
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
                        result = Some(DiscoveredServiceSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceCli, returing a ROS2DiscoveryEvent::DiscoveredServiceCli if new and complete or changed
    pub fn update_service_cli_rep_reader(
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
                        result = Some(DiscoveredServiceCli(v.clone()))
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
                        result = Some(DiscoveredServiceCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceCli, returing a ROS2DiscoveryEvent::DiscoveredServiceCli if new and complete or changed
    pub fn update_service_cli_req_writer(
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
                        result = Some(DiscoveredServiceCli(v.clone()))
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
                        result = Some(DiscoveredServiceCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    pub fn update_action_srv_send_req_reader(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    pub fn update_action_srv_send_rep_writer(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    pub fn update_action_srv_cancel_req_reader(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    pub fn update_action_srv_cancel_rep_writer(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    pub fn update_action_srv_result_req_reader(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    pub fn update_action_srv_result_rep_writer(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of Status topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    pub fn update_action_srv_status_writer(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    pub fn update_action_srv_feedback_writer(
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
                        result = Some(DiscoveredActionSrv(v.clone()))
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
                        result = Some(DiscoveredActionSrv(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    pub fn update_action_cli_send_rep_reader(
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
                        result = Some(DiscoveredActionCli(v.clone()))
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    pub fn update_action_cli_send_req_writer(
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
                        result = Some(DiscoveredActionCli(v.clone()))
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    pub fn update_action_cli_cancel_rep_reader(
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    pub fn update_action_cli_cancel_req_writer(
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    pub fn update_action_cli_result_rep_reader(
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
                        result = Some(DiscoveredActionCli(v.clone()))
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    pub fn update_action_cli_result_req_writer(
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
                        result = Some(DiscoveredActionCli(v.clone()))
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of Status topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    pub fn update_action_cli_status_reader(
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    pub fn update_action_cli_feedback_reader(
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
                        result = Some(DiscoveredActionCli(v.clone()))
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
                        result = Some(DiscoveredActionCli(v.clone()))
                    };
                }
                result
            }
        }
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
