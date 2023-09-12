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
use std::collections::HashMap;

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
