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

use crate::gid::Gid;

#[derive(Clone)]
pub struct TopicPub {
    pub name: String,
    pub typ: String,
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

#[derive(Clone)]
pub struct TopicSub {
    pub name: String,
    pub typ: String,
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

#[derive(Clone)]
pub struct ServiceSrv {
    pub name: String,
    pub typ: String,
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

#[derive(Clone)]
pub struct ServiceCli {
    pub name: String,
    pub typ: String,
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

#[derive(Clone)]
pub struct ActionSrv {
    pub name: String,
    pub typ: String,
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

#[derive(Clone)]
pub struct ActionCli {
    pub name: String,
    pub typ: String,
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

pub struct NodeInfo {
    pub namespace: Option<String>,
    pub name: String,
    pub participant: Gid,
    pub topic_pub: HashMap<String, TopicPub>,
    pub topic_sub: HashMap<String, TopicSub>,
    pub service_srv: HashMap<String, ServiceSrv>,
    pub service_cli: HashMap<String, ServiceCli>,
    pub action_srv: HashMap<String, ActionSrv>,
    pub action_cli: HashMap<String, ActionCli>,
}
