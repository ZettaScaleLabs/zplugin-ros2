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
use zenoh::{prelude::*, queryable::Query};
use zenoh::prelude::r#async::AsyncResolve;

use crate::AdminRef;
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
    consolidated_node_info: HashMap<Gid, HashMap<String, NodeInfo>>,
    admin_space: HashMap<OwnedKeyExpr, EntityRef>,
}

#[derive(Debug)]
enum EntityRef {
    Participant(Gid),
    Writer(Gid),
    Reader(Gid),
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
            EntityRef::Participant(participant.key)
        );
        self.participants.insert(participant.key, participant);
    }

    #[inline]
    pub fn remove_participant(&mut self, gid: &Gid) {
        self.participants.remove(gid);
        self.admin_space.remove(
            &zenoh::keformat!(ke_admin_participant::formatter(), pgid = gid).unwrap(),
        );
    }

    #[inline]
    pub fn add_writer(&mut self, writer: DdsEntity) {
        self.admin_space.insert(
            zenoh::keformat!(ke_admin_writer::formatter(),
                pgid = writer.participant_key,
                wgid = writer.key,
                topic = &writer.topic_name,
                ).unwrap(),
            EntityRef::Writer(writer.key)
        );
        self.writers.insert(writer.key, writer);
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) {
        if let Some(writer) = self.writers.remove(gid) {
            self.admin_space.remove(
               &zenoh::keformat!(ke_admin_writer::formatter(),
                    pgid = writer.participant_key,
                    wgid = writer.key,
                    topic = &writer.topic_name,
                    ).unwrap()
            );
        }
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) {
        self.admin_space.insert(
            zenoh::keformat!(ke_admin_reader::formatter(),
                pgid = reader.participant_key,
                wgid = reader.key,
                topic = &reader.topic_name,
                ).unwrap(),
            EntityRef::Reader(reader.key)
        );
        self.readers.insert(reader.key, reader);
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) {
        if let Some(reader) = self.readers.remove(gid) {
            self.admin_space.remove(
               &zenoh::keformat!(ke_admin_reader::formatter(),
                    pgid = reader.participant_key,
                    wgid = reader.key,
                    topic = &reader.topic_name,
                    ).unwrap()
            );
        }
    }

    pub fn update_participant_info(&mut self, info: ParticipantEntitiesInfo) {
        match self.ros_participant_info.insert(info.gid, info) {
            Some(old) => {
                // compare and check changes in each nodes
            }
            None => {
                // check all new nodes
            }
        }
    }

    fn get_entity_json_value(&self, entity_ref: &EntityRef) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match entity_ref {
            EntityRef::Participant(gid) =>
                self.participants.get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Writer(gid) =>
                self.writers.get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Reader(gid) =>
                self.readers.get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
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
                        self.send_admin_reply(query, admin_keyexpr_prefix, ke, entity_ref).await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(entity_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, admin_keyexpr_prefix, sub_ke, entity_ref).await;
                }
            }
        }
    }

    async fn send_admin_reply(&self, query: &Query, admin_keyexpr_prefix: &keyexpr, key_expr: &keyexpr, entity_ref: &EntityRef) {
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
            },
            Ok(None) => log::error!("INTERNAL ERROR: Dangling {:?} for {}", entity_ref, key_expr),
            Err(e) => {
                log::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }


}

#[derive(Debug)]
pub enum ROS2DiscoveryEvent {
    DiscoveredTopicPub { tpub: TopicPub },
    UndiscoveredTopicPub { name: String },
    DiscoveredTopicSub { tsub: TopicSub },
    UndiscoveredTopicSub { name: String },
    DiscoveredServiceSrv { tsub: ServiceSrv },
    UndiscoveredServiceSrv { name: String },
    DiscoveredServiceCli { tsub: ServiceCli },
    UndiscoveredServiceCli { name: String },
    DiscoveredActionSrv { tsub: ActionSrv },
    UndiscoveredActionSrv { name: String },
    DiscoveredActionCli { tsub: ActionCli },
    UndiscoveredActionCli { name: String },
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

