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
}

impl DiscoveredEntities {
    #[inline]
    pub fn add_participant(&mut self, participant: DdsParticipant) {
        self.participants.insert(participant.key, participant);
    }

    #[inline]
    pub fn remove_participant(&mut self, gid: &Gid) {
        self.participants.remove(gid);
    }

    #[inline]
    pub fn add_writer(&mut self, writer: DdsEntity) {
        self.writers.insert(writer.key, writer);
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) {
        self.writers.remove(gid);
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) {
        self.readers.insert(reader.key, reader);
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) {
        self.readers.remove(gid);
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
