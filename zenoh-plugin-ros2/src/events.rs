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

use std::fmt::Display;

use cyclors::qos::Qos;
use zenoh::prelude::OwnedKeyExpr;

use crate::node_info::*;

/// A (local) discovery event of a ROS2 interface
#[derive(Debug)]
pub enum ROS2DiscoveryEvent {
    DiscoveredMsgPub(String, MsgPub),
    UndiscoveredMsgPub(String, MsgPub),
    DiscoveredMsgSub(String, MsgSub),
    UndiscoveredMsgSub(String, MsgSub),
    DiscoveredServiceSrv(String, ServiceSrv),
    UndiscoveredServiceSrv(String, ServiceSrv),
    DiscoveredServiceCli(String, ServiceCli),
    UndiscoveredServiceCli(String, ServiceCli),
    DiscoveredActionSrv(String, ActionSrv),
    UndiscoveredActionSrv(String, ActionSrv),
    DiscoveredActionCli(String, ActionCli),
    UndiscoveredActionCli(String, ActionCli),
}

impl ROS2DiscoveryEvent {
    pub fn node_name(&self) -> &str {
        use ROS2DiscoveryEvent::*;
        match self {
            DiscoveredMsgPub(node, _)
            | UndiscoveredMsgPub(node, _)
            | DiscoveredMsgSub(node, _)
            | UndiscoveredMsgSub(node, _)
            | DiscoveredServiceSrv(node, _)
            | UndiscoveredServiceSrv(node, _)
            | DiscoveredServiceCli(node, _)
            | UndiscoveredServiceCli(node, _)
            | DiscoveredActionSrv(node, _)
            | UndiscoveredActionSrv(node, _)
            | DiscoveredActionCli(node, _)
            | UndiscoveredActionCli(node, _) => &node,
        }
    }

    pub fn interface_name(&self) -> &str {
        use ROS2DiscoveryEvent::*;
        match self {
            DiscoveredMsgPub(_, iface) | UndiscoveredMsgPub(_, iface) => &iface.name,
            DiscoveredMsgSub(_, iface) | UndiscoveredMsgSub(_, iface) => &iface.name,
            DiscoveredServiceSrv(_, iface) | UndiscoveredServiceSrv(_, iface) => &iface.name,
            DiscoveredServiceCli(_, iface) | UndiscoveredServiceCli(_, iface) => &iface.name,
            DiscoveredActionSrv(_, iface) | UndiscoveredActionSrv(_, iface) => &iface.name,
            DiscoveredActionCli(_, iface) | UndiscoveredActionCli(_, iface) => &iface.name,
        }
    }
}

impl std::fmt::Display for ROS2DiscoveryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ROS2DiscoveryEvent::*;
        match self {
            DiscoveredMsgPub(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredMsgSub(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredServiceSrv(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredServiceCli(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredActionSrv(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredActionCli(node, iface) => write!(f, "Node {node} declares {iface}"),
            UndiscoveredMsgPub(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredMsgSub(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredServiceSrv(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredServiceCli(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredActionSrv(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredActionCli(node, iface) => write!(f, "Node {node} undeclares {iface}"),
        }
    }
}

/// A (remote) announcement/retirement of a ROS2 interface
#[derive(Debug)]
pub enum ROS2AnnouncementEvent {
    AnnouncedMsgPub {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
        ros2_type: String,
        keyless: bool,
        writer_qos: Qos,
    },
    RetiredMsgPub {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
    },
    AnnouncedMsgSub {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
        ros2_type: String,
        keyless: bool,
        reader_qos: Qos,
    },
    RetiredMsgSub {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
    },
    AnnouncedServiceSrv {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredServiceSrv {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
    },
    AnnouncedServiceCli {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredServiceCli {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
    },
    AnnouncedActionSrv {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredActionSrv {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
    },
    AnnouncedActionCli {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredActionCli {
        liveliness_ke: OwnedKeyExpr,
        route_ke: OwnedKeyExpr,
    },
}

impl Display for ROS2AnnouncementEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ROS2AnnouncementEvent::*;
        match self {
            AnnouncedMsgPub{ route_ke, ..} => write!(f, "announces Publisher {route_ke}"),
            AnnouncedMsgSub{ route_ke, ..} => write!(f, "announces Subscriber {route_ke}"),
            AnnouncedServiceSrv{ route_ke, ..} => write!(f, "announces Service Server {route_ke}"),
            AnnouncedServiceCli{ route_ke, ..} => write!(f, "announces Service Client {route_ke}"),
            AnnouncedActionSrv{ route_ke, ..} => write!(f, "announces Action Server {route_ke}"),
            AnnouncedActionCli{ route_ke, ..} => write!(f, "announces Action Client {route_ke}"),
            RetiredMsgPub{ route_ke, ..} => write!(f, "retires Publisher {route_ke}"),
            RetiredMsgSub{ route_ke, ..} => write!(f, "retires Subscriber {route_ke}"),
            RetiredServiceSrv{ route_ke, ..} => write!(f, "retires Service Server {route_ke}"),
            RetiredServiceCli{ route_ke, ..} => write!(f, "retires Service Client {route_ke}"),
            RetiredActionSrv{ route_ke, ..} => write!(f, "retires Action Server {route_ke}"),
            RetiredActionCli{ route_ke, ..} => write!(f, "retires Action Client {route_ke}"),
        }
    }
}