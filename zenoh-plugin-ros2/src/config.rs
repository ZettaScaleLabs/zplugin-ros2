use regex::Regex;
use serde::de::Visitor;
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
use serde::{de, Deserialize, Deserializer};
use std::env;
use std::fmt;
use std::time::Duration;
use zenoh::prelude::*;

pub const DEFAULT_NODENAME: &str = "zenoh-bridge-ros2";
pub const DEFAULT_DOMAIN: u32 = 0;
pub const DEFAULT_RELIABLE_ROUTES_BLOCKING: bool = true;
pub const DEFAULT_QUERIES_TIMEOUT: f32 = 5.0;
pub const DEFAULT_DDS_LOCALHOST_ONLY: bool = false;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub id: Option<OwnedKeyExpr>,
    #[serde(default)]
    pub namespace: Option<OwnedKeyExpr>,
    #[serde(default = "default_nodename")]
    pub nodename: OwnedKeyExpr,
    #[serde(default = "default_domain")]
    pub domain: u32,
    #[serde(default = "default_localhost_only")]
    pub ros_localhost_only: bool,
    #[serde(default, flatten)]
    pub allowance: Option<Allowance>,
    #[serde(default)]
    #[cfg(feature = "dds_shm")]
    pub shm_enabled: bool,
    #[serde(
        default = "default_queries_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub queries_timeout: Duration,
    #[serde(default = "default_reliable_routes_blocking")]
    pub reliable_routes_blocking: bool,
    #[serde(default)]
    __required__: bool,
    #[serde(default, deserialize_with = "deserialize_paths")]
    __path__: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub enum Allowance {
    #[serde(rename = "allow")]
    Allow(ROS2InterfacesRegex),
    #[serde(rename = "deny")]
    Deny(ROS2InterfacesRegex),
}

impl Allowance {
    pub fn is_publisher_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .publishers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .publishers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_subscriber_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .subscribers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .subscribers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_service_srv_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .service_servers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .service_servers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_service_cli_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .service_clients
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .service_clients
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_action_srv_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .action_servers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .action_servers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_action_cli_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .action_clients
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .action_clients
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }
}

#[derive(Deserialize, Debug, Default)]
pub struct ROS2InterfacesRegex {
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub publishers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub subscribers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub service_servers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub service_clients: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_servers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_clients: Option<Regex>,
}

fn default_nodename() -> OwnedKeyExpr {
    unsafe { OwnedKeyExpr::from_string_unchecked(DEFAULT_NODENAME.into()) }
}

fn default_domain() -> u32 {
    if let Ok(s) = env::var("ROS_DOMAIN_ID") {
        s.parse::<u32>().unwrap_or(DEFAULT_DOMAIN)
    } else {
        DEFAULT_DOMAIN
    }
}

fn deserialize_paths<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = Vec<String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a string or vector of strings")
        }
        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![v.into()])
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let mut v = if let Some(l) = seq.size_hint() {
                Vec::with_capacity(l)
            } else {
                Vec::new()
            };
            while let Some(s) = seq.next_element()? {
                v.push(s);
            }
            Ok(v)
        }
    }
    deserializer.deserialize_any(V)
}

fn default_reliable_routes_blocking() -> bool {
    DEFAULT_RELIABLE_ROUTES_BLOCKING
}

fn default_localhost_only() -> bool {
    env::var("ROS_LOCALHOST_ONLY").as_deref() == Ok("1")
}

fn default_queries_timeout() -> Duration {
    Duration::from_secs_f32(DEFAULT_QUERIES_TIMEOUT)
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let seconds: f32 = Deserialize::deserialize(deserializer)?;
    Ok(Duration::from_secs_f32(seconds))
}

fn deserialize_regex<'de, D>(deserializer: D) -> Result<Option<Regex>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(RegexVisitor)
}

// Serde Visitor for Regex deserialization.
// It accepts either a String, either a list of Strings (that are concatenated with `|`)
struct RegexVisitor;

impl<'de> Visitor<'de> for RegexVisitor {
    type Value = Option<Regex>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(r#"either a string or a list of strings"#)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Regex::new(&format!("^{value}$"))
            .map(Some)
            .map_err(|e| de::Error::custom(format!("Invalid regex '{value}': {e}")))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut vec: Vec<String> = Vec::new();
        while let Some(s) = seq.next_element::<String>()? {
            vec.push(format!("^{s}$"));
        }
        let s: String = vec.join("|");
        Regex::new(&s)
            .map(Some)
            .map_err(|e| de::Error::custom(format!("Invalid regex '{s}': {e}")))
    }
}

mod tests {
    use super::*;
    use serde::{de, Deserialize, Deserializer, Serialize};
    use std::ops::Deref;
    use std::str::FromStr;

    #[derive(Deserialize, Debug, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct Config1 {
        pub domain: u32,
        #[serde(flatten)]
        pub allowance: Option<Allowance1>,
    }

    #[derive(Deserialize, Debug, Serialize)]
    pub enum Allowance1 {
        #[serde(rename = "allow")]
        Allow(MyRegex),
        #[serde(rename = "deny")]
        Deny(MyRegex),
    }

    #[derive(Deserialize, Debug, Serialize)]
    pub struct MyRegex {
        pub pubs: String,
        pub subs: String,
    }

    #[test]
    fn test_serde() {
        let conf: Config1 = Config1 {
            domain: 1,
            allowance: Some(Allowance1::Allow(MyRegex {
                pubs: "P".into(),
                subs: "S".into(),
            })),
        };

        println!("conf: {conf:?}");

        println!("json: {}", serde_json::to_string(&conf).unwrap());

        let x: Config1 =
            serde_json::from_str(r#"{"domain":1,"allow":{"pubs":"P","subs":"S"}}"#).unwrap();
        println!("conf: {x:?}");

        let x: Config1 =
            serde_json::from_str(r#"{"domain":1,"deny":{"pubs":"P","subs":"S"}}"#).unwrap();
        println!("conf: {x:?}");

        let x: Config1 = serde_json::from_str(
            r#"{"domain":1,"allow":{"pubs":"P","subs":"S"},"deny":{"pubs":"P","subs":"S"}}"#,
        )
        .unwrap();
        println!("conf: {x:?}");
    }
}
