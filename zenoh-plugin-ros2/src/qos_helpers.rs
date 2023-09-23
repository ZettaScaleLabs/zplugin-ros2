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
use cyclors::{qos::*, DDS_LENGTH_UNLIMITED};
use zenoh::prelude::{keyexpr, OwnedKeyExpr};

use crate::ke_for_sure;

pub fn get_history_or_default(qos: &Qos) -> History {
    match &qos.history {
        None => History::default(),
        Some(history) => history.clone(),
    }
}

pub fn get_durability_service_or_default(qos: &Qos) -> DurabilityService {
    match &qos.durability_service {
        None => DurabilityService::default(),
        Some(durability_service) => durability_service.clone(),
    }
}

pub fn partition_is_empty(partition: &Option<Vec<String>>) -> bool {
    partition
        .as_ref()
        .map_or(true, |partition| partition.is_empty())
}

pub fn partition_contains(partition: &Option<Vec<String>>, name: &String) -> bool {
    partition
        .as_ref()
        .map_or(false, |partition| partition.contains(name))
}

pub fn is_writer_reliable(reliability: &Option<Reliability>) -> bool {
    reliability.as_ref().map_or(true, |reliability| {
        reliability.kind == ReliabilityKind::RELIABLE
    })
}

pub fn is_reader_reliable(reliability: &Option<Reliability>) -> bool {
    reliability.as_ref().map_or(false, |reliability| {
        reliability.kind == ReliabilityKind::RELIABLE
    })
}

pub fn is_transient_local(qos: &Qos) -> bool {
    qos.durability.as_ref().map_or(false, |durability| {
        durability.kind == DurabilityKind::TRANSIENT_LOCAL
    })
}

// Copy and adapt Writer's QoS for creation of a matching Reader
pub fn adapt_writer_qos_for_reader(qos: &Qos) -> Qos {
    let mut reader_qos = qos.clone();

    // Unset any writer QoS that doesn't apply to data readers
    reader_qos.durability_service = None;
    reader_qos.ownership_strength = None;
    reader_qos.transport_priority = None;
    reader_qos.lifespan = None;
    reader_qos.writer_data_lifecycle = None;
    reader_qos.writer_batching = None;

    // Unset proprietary QoS which shouldn't apply
    reader_qos.properties = None;
    reader_qos.entity_name = None;
    reader_qos.ignore_local = None;

    // Set default Reliability QoS if not set for writer
    if reader_qos.reliability.is_none() {
        reader_qos.reliability = Some({
            Reliability {
                kind: ReliabilityKind::BEST_EFFORT,
                max_blocking_time: DDS_100MS_DURATION,
            }
        });
    }

    reader_qos
}

// Copy and adapt Reader's QoS for creation of a matching Writer
pub fn adapt_reader_qos_for_writer(qos: &Qos) -> Qos {
    let mut writer_qos = qos.clone();

    // Unset any reader QoS that doesn't apply to data writers
    writer_qos.time_based_filter = None;
    writer_qos.reader_data_lifecycle = None;
    writer_qos.properties = None;
    writer_qos.entity_name = None;

    // Don't match with readers with the same participant
    writer_qos.ignore_local = Some(IgnoreLocal {
        kind: IgnoreLocalKind::PARTICIPANT,
    });

    // if Reader is TRANSIENT_LOCAL, configure durability_service QoS with same history as the Reader.
    // This is because CycloneDDS is actually using durability_service.history for transient_local historical data.
    if is_transient_local(qos) {
        let history = qos
            .history
            .as_ref()
            .map_or(History::default(), |history| history.clone());

        writer_qos.durability_service = Some(DurabilityService {
            service_cleanup_delay: 60 * DDS_1S_DURATION,
            history_kind: history.kind,
            history_depth: history.depth,
            max_samples: DDS_LENGTH_UNLIMITED,
            max_instances: DDS_LENGTH_UNLIMITED,
            max_samples_per_instance: DDS_LENGTH_UNLIMITED,
        });
    }
    // Workaround for the DDS Writer to correctly match with a FastRTPS Reader
    writer_qos.reliability = match writer_qos.reliability {
        Some(mut reliability) => {
            reliability.max_blocking_time = reliability.max_blocking_time.saturating_add(1);
            Some(reliability)
        }
        _ => {
            let mut reliability = Reliability::default();
            reliability.max_blocking_time = reliability.max_blocking_time.saturating_add(1);
            Some(reliability)
        }
    };

    writer_qos
}

// Serialize QoS as a KeyExpr-compatible string (for usage in liveliness keyexpr)
// NOTE: only significant Qos for ROS2 are serialized
// See https://docs.ros.org/en/rolling/Concepts/Intermediate/About-Quality-of-Service-Settings.html
//
// format: "<keyless>:<ReliabilityKind>:<DurabilityKind>:<HistoryKid>,<HistoryDepth>"
// where each element is "" if default QoS, or an integer in case of enum, and 'K' for !keyless
pub fn qos_to_key_expr(keyless: bool, qos: &Qos) -> OwnedKeyExpr {
    use std::io::Write;
    let mut w: Vec<u8> = Vec::new();

    if !keyless {
        write!(w, "K").unwrap();
    }
    write!(w, ":").unwrap();
    if let Some(Reliability { kind, .. }) = &qos.reliability {
        write!(&mut w, "{}", *kind as isize).unwrap();
    }
    write!(w, ":").unwrap();
    if let Some(Durability { kind }) = &qos.durability {
        write!(&mut w, "{}", *kind as isize).unwrap();
    }
    write!(w, ":").unwrap();
    if let Some(History { kind, depth }) = &qos.history {
        write!(&mut w, "{},{}", *kind as isize, depth).unwrap();
    }

    unsafe {
        let s: String = String::from_utf8_unchecked(w);
        OwnedKeyExpr::from_string_unchecked(s)
    }
}

pub fn key_expr_to_qos(ke: &keyexpr) -> Result<(bool, Qos), String> {
    let elts: Vec<&str> = ke.split(':').collect();
    if elts.len() != 4 {
        return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - 4 elements between : were expected"));
    }
    let mut qos = Qos::default();
    let keyless = elts[0].is_empty();
    if !elts[1].is_empty() {
        match elts[1].parse::<u32>() {
            Ok(i) => qos.reliability = Some(Reliability {kind: ReliabilityKind::from(&i), max_blocking_time: DDS_100MS_DURATION }),
            Err(_) => return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - failed to parse Reliability in 2nd element")),
        }
    }
    if !elts[2].is_empty() {
        match elts[2].parse::<u32>() {
            Ok(i) => qos.durability = Some(Durability {kind: DurabilityKind::from(&i)}),
            Err(_) => return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - failed to parse Durability in 3d element")),
        }
    }
    if !elts[3].is_empty() {
        match elts[3].split_once(',').map(|(s1, s2)|
            (
                s1.parse::<u32>(),
                s2.parse::<i32>(),
            )
        ) {
            Some((Ok(k), Ok(depth))) => qos.history = Some(History {kind: HistoryKind::from(&k), depth }),
            _ => return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - failed to parse History in 4th element")),
        }
    }

    Ok((keyless, qos))
}

mod tests {
    use super::*;
    use std::ops::Deref;
    use std::str::FromStr;

    #[test]
    fn test_qos_key_expr() {
        let mut q = Qos::default();
        assert_eq!(qos_to_key_expr(true, &q).to_string(), ":::");
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        assert_eq!(qos_to_key_expr(false, &q).to_string(), "K:::");
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(false, &q)),
            Ok((false, q.clone()))
        );

        q.reliability = Some(Reliability {
            kind: ReliabilityKind::RELIABLE,
            max_blocking_time: DDS_100MS_DURATION,
        });
        assert_eq!(
            qos_to_key_expr(true, &q).to_string(),
            format!(":{}::", ReliabilityKind::RELIABLE as u8)
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        q.reliability = None;

        q.durability = Some(Durability {
            kind: DurabilityKind::TRANSIENT_LOCAL,
        });
        assert_eq!(
            qos_to_key_expr(true, &q).to_string(),
            format!("::{}:", DurabilityKind::TRANSIENT_LOCAL as u8)
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        q.durability = None;

        q.history = Some(History {
            kind: HistoryKind::KEEP_LAST,
            depth: 3,
        });
        assert_eq!(
            qos_to_key_expr(true, &q).to_string(),
            format!(":::{},3", HistoryKind::KEEP_LAST as u8)
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        q.reliability = None;
    }
}
