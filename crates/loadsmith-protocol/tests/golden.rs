//! Golden wire fixtures: each line of `fixtures/*.jsonl` is the exact byte
//! form of one protocol v1 message. A fixture must parse to a known message
//! and serialize back to the identical line, so a change to a covered wire
//! shape (renamed field, new required field, changed tag) fails here.

use loadsmith_protocol::Message;

fn check_fixture(name: &str, text: &str) -> Vec<Message> {
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let msg: Message = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{name}:{}: does not parse: {e}", i + 1));
        assert!(!matches!(msg, Message::Unknown), "{name}:{}: parsed as an unknown type", i + 1);
        let back = serde_json::to_string(&msg).expect("serialize");
        assert_eq!(back, line, "{name}:{}: does not round-trip byte for byte", i + 1);
        out.push(msg);
    }
    assert!(!out.is_empty(), "{name}: empty fixture");
    out
}

#[test]
fn abort_messages_match_golden_fixture() {
    let msgs = check_fixture("abort_v1.jsonl", include_str!("fixtures/abort_v1.jsonl"));
    let count = |f: fn(&Message) -> bool| msgs.iter().filter(|m| f(m)).count();
    assert_eq!(count(|m| matches!(m, Message::Cancel(_))), 2);
    assert_eq!(count(|m| matches!(m, Message::Error(_))), 5);
    assert_eq!(count(|m| matches!(m, Message::Resend(_))), 2);
    assert_eq!(count(|m| matches!(m, Message::Finished(_))), 2);
}

#[test]
fn error_fixture_codes_are_the_published_constants() {
    use loadsmith_protocol::error_codes::*;
    let msgs = check_fixture("abort_v1.jsonl", include_str!("fixtures/abort_v1.jsonl"));
    let codes: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            Message::Error(e) => Some(e.code.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        codes,
        [UNSUPPORTED_PROTOCOL_VERSION, WRONG_PLUGIN_KIND, PROTOCOL_VIOLATION, MALFORMED_MESSAGE, TIMEOUT]
    );
}

#[test]
fn provider_messages_match_golden_fixture() {
    use loadsmith_protocol::{capabilities, PluginKind};
    let msgs = check_fixture("provider_v1.jsonl", include_str!("fixtures/provider_v1.jsonl"));
    assert!(msgs.iter().any(|m| matches!(m, Message::HandshakeAck(a) if a.kind == PluginKind::Provider)));
    assert!(msgs.iter().any(
        |m| matches!(m, Message::CapabilitiesResponse(c) if c.supports == [capabilities::RESOLVE])
    ));
    let resolved: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            Message::Resolved(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[0].value.as_deref(), Some("s3cr3t"));
    assert!(resolved[0].error.is_none());
    assert!(resolved[1].value.is_none());
    assert_eq!(resolved[1].error.as_ref().unwrap().code, "NOT_FOUND");
    assert_eq!(msgs.iter().filter(|m| matches!(m, Message::Resolve(_))).count(), 2);
    // A provider's `finished` carries no payload.
    let Some(Message::Finished(f)) = msgs.last() else { panic!("ends with finished") };
    assert!(f.message.is_none() && f.rows_read.is_none());
}

#[test]
fn parser_messages_match_golden_fixture() {
    use loadsmith_protocol::{capabilities, PluginKind};
    let msgs = check_fixture("parser_v1.jsonl", include_str!("fixtures/parser_v1.jsonl"));
    assert!(msgs.iter().any(|m| matches!(m, Message::HandshakeAck(a) if a.kind == PluginKind::Parser)));
    for cap in [capabilities::PARSE, capabilities::OBJECT_OUTPUT] {
        assert!(msgs.iter().any(|m| matches!(m, Message::CapabilitiesResponse(c) if c.supports == [cap])));
    }
    assert!(msgs.iter().any(|m| matches!(m, Message::Ready)));
    assert!(msgs.iter().any(|m| matches!(m, Message::ParseObject(p) if p.path.ends_with(".csv"))));
    let parsed: Vec<_> = msgs
        .iter()
        .filter_map(|m| match m {
            Message::ObjectParsed(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(parsed.len(), 2);
    assert_eq!((parsed[0].rows, parsed[0].batches, parsed[0].last_batch_seq), (1500, 2, 2));
}

#[test]
fn schema_vocabulary_matches_golden_fixture() {
    let msgs = check_fixture("schema_v1.jsonl", include_str!("fixtures/schema_v1.jsonl"));
    for msg in &msgs {
        let Message::Schema(s) = msg else { panic!("not a schema: {msg:?}") };
        for f in &s.fields {
            assert!(!f.field_type.is_unknown(), "{} parsed as unknown", f.name);
        }
    }
}

/// Plugins built with an SDK before the rich vocabulary send bare type
/// strings, no `nullable`, and `timestamp_ms`. They must keep parsing.
#[test]
fn legacy_schema_messages_still_parse() {
    use loadsmith_protocol::{Field, FieldType};
    let line = r#"{"type":"schema","fields":[{"name":"id","type":"int64"},{"name":"name","type":"utf8"},{"name":"created_at","type":"timestamp_ms"}]}"#;
    let Message::Schema(s) = serde_json::from_str(line).unwrap() else { panic!() };
    assert_eq!(
        s.fields,
        [
            Field::new("id", FieldType::Int64),
            Field::new("name", FieldType::Utf8),
            Field::new("created_at", FieldType::timestamp_ms()),
        ]
    );
}

/// A type from a newer vocabulary never makes the message malformed.
#[test]
fn unknown_field_types_do_not_fail_the_message() {
    let line = r#"{"type":"schema","fields":[{"name":"g","type":"geometry","srid":4326},{"name":"id","type":"int64"}]}"#;
    let Message::Schema(s) = serde_json::from_str(line).unwrap() else { panic!() };
    assert!(s.fields[0].field_type.is_unknown());
    assert!(!s.fields[1].field_type.is_unknown());
}
