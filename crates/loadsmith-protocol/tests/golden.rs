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
