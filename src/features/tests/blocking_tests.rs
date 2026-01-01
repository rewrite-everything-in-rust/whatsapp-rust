use super::super::blocking::*;
use wacore_binary::jid::Jid;

#[test]
fn test_blocklist_entry() {
    let jid: Jid = "1234567890@s.whatsapp.net"
        .parse()
        .expect("test JID should be valid");
    let entry = BlocklistEntry {
        jid: jid.clone(),
        timestamp: Some(1234567890),
    };

    assert_eq!(entry.jid.user, "1234567890");
    assert_eq!(entry.timestamp, Some(1234567890));
}
