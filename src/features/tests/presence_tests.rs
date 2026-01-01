use super::super::presence::*;

#[test]
fn test_presence_status_display() {
    assert_eq!(PresenceStatus::Available.to_string(), "available");
    assert_eq!(PresenceStatus::Unavailable.to_string(), "unavailable");
}

#[test]
fn test_presence_status_as_str() {
    assert_eq!(PresenceStatus::Available.as_str(), "available");
    assert_eq!(PresenceStatus::Unavailable.as_str(), "unavailable");
}
