use super::super::chatstate::*;

#[test]
fn test_chat_state_type_display() {
    assert_eq!(ChatStateType::Composing.to_string(), "composing");
    assert_eq!(ChatStateType::Recording.to_string(), "recording");
    assert_eq!(ChatStateType::Paused.to_string(), "paused");
}

#[test]
fn test_chat_state_type_as_str() {
    assert_eq!(ChatStateType::Composing.as_str(), "composing");
    assert_eq!(ChatStateType::Recording.as_str(), "recording");
    assert_eq!(ChatStateType::Paused.as_str(), "paused");
}
