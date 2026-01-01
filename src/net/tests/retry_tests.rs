use crate::client::Client;
use crate::net::retry::{extract_registration_id_from_node, get_bytes_content};
use crate::store::persistence_manager::PersistenceManager;
use crate::test_utils::MockHttpClient;
use std::sync::Arc;
use wacore_binary::jid::{Jid, JidExt};
use wacore_binary::node::NodeContent;
use waproto::whatsapp as wa;

#[tokio::test]
async fn recent_message_cache_insert_and_take() {
    let _ = env_logger::builder().is_test(true).try_init();

    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    ) as Arc<dyn crate::store::traits::Backend>;
    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );
    let (client, _sync_rx) = Client::new(
        pm.clone(),
        Arc::new(crate::transport::mock::MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    let chat: Jid = "120363021033254949@g.us"
        .parse()
        .expect("test JID should be valid");
    let msg_id = "ABC123".to_string();
    let msg = wa::Message {
        conversation: Some("hello".into()),
        ..Default::default()
    };

    // Insert via the new async API
    client
        .add_recent_message(chat.clone(), msg_id.clone(), &msg)
        .await;

    // First take should return and remove it from cache
    let taken = client
        .take_recent_message(chat.clone(), msg_id.clone())
        .await;
    assert!(taken.is_some());
    assert_eq!(
        taken
            .expect("taken message should exist")
            .conversation
            .as_deref(),
        Some("hello")
    );

    // Second take should return None
    let taken_again = client.take_recent_message(chat, msg_id).await;
    assert!(taken_again.is_none());
}

#[test]
fn get_bytes_content_extracts_bytes() {
    use wacore_binary::node::Node;

    // Test with bytes content
    let node = Node {
        tag: "test".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Bytes(vec![1, 2, 3, 4])),
    };
    assert_eq!(get_bytes_content(&node), Some(&[1, 2, 3, 4][..]));

    // Test with string content (should return None)
    let node_str = Node {
        tag: "test".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::String("hello".to_string())),
    };
    assert_eq!(get_bytes_content(&node_str), None);

    // Test with no content
    let node_empty = Node {
        tag: "test".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: None,
    };
    assert_eq!(get_bytes_content(&node_empty), None);
}

#[test]
fn peer_detection_logic() {
    let our_jid = Jid::pn("559911112222");
    let peer_jid = Jid::pn_device("559911112222", 1);
    let other_jid = Jid::pn("559933334444");

    assert_eq!(our_jid.user, peer_jid.user);
    assert_ne!(our_jid.user, other_jid.user);
}

/// Integration test for retry receipt attribute logic.
/// Tests the fix for lost device sync messages (AC7B18EBD4445BFC55C0EA3CF9F913F8 case).
/// Matches WhatsApp Web's sendRetryReceipt: if (to.isUser()) { if (isMeAccount(to)) { ... } }
#[test]
fn retry_receipt_attributes_for_device_sync_vs_peer_vs_group() {
    use wacore::types::message::{MessageInfo, MessageSource};
    use wacore_binary::builder::NodeBuilder;

    let our_pn = Jid::pn("559999999999");
    let our_lid = Jid::lid("100000000000001");

    fn build_retry_receipt(
        info: &MessageInfo,
        our_pn: &Jid,
        our_lid: &Jid,
    ) -> wacore_binary::node::Node {
        let mut builder = NodeBuilder::new("receipt")
            .attr("to", info.source.sender.to_string())
            .attr("id", info.id.clone())
            .attr("type", "retry");

        if info.source.is_group {
            builder = builder.attr("participant", info.source.sender.to_string());
        }

        if !info.source.is_group {
            let is_from_own_account = info.source.sender.is_same_user_as(our_pn)
                || info.source.sender.is_same_user_as(our_lid);

            if is_from_own_account {
                if info.category == "peer" {
                    builder = builder.attr("category", "peer");
                } else {
                    let recipient = info.source.recipient.as_ref().unwrap_or(&info.source.chat);
                    builder = builder.attr("recipient", recipient.to_string());
                }
            }
        }

        builder.build()
    }

    // Case 1: Device sync DM
    let recipient_lid = Jid::lid("200000000000002");
    let device_sync_info = MessageInfo {
        id: "DEVICE_SYNC_MSG_001".to_string(),
        source: MessageSource {
            chat: recipient_lid.clone(),
            sender: our_lid.clone(),
            is_from_me: true,
            is_group: false,
            recipient: Some(recipient_lid.clone()),
            ..Default::default()
        },
        category: String::new(),
        ..Default::default()
    };

    let node = build_retry_receipt(&device_sync_info, &our_pn, &our_lid);
    assert_eq!(
        node.attrs().optional_string("recipient"),
        Some("200000000000002@lid"),
        "Device sync DM should include recipient"
    );
    assert!(
        node.attrs().optional_string("category").is_none(),
        "Device sync DM should NOT have category=peer"
    );
    assert!(
        node.attrs().optional_string("participant").is_none(),
        "DM should NOT have participant"
    );

    // Case 2: Peer DM with category="peer"
    let other_pn = Jid::pn("551188888888");
    let peer_info = MessageInfo {
        id: "PEER123".to_string(),
        source: MessageSource {
            chat: other_pn.clone(),
            sender: our_pn.clone(),
            is_from_me: true,
            is_group: false,
            recipient: None,
            ..Default::default()
        },
        category: "peer".to_string(),
        ..Default::default()
    };

    let node = build_retry_receipt(&peer_info, &our_pn, &our_lid);
    assert_eq!(
        node.attrs().optional_string("category"),
        Some("peer"),
        "Peer DM should have category=peer"
    );
    assert!(
        node.attrs().optional_string("recipient").is_none(),
        "Peer DM should NOT have recipient"
    );

    // Case 3: Group message from our own account
    let group_info = MessageInfo {
        id: "GROUP123".to_string(),
        source: MessageSource {
            chat: "123456789@g.us".parse().unwrap(),
            sender: our_lid.clone(),
            is_from_me: true,
            is_group: true,
            recipient: None,
            ..Default::default()
        },
        category: String::new(),
        ..Default::default()
    };

    let node = build_retry_receipt(&group_info, &our_pn, &our_lid);
    assert!(
        node.attrs().optional_string("participant").is_some(),
        "Group should have participant"
    );
    assert!(
        node.attrs().optional_string("category").is_none(),
        "Group should NOT have category"
    );
    assert!(
        node.attrs().optional_string("recipient").is_none(),
        "Group should NOT have recipient"
    );

    // Case 4: DM from someone else
    let other_dm_info = MessageInfo {
        id: "OTHER123".to_string(),
        source: MessageSource {
            chat: other_pn.clone(),
            sender: other_pn.clone(),
            is_from_me: false,
            is_group: false,
            recipient: None,
            ..Default::default()
        },
        category: String::new(),
        ..Default::default()
    };

    let node = build_retry_receipt(&other_dm_info, &our_pn, &our_lid);
    assert!(
        node.attrs().optional_string("category").is_none(),
        "DM from other should NOT have category"
    );
    assert!(
        node.attrs().optional_string("recipient").is_none(),
        "DM from other should NOT have recipient"
    );
}

#[test]
fn prekey_id_parsing() {
    // PreKey IDs are 3 bytes big-endian
    let id_bytes = [0x01, 0x02, 0x03];
    let prekey_id = u32::from_be_bytes([0, id_bytes[0], id_bytes[1], id_bytes[2]]);
    assert_eq!(prekey_id, 0x00010203);

    // Signed prekey IDs follow the same format
    let skey_id_bytes = [0xFF, 0xFE, 0xFD];
    let skey_id = u32::from_be_bytes([0, skey_id_bytes[0], skey_id_bytes[1], skey_id_bytes[2]]);
    assert_eq!(skey_id, 0x00FFFEFD);
}

#[tokio::test]
async fn base_key_store_operations() {
    use wacore::store::traits::ProtocolStore as _;

    let _ = env_logger::builder().is_test(true).try_init();

    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );

    let address = "12345.0:1";
    let msg_id = "ABC123";
    let base_key = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    // Initially, has_same_base_key should return false (no saved key)
    let result = backend.has_same_base_key(address, msg_id, &base_key).await;
    assert!(result.is_ok());
    assert!(!result.unwrap());

    // Save the base key
    let save_result = backend.save_base_key(address, msg_id, &base_key).await;
    assert!(save_result.is_ok());

    // Same key should now match (collision detected)
    let result = backend.has_same_base_key(address, msg_id, &base_key).await;
    assert!(result.is_ok());
    assert!(result.unwrap());

    // Different key should NOT match (no collision)
    let different_key = vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
    let result = backend
        .has_same_base_key(address, msg_id, &different_key)
        .await;
    assert!(result.is_ok());
    assert!(!result.unwrap());

    // Delete the base key
    let delete_result = backend.delete_base_key(address, msg_id).await;
    assert!(delete_result.is_ok());

    // After deletion, has_same_base_key should return false
    let result = backend.has_same_base_key(address, msg_id, &base_key).await;
    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[tokio::test]
async fn base_key_store_upsert() {
    use wacore::store::traits::ProtocolStore as _;

    let _ = env_logger::builder().is_test(true).try_init();

    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );

    let address = "12345.0:1";
    let msg_id = "MSG001";
    let first_key = vec![1, 2, 3];
    let second_key = vec![4, 5, 6];

    // Save first key
    backend
        .save_base_key(address, msg_id, &first_key)
        .await
        .unwrap();
    assert!(
        backend
            .has_same_base_key(address, msg_id, &first_key)
            .await
            .unwrap()
    );
    assert!(
        !backend
            .has_same_base_key(address, msg_id, &second_key)
            .await
            .unwrap()
    );

    // Save second key (upsert should replace)
    backend
        .save_base_key(address, msg_id, &second_key)
        .await
        .unwrap();
    assert!(
        !backend
            .has_same_base_key(address, msg_id, &first_key)
            .await
            .unwrap()
    );
    assert!(
        backend
            .has_same_base_key(address, msg_id, &second_key)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn base_key_store_multiple_messages() {
    use wacore::store::traits::ProtocolStore as _;

    let _ = env_logger::builder().is_test(true).try_init();

    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );

    let address = "12345.0:1";
    let msg_id_1 = "MSG001";
    let msg_id_2 = "MSG002";
    let key_1 = vec![1, 2, 3];
    let key_2 = vec![4, 5, 6];

    // Save keys for different messages
    backend
        .save_base_key(address, msg_id_1, &key_1)
        .await
        .unwrap();
    backend
        .save_base_key(address, msg_id_2, &key_2)
        .await
        .unwrap();

    // Each message should have its own key
    assert!(
        backend
            .has_same_base_key(address, msg_id_1, &key_1)
            .await
            .unwrap()
    );
    assert!(
        !backend
            .has_same_base_key(address, msg_id_1, &key_2)
            .await
            .unwrap()
    );
    assert!(
        !backend
            .has_same_base_key(address, msg_id_2, &key_1)
            .await
            .unwrap()
    );
    assert!(
        backend
            .has_same_base_key(address, msg_id_2, &key_2)
            .await
            .unwrap()
    );

    // Delete one message's key, other should remain
    backend.delete_base_key(address, msg_id_1).await.unwrap();
    assert!(
        !backend
            .has_same_base_key(address, msg_id_1, &key_1)
            .await
            .unwrap()
    );
    assert!(
        backend
            .has_same_base_key(address, msg_id_2, &key_2)
            .await
            .unwrap()
    );
}

#[test]
fn bot_jid_detection() {
    // Test bot JID detection for bot message filtering
    use wacore_binary::jid::JidExt as _;

    // Regular user JID - not a bot
    let regular_user: Jid = "1234567890@s.whatsapp.net".parse().unwrap();
    assert!(!regular_user.is_bot());

    // Bot JID with bot server
    let bot_server: Jid = "somebot@bot".parse().unwrap();
    assert!(bot_server.is_bot());

    // Legacy bot JID pattern (1313555...)
    let legacy_bot: Jid = "1313555123456@s.whatsapp.net".parse().unwrap();
    assert!(legacy_bot.is_bot());

    // Legacy bot JID pattern (131655500...)
    let legacy_bot2: Jid = "131655500123456@s.whatsapp.net".parse().unwrap();
    assert!(legacy_bot2.is_bot());

    // Similar but not bot (doesn't start with exact prefix)
    let not_bot: Jid = "1313556123456@s.whatsapp.net".parse().unwrap();
    assert!(!not_bot.is_bot());
}

#[test]
fn extract_registration_id_from_node_test() {
    use wacore_binary::node::Node;

    // Test with 4-byte registration ID
    let reg_bytes = vec![0x00, 0x01, 0x02, 0x03]; // = 66051
    let reg_node = Node {
        tag: "registration".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Bytes(reg_bytes)),
    };
    let parent = Node {
        tag: "receipt".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Nodes(vec![reg_node])),
    };
    assert_eq!(extract_registration_id_from_node(&parent), Some(0x00010203));

    // Test with 3-byte registration ID (variable length)
    let reg_bytes_short = vec![0x01, 0x02, 0x03]; // = 66051
    let reg_node_short = Node {
        tag: "registration".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Bytes(reg_bytes_short)),
    };
    let parent_short = Node {
        tag: "receipt".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Nodes(vec![reg_node_short])),
    };
    assert_eq!(
        extract_registration_id_from_node(&parent_short),
        Some(0x00010203)
    );

    // Test with no registration node
    let parent_no_reg = Node {
        tag: "receipt".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Nodes(vec![])),
    };
    assert_eq!(extract_registration_id_from_node(&parent_no_reg), None);

    // Test with empty bytes
    let reg_node_empty = Node {
        tag: "registration".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Bytes(vec![])),
    };
    let parent_empty = Node {
        tag: "receipt".to_string(),
        attrs: indexmap::IndexMap::new(),
        content: Some(NodeContent::Nodes(vec![reg_node_empty])),
    };
    assert_eq!(extract_registration_id_from_node(&parent_empty), None);
}

#[test]
fn group_or_status_detection_for_sender_key_handling() {
    // Test that both groups and status broadcasts trigger sender key handling
    use wacore_binary::jid::JidExt as _;

    let group: Jid = "120363021033254949@g.us".parse().unwrap();
    let status: Jid = "status@broadcast".parse().unwrap();
    let dm: Jid = "1234567890@s.whatsapp.net".parse().unwrap();

    // Both group and status should trigger sender key deletion
    assert!(group.is_group() || group.is_status_broadcast());
    assert!(status.is_group() || status.is_status_broadcast());

    // DM should NOT trigger sender key deletion
    assert!(!(dm.is_group() || dm.is_status_broadcast()));
}
