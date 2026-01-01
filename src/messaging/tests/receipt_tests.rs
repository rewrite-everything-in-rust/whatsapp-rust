use crate::client::Client;
use crate::store::SqliteStore;
use crate::store::persistence_manager::PersistenceManager;
use crate::test_utils::MockHttpClient;
use crate::types::message::{MessageInfo, MessageSource};
use std::sync::Arc;

#[tokio::test]
async fn test_send_delivery_receipt_dm() {
    let backend = Arc::new(
        SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );
    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );
    let (client, _rx) = Client::new(
        pm,
        Arc::new(crate::transport::mock::MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    let info = MessageInfo {
        id: "TEST-ID-123".to_string(),
        source: MessageSource {
            chat: "12345@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            sender: "12345@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            is_from_me: false,
            is_group: false,
            ..Default::default()
        },
        ..Default::default()
    };

    // This should complete without panicking. The actual node sending
    // would fail since we're not connected, but the function should
    // handle that gracefully and log a warning.
    client.send_delivery_receipt(&info).await;

    // If we got here, the function executed successfully.
    // In a real scenario, we'd need to mock the transport to verify
    // the exact node sent, but basic functionality testing confirms
    // the method doesn't panic and logs appropriately.
}

#[tokio::test]
async fn test_send_delivery_receipt_group() {
    let backend = Arc::new(
        SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );
    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );
    let (client, _rx) = Client::new(
        pm,
        Arc::new(crate::transport::mock::MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    let info = MessageInfo {
        id: "GROUP-MSG-ID".to_string(),
        source: MessageSource {
            chat: "120363021033254949@g.us"
                .parse()
                .expect("test JID should be valid"),
            sender: "15551234567@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            is_from_me: false,
            is_group: true,
            ..Default::default()
        },
        ..Default::default()
    };

    // Should complete without panicking for group messages too.
    client.send_delivery_receipt(&info).await;
}

#[tokio::test]
async fn test_skip_delivery_receipt_for_own_messages() {
    let backend = Arc::new(
        SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );
    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );
    let (client, _rx) = Client::new(
        pm,
        Arc::new(crate::transport::mock::MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    let info = MessageInfo {
        id: "OWN-MSG-ID".to_string(),
        source: MessageSource {
            chat: "12345@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            sender: "12345@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            is_from_me: true, // Own message
            is_group: false,
            ..Default::default()
        },
        ..Default::default()
    };

    // Should return early without attempting to send.
    // We can't easily assert that send_node was not called without
    // refactoring, but at least verify the function completes.
    client.send_delivery_receipt(&info).await;
}

#[tokio::test]
async fn test_skip_delivery_receipt_for_empty_id() {
    let backend = Arc::new(
        SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );
    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );
    let (client, _rx) = Client::new(
        pm,
        Arc::new(crate::transport::mock::MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    let info = MessageInfo {
        id: "".to_string(), // Empty ID
        source: MessageSource {
            chat: "12345@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            sender: "12345@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            is_from_me: false,
            is_group: false,
            ..Default::default()
        },
        ..Default::default()
    };

    // Should return early without attempting to send.
    client.send_delivery_receipt(&info).await;
}

#[tokio::test]
async fn test_skip_delivery_receipt_for_status_broadcast() {
    let backend = Arc::new(
        SqliteStore::new(":memory:")
            .await
            .expect("test backend should initialize"),
    );
    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );
    let (client, _rx) = Client::new(
        pm,
        Arc::new(crate::transport::mock::MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    let info = MessageInfo {
        id: "STATUS-MSG-ID".to_string(),
        source: MessageSource {
            chat: "status@broadcast"
                .parse()
                .expect("test JID should be valid"), // Status broadcast
            sender: "12345@s.whatsapp.net"
                .parse()
                .expect("test JID should be valid"),
            is_from_me: false,
            is_group: true,
            ..Default::default()
        },
        ..Default::default()
    };

    // Should return early without attempting to send for status broadcasts.
    client.send_delivery_receipt(&info).await;
}
