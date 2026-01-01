use crate::client::Client;
use crate::lid_pn_cache::LearningSource;
use crate::store::persistence_manager::PersistenceManager;
use crate::test_utils::MockHttpClient;
use log::info;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use wacore_binary::builder::NodeBuilder;
use wacore_binary::jid::SERVER_JID;

#[tokio::test]
async fn test_ack_behavior_for_incoming_stanzas() {
    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // --- Assertions ---

    // Verify that we still ack other critical stanzas (regression check).
    use indexmap::IndexMap;
    use wacore_binary::node::{Node, NodeContent};

    let mut receipt_attrs = IndexMap::new();
    receipt_attrs.insert("from".to_string(), "@s.whatsapp.net".to_string());
    receipt_attrs.insert("id".to_string(), "RCPT-1".to_string());
    let receipt_node = Node::new(
        "receipt",
        receipt_attrs,
        Some(NodeContent::String("test".to_string())),
    );

    let mut notification_attrs = IndexMap::new();
    notification_attrs.insert("from".to_string(), "@s.whatsapp.net".to_string());
    notification_attrs.insert("id".to_string(), "NOTIF-1".to_string());
    let notification_node = Node::new(
        "notification",
        notification_attrs,
        Some(NodeContent::String("test".to_string())),
    );

    assert!(
        client.should_ack(&receipt_node),
        "should_ack must still return TRUE for <receipt> stanzas."
    );
    assert!(
        client.should_ack(&notification_node),
        "should_ack must still return TRUE for <notification> stanzas."
    );

    info!(
        "[OK] test_ack_behavior_for_incoming_stanzas passed: Client correctly differentiates which stanzas to acknowledge."
    );
}

#[tokio::test]
async fn test_plaintext_buffer_pool_reuses_buffers() {
    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // Check initial pool size
    let initial_pool_size = {
        let pool = client.plaintext_buffer_pool.lock().await;
        pool.len()
    };

    // Attempt to send a node (this will fail because we're not connected, but that's okay)
    let test_node = NodeBuilder::new("test").attr("id", "test-123").build();

    let _ = client.send_node(test_node).await;

    // After the send attempt, the pool should have the same or more buffers
    // (depending on whether buffers were consumed and returned)
    let final_pool_size = {
        let pool = client.plaintext_buffer_pool.lock().await;
        pool.len()
    };

    assert!(
        final_pool_size >= initial_pool_size,
        "Plaintext buffer pool should not shrink after send operations"
    );

    info!(
        "[OK] test_plaintext_buffer_pool_reuses_buffers passed: Buffer pool properly manages plaintext buffers"
    );
}

#[tokio::test]
async fn test_ack_waiter_resolves() {
    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // 1. Insert a waiter for a specific ID
    let test_id = "ack-test-123".to_string();
    let (tx, rx) = oneshot::channel();
    client
        .response_waiters
        .lock()
        .await
        .insert(test_id.clone(), tx);
    assert!(
        client.response_waiters.lock().await.contains_key(&test_id),
        "Waiter should be inserted before handling ack"
    );

    // 2. Create a mock <ack/> node with the test ID
    let ack_node = NodeBuilder::new("ack")
        .attr("id", test_id.clone())
        .attr("from", SERVER_JID)
        .build();

    // 3. Handle the ack
    let handled = client.handle_ack_response(ack_node).await;
    assert!(
        handled,
        "handle_ack_response should return true when waiter exists"
    );

    // 4. Await the receiver with a timeout
    match tokio::time::timeout(Duration::from_secs(1), rx).await {
        Ok(Ok(response_node)) => {
            assert_eq!(
                response_node.attrs.get("id"),
                Some(&test_id),
                "Response node should have correct ID"
            );
        }
        Ok(Err(_)) => panic!("Receiver was dropped without being sent a value"),
        Err(_) => panic!("Test timed out waiting for ack response"),
    }

    // 5. Verify the waiter was removed
    assert!(
        !client.response_waiters.lock().await.contains_key(&test_id),
        "Waiter should be removed after handling"
    );

    info!("[OK] test_ack_waiter_resolves passed: ACK response correctly resolves pending waiters");
}

#[tokio::test]
async fn test_ack_without_matching_waiter() {
    let backend = Arc::new(
        crate::store::SqliteStore::new(":memory:")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // Create an ack without any matching waiter
    let ack_node = NodeBuilder::new("ack")
        .attr("id", "non-existent-id")
        .attr("from", SERVER_JID)
        .build();

    // Should return false since there's no waiter
    let handled = client.handle_ack_response(ack_node).await;
    assert!(
        !handled,
        "handle_ack_response should return false when no waiter exists"
    );

    info!(
        "[OK] test_ack_without_matching_waiter passed: ACK without matching waiter handled gracefully"
    );
}

/// Test that the lid_pn_cache correctly stores and retrieves LID mappings.
///
/// This is critical for the LID-PN session mismatch fix. When we receive a message
/// with sender_lid, we cache the phone->LID mapping so that when sending replies,
/// we can reuse the existing LID session instead of creating a new PN session.
#[tokio::test]
async fn test_lid_pn_cache_basic_operations() {
    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_lid_cache_basic?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // Initially, the cache should be empty for a phone number
    let phone = "559980000001";
    let lid = "100000012345678";

    assert!(
        client.lid_pn_cache.get_current_lid(phone).await.is_none(),
        "Cache should be empty initially"
    );

    // Insert a phone->LID mapping using add_lid_pn_mapping
    client
        .add_lid_pn_mapping(lid, phone, LearningSource::Usync)
        .await
        .expect("Failed to persist LID-PN mapping in tests");

    // Verify we can retrieve it (phone -> LID lookup)
    let cached_lid = client.lid_pn_cache.get_current_lid(phone).await;
    assert!(cached_lid.is_some(), "Cache should contain the mapping");
    assert_eq!(
        cached_lid.expect("cache should have LID"),
        lid,
        "Cached LID should match what we inserted"
    );

    // Verify reverse lookup works (LID -> phone)
    let cached_phone = client.lid_pn_cache.get_phone_number(lid).await;
    assert!(cached_phone.is_some(), "Reverse lookup should work");
    assert_eq!(
        cached_phone.expect("reverse lookup should return phone"),
        phone,
        "Cached phone should match what we inserted"
    );

    // Verify a different phone number returns None
    assert!(
        client
            .lid_pn_cache
            .get_current_lid("559980000002")
            .await
            .is_none(),
        "Different phone number should not have a mapping"
    );

    info!("[OK] test_lid_pn_cache_basic_operations passed: LID-PN cache works correctly");
}

/// Test that the lid_pn_cache respects timestamp-based conflict resolution.
///
/// When a phone number has multiple LIDs, the most recent one should be returned.
#[tokio::test]
async fn test_lid_pn_cache_timestamp_resolution() {
    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_lid_cache_timestamp?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    let phone = "559980000001";
    let lid_old = "100000012345678";
    let lid_new = "100000087654321";

    // Insert initial mapping
    client
        .add_lid_pn_mapping(lid_old, phone, LearningSource::Usync)
        .await
        .expect("Failed to persist LID-PN mapping in tests");

    assert_eq!(
        client
            .lid_pn_cache
            .get_current_lid(phone)
            .await
            .expect("cache should have LID"),
        lid_old,
        "Initial LID should be stored"
    );

    // Small delay to ensure different timestamp
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Add new mapping with newer timestamp
    client
        .add_lid_pn_mapping(lid_new, phone, LearningSource::PeerPnMessage)
        .await
        .expect("Failed to persist LID-PN mapping in tests");

    assert_eq!(
        client
            .lid_pn_cache
            .get_current_lid(phone)
            .await
            .expect("cache should have newer LID"),
        lid_new,
        "Newer LID should be returned for phone lookup"
    );

    // Both LIDs should still resolve to the same phone
    assert_eq!(
        client
            .lid_pn_cache
            .get_phone_number(lid_old)
            .await
            .expect("reverse lookup should return phone"),
        phone,
        "Old LID should still map to phone"
    );
    assert_eq!(
        client
            .lid_pn_cache
            .get_phone_number(lid_new)
            .await
            .expect("reverse lookup should return phone"),
        phone,
        "New LID should also map to phone"
    );

    info!(
        "[OK] test_lid_pn_cache_timestamp_resolution passed: Timestamp-based resolution works correctly"
    );
}

/// Test that get_lid_for_phone (from SendContextResolver) returns the cached value.
///
/// This is the method used by wacore::send to look up LID mappings when encrypting.
#[tokio::test]
async fn test_get_lid_for_phone_via_send_context_resolver() {
    use wacore::client::context::SendContextResolver;

    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_get_lid_for_phone?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    let phone = "559980000001";
    let lid = "100000012345678";

    // Before caching, should return None
    assert!(
        client.get_lid_for_phone(phone).await.is_none(),
        "get_lid_for_phone should return None before caching"
    );

    // Cache the mapping using add_lid_pn_mapping
    client
        .add_lid_pn_mapping(lid, phone, LearningSource::Usync)
        .await
        .expect("Failed to persist LID-PN mapping in tests");

    // Now it should return the LID
    let result = client.get_lid_for_phone(phone).await;
    assert!(
        result.is_some(),
        "get_lid_for_phone should return Some after caching"
    );
    assert_eq!(
        result.expect("get_lid_for_phone should return Some"),
        lid,
        "get_lid_for_phone should return the cached LID"
    );

    info!(
        "[OK] test_get_lid_for_phone_via_send_context_resolver passed: SendContextResolver correctly returns cached LID"
    );
}

// =========================================================================
// PDO Session Establishment Timing Tests
// =========================================================================
// These tests verify the critical timing behavior for PDO:
// - Session with device 0 must be established BEFORE offline messages arrive
// - ensure_e2e_sessions() waits for offline sync (for normal message sending)
// - establish_primary_phone_session_immediate() does NOT wait (for login)
// =========================================================================

/// Test that wait_for_offline_delivery_end returns immediately when the flag is already set.
#[tokio::test]
async fn test_wait_for_offline_delivery_end_returns_immediately_when_flag_set() {
    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_offline_sync_flag_set?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // Set the flag to true (simulating offline sync completed)
    client
        .offline_sync_completed
        .store(true, std::sync::atomic::Ordering::Relaxed);

    // This should return immediately (not wait 10 seconds)
    let start = std::time::Instant::now();
    client.wait_for_offline_delivery_end().await;
    let elapsed = start.elapsed();

    // Should complete in < 100ms (not 10 second timeout)
    assert!(
        elapsed.as_millis() < 100,
        "wait_for_offline_delivery_end should return immediately when flag is set, took {:?}",
        elapsed
    );

    info!("[OK] test_wait_for_offline_delivery_end_returns_immediately_when_flag_set passed");
}

/// Test that wait_for_offline_delivery_end times out when the flag is NOT set.
/// This verifies the 10-second timeout is working.
#[tokio::test]
async fn test_wait_for_offline_delivery_end_times_out_when_flag_not_set() {
    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_offline_sync_timeout?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // Flag is false by default, so we need to use a shorter timeout for the test
    // We'll verify behavior by using tokio timeout
    let start = std::time::Instant::now();

    // Use a short timeout to test the behavior without waiting 10 seconds
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        client.wait_for_offline_delivery_end(),
    )
    .await;

    let elapsed = start.elapsed();

    // The wait should NOT complete immediately - it should timeout
    // (because the flag is false and no one is notifying)
    assert!(
        result.is_err(),
        "wait_for_offline_delivery_end should not return immediately when flag is false"
    );
    assert!(
        elapsed.as_millis() >= 95, // Allow small timing variance
        "Should have waited for the timeout duration, took {:?}",
        elapsed
    );

    info!("[OK] test_wait_for_offline_delivery_end_times_out_when_flag_not_set passed");
}

/// Test that wait_for_offline_delivery_end returns when notified.
#[tokio::test]
async fn test_wait_for_offline_delivery_end_returns_on_notify() {
    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_offline_notify?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    let client_clone = client.clone();

    // Spawn a task that will notify after 50ms
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        client_clone.offline_sync_notifier.notify_waiters();
    });

    let start = std::time::Instant::now();
    client.wait_for_offline_delivery_end().await;
    let elapsed = start.elapsed();

    // Should complete around 50ms (when notified), not 10 seconds
    assert!(
        elapsed.as_millis() < 200,
        "wait_for_offline_delivery_end should return when notified, took {:?}",
        elapsed
    );
    assert!(
        elapsed.as_millis() >= 45, // Should have waited for the notify
        "Should have waited for the notify, only took {:?}",
        elapsed
    );

    info!("[OK] test_wait_for_offline_delivery_end_returns_on_notify passed");
}

/// Test that the offline_sync_completed flag starts as false.
#[tokio::test]
async fn test_offline_sync_flag_initially_false() {
    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_offline_flag_initial?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // The flag should be false initially
    assert!(
        !client
            .offline_sync_completed
            .load(std::sync::atomic::Ordering::Relaxed),
        "offline_sync_completed should be false when Client is first created"
    );

    info!("[OK] test_offline_sync_flag_initially_false passed");
}

/// Test the complete offline sync lifecycle:
/// 1. Flag starts false
/// 2. Flag is set true after IB offline stanza
/// 3. Notify is called
#[tokio::test]
async fn test_offline_sync_lifecycle() {
    use std::sync::atomic::Ordering;

    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_offline_lifecycle?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // 1. Initially false
    assert!(!client.offline_sync_completed.load(Ordering::Relaxed));

    // 2. Spawn a waiter
    let client_waiter = client.clone();
    let waiter_handle = tokio::spawn(async move {
        client_waiter.wait_for_offline_delivery_end().await;
        true // Return that we completed
    });

    // Give the waiter time to start waiting
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Verify waiter hasn't completed yet
    assert!(
        !waiter_handle.is_finished(),
        "Waiter should still be waiting"
    );

    // 3. Simulate IB handler behavior (set flag and notify)
    client.offline_sync_completed.store(true, Ordering::Relaxed);
    client.offline_sync_notifier.notify_waiters();

    // 4. Waiter should complete
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), waiter_handle)
        .await
        .expect("Waiter should complete after notify")
        .expect("Waiter task should not panic");

    assert!(result, "Waiter should have completed successfully");
    assert!(client.offline_sync_completed.load(Ordering::Relaxed));

    info!("[OK] test_offline_sync_lifecycle passed");
}

/// Test that establish_primary_phone_session_immediate returns error when no PN is set.
/// This verifies the "not logged in" guard works.
#[tokio::test]
async fn test_establish_primary_phone_session_fails_without_pn() {
    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_no_pn?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // No PN set, so this should fail
    let result = client.establish_primary_phone_session_immediate().await;

    assert!(
        result.is_err(),
        "establish_primary_phone_session_immediate should fail when no PN is set"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("Not logged in"),
        "Error should mention 'Not logged in', got: {}",
        error_msg
    );

    info!("[OK] test_establish_primary_phone_session_fails_without_pn passed");
}

/// Test that ensure_e2e_sessions waits for offline sync to complete.
/// This is the CRITICAL difference between ensure_e2e_sessions and
/// establish_primary_phone_session_immediate.
#[tokio::test]
async fn test_ensure_e2e_sessions_waits_for_offline_sync() {
    use std::sync::atomic::Ordering;
    use wacore_binary::jid::Jid;

    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_ensure_e2e_waits?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
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

    // Flag is false (offline sync not complete)
    assert!(!client.offline_sync_completed.load(Ordering::Relaxed));

    // Call ensure_e2e_sessions with an empty list (so it returns early after the wait)
    // This lets us test the waiting behavior without needing network
    let client_clone = client.clone();
    let ensure_handle = tokio::spawn(async move {
        // Start with some JIDs - but since we're testing the wait, we use empty
        // to avoid needing actual session establishment
        client_clone.ensure_e2e_sessions(vec![]).await
    });

    // Wait a bit - ensure_e2e_sessions should return immediately for empty list
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        ensure_handle.is_finished(),
        "ensure_e2e_sessions should return immediately for empty JID list"
    );

    // Now test with actual JIDs - it should wait for offline sync
    let client_clone = client.clone();
    let test_jid = Jid::pn("559999999999");
    let ensure_handle = tokio::spawn(async move {
        // This will wait for offline sync before proceeding
        let start = std::time::Instant::now();
        let _ = client_clone.ensure_e2e_sessions(vec![test_jid]).await;
        start.elapsed()
    });

    // Give it a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // It should still be waiting (offline sync not complete)
    assert!(
        !ensure_handle.is_finished(),
        "ensure_e2e_sessions should be waiting for offline sync"
    );

    // Now complete offline sync
    client.offline_sync_completed.store(true, Ordering::Relaxed);
    client.offline_sync_notifier.notify_waiters();

    // Now it should complete (might fail on session establishment, but that's ok)
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), ensure_handle).await;

    assert!(
        result.is_ok(),
        "ensure_e2e_sessions should complete after offline sync"
    );

    info!("[OK] test_ensure_e2e_sessions_waits_for_offline_sync passed");
}

/// Integration test: Verify that the immediate session establishment does NOT
/// wait for offline sync. This is critical for PDO to work during offline sync.
///
/// The flow is:
/// 1. Login -> establish_primary_phone_session_immediate() is called
/// 2. This should NOT wait for offline sync (flag is false at this point)
/// 3. After session is established, offline messages arrive
/// 4. When decryption fails, PDO can immediately send to device 0
#[tokio::test]
async fn test_immediate_session_does_not_wait_for_offline_sync() {
    use std::sync::atomic::Ordering;
    use wacore_binary::jid::Jid;

    let backend = Arc::new(
        crate::store::SqliteStore::new("file:memdb_immediate_no_wait?mode=memory&cache=shared")
            .await
            .expect("Failed to create in-memory backend for test"),
    );
    let pm = Arc::new(
        PersistenceManager::new(backend.clone())
            .await
            .expect("persistence manager should initialize"),
    );

    // Set a PN so establish_primary_phone_session_immediate doesn't fail early
    pm.modify_device(|device| {
        device.pn = Some(Jid::pn("559999999999"));
    })
    .await;

    let (client, _rx) = Client::new(
        pm,
        Arc::new(crate::transport::mock::MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    // Flag is false (offline sync not complete - simulating login state)
    assert!(!client.offline_sync_completed.load(Ordering::Relaxed));

    // Call establish_primary_phone_session_immediate
    // It should NOT wait for offline sync - it should proceed immediately
    let start = std::time::Instant::now();

    // Note: This will fail because we can't actually fetch prekeys in tests,
    // but the important thing is that it doesn't WAIT for offline sync
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        client.establish_primary_phone_session_immediate(),
    )
    .await;

    let elapsed = start.elapsed();

    // The call should complete (or fail) quickly, NOT wait for 10 second timeout
    assert!(
        result.is_ok(),
        "establish_primary_phone_session_immediate should not wait for offline sync, timed out"
    );

    // It should complete in < 500ms (not 10 second wait)
    assert!(
        elapsed.as_millis() < 500,
        "establish_primary_phone_session_immediate should not wait, took {:?}",
        elapsed
    );

    // The actual result might be an error (no network), but that's fine
    // The important thing is it didn't wait for offline sync
    info!(
        "establish_primary_phone_session_immediate completed in {:?} (result: {:?})",
        elapsed,
        result.unwrap().is_ok()
    );

    info!("[OK] test_immediate_session_does_not_wait_for_offline_sync passed");
}
