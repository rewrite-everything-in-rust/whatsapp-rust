use crate::client::Client;
use crate::lid_pn_cache::LearningSource;
use crate::test_utils::create_test_client_with_failing_http;
use std::sync::Arc;

async fn create_test_client() -> Arc<Client> {
    create_test_client_with_failing_http("device_registry").await
}

#[tokio::test]
async fn test_resolve_to_canonical_key_unknown_user() {
    let client = create_test_client().await;
    let result = client.resolve_to_canonical_key("15551234567").await;
    assert_eq!(result, "15551234567");
}

#[tokio::test]
async fn test_resolve_to_canonical_key_with_lid_mapping() {
    use crate::lid_pn_cache::LidPnEntry;

    let client = create_test_client().await;
    let lid = "100000000000001";
    let pn = "15551234567";

    // Add directly to cache (avoids persistence layer which needs DB tables)
    let entry = LidPnEntry::new(lid.to_string(), pn.to_string(), LearningSource::Usync);
    client.lid_pn_cache.add(entry).await;

    // PN should resolve to LID
    let result = client.resolve_to_canonical_key(pn).await;
    assert_eq!(result, lid);

    // LID should stay as LID
    let result = client.resolve_to_canonical_key(lid).await;
    assert_eq!(result, lid);
}

#[tokio::test]
async fn test_get_lookup_keys_unknown_user() {
    let client = create_test_client().await;
    let keys = client.get_lookup_keys("15551234567").await;
    assert_eq!(keys, vec!["15551234567"]);
}

#[tokio::test]
async fn test_get_lookup_keys_with_lid_mapping() {
    use crate::lid_pn_cache::LidPnEntry;

    let client = create_test_client().await;
    let lid = "100000000000001";
    let pn = "15551234567";

    // Add directly to cache (avoids persistence layer which needs DB tables)
    let entry = LidPnEntry::new(lid.to_string(), pn.to_string(), LearningSource::Usync);
    client.lid_pn_cache.add(entry).await;

    // Looking up by PN should return [LID, PN]
    let keys = client.get_lookup_keys(pn).await;
    assert_eq!(keys, vec![lid.to_string(), pn.to_string()]);

    // Looking up by LID should return [LID, PN]
    let keys = client.get_lookup_keys(lid).await;
    assert_eq!(keys, vec![lid.to_string(), pn.to_string()]);
}

#[tokio::test]
async fn test_15_digit_lid_handling() {
    use crate::lid_pn_cache::LidPnEntry;

    let client = create_test_client().await;
    // Real example: 15-digit LID
    let lid = "100000000000001";
    let pn = "15551234567";

    assert_eq!(lid.len(), 15, "LID should be 15 digits");

    // Add directly to cache (avoids persistence layer which needs DB tables)
    let entry = LidPnEntry::new(lid.to_string(), pn.to_string(), LearningSource::Usync);
    client.lid_pn_cache.add(entry).await;

    // 15-digit LID should be properly recognized via cache lookup
    let canonical = client.resolve_to_canonical_key(lid).await;
    assert_eq!(canonical, lid);

    let keys = client.get_lookup_keys(lid).await;
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], lid);
    assert_eq!(keys[1], pn);
}

#[tokio::test]
async fn test_has_device_primary_always_exists() {
    let client = create_test_client().await;
    assert!(client.has_device("anyuser", 0).await);
}

#[tokio::test]
async fn test_has_device_unknown_device() {
    let client = create_test_client().await;
    assert!(!client.has_device("15551234567", 5).await);
}

#[tokio::test]
async fn test_has_device_with_cached_record() {
    use crate::lid_pn_cache::LidPnEntry;

    let client = create_test_client().await;
    let lid = "100000000000001";
    let pn = "15551234567";

    // Add directly to cache (avoids persistence layer which needs DB tables)
    let entry = LidPnEntry::new(lid.to_string(), pn.to_string(), LearningSource::Usync);
    client.lid_pn_cache.add(entry).await;

    // Manually insert into cache to test lookup logic
    let record = wacore::store::traits::DeviceListRecord {
        user: lid.to_string(),
        devices: vec![wacore::store::traits::DeviceInfo {
            device_id: 1,
            key_index: None,
        }],
        timestamp: 12345,
        phash: None,
    };
    client
        .device_registry_cache
        .insert(lid.to_string(), record)
        .await;

    // Device should be findable via both PN and LID (bidirectional lookup)
    assert!(client.has_device(pn, 1).await);
    assert!(client.has_device(lid, 1).await);
    // Non-existent device should return false
    assert!(!client.has_device(lid, 99).await);
}

/// Test that invalidate_device_cache uses correctly-typed JIDs.
///
/// This test prevents a regression where the code was using both
/// Jid::pn(user) and Jid::lid(user) on the raw user string, which
/// creates invalid JIDs (e.g., "15551234567@lid" for a phone number).
///
/// The fix uses the lid_pn_cache to determine the correct Jid type
/// for each lookup key.
#[tokio::test]
async fn test_invalidate_device_cache_uses_correct_jid_types() {
    use crate::lid_pn_cache::LidPnEntry;
    use wacore_binary::jid::Jid;

    let client = create_test_client().await;
    let lid = "100000000000001";
    let pn = "15551234567";

    // Set up LID-to-PN mapping
    let entry = LidPnEntry::new(lid.to_string(), pn.to_string(), LearningSource::Usync);
    client.lid_pn_cache.add(entry).await;

    // Insert device registry record
    let record = wacore::store::traits::DeviceListRecord {
        user: lid.to_string(),
        devices: vec![wacore::store::traits::DeviceInfo {
            device_id: 1,
            key_index: None,
        }],
        timestamp: 12345,
        phash: None,
    };
    client
        .device_registry_cache
        .insert(lid.to_string(), record)
        .await;

    // Insert into device cache using correctly-typed JIDs
    let lid_jid = Jid::lid(lid);
    let pn_jid = Jid::pn(pn);

    // Simulate devices being cached under both JID types
    let device_cache = client.get_device_cache().await;
    device_cache
        .insert(lid_jid.clone(), vec![lid_jid.clone()])
        .await;
    device_cache
        .insert(pn_jid.clone(), vec![pn_jid.clone()])
        .await;

    // Verify cache entries exist before invalidation
    assert!(
        client.device_registry_cache.get(lid).await.is_some(),
        "Device registry cache should have LID entry before invalidation"
    );
    assert!(
        device_cache.get(&lid_jid).await.is_some(),
        "Device cache should have LID JID entry before invalidation"
    );
    assert!(
        device_cache.get(&pn_jid).await.is_some(),
        "Device cache should have PN JID entry before invalidation"
    );

    // Call invalidate_device_cache with the phone number (tests PN -> LID resolution)
    client.invalidate_device_cache(pn).await;

    // Verify all caches are properly invalidated
    assert!(
        client.device_registry_cache.get(lid).await.is_none(),
        "Device registry cache should be invalidated for LID"
    );
    assert!(
        device_cache.get(&lid_jid).await.is_none(),
        "Device cache should be invalidated for LID JID"
    );
    assert!(
        device_cache.get(&pn_jid).await.is_none(),
        "Device cache should be invalidated for PN JID"
    );

    // Also test invalidation when called with LID directly
    // Re-insert entries
    let record2 = wacore::store::traits::DeviceListRecord {
        user: lid.to_string(),
        devices: vec![wacore::store::traits::DeviceInfo {
            device_id: 2,
            key_index: None,
        }],
        timestamp: 12346,
        phash: None,
    };
    client
        .device_registry_cache
        .insert(lid.to_string(), record2)
        .await;
    device_cache
        .insert(lid_jid.clone(), vec![lid_jid.clone()])
        .await;
    device_cache
        .insert(pn_jid.clone(), vec![pn_jid.clone()])
        .await;

    // Call invalidate_device_cache with the LID
    client.invalidate_device_cache(lid).await;

    // Verify all caches are properly invalidated
    assert!(
        client.device_registry_cache.get(lid).await.is_none(),
        "Device registry cache should be invalidated for LID (called with LID)"
    );
    assert!(
        device_cache.get(&lid_jid).await.is_none(),
        "Device cache should be invalidated for LID JID (called with LID)"
    );
    assert!(
        device_cache.get(&pn_jid).await.is_none(),
        "Device cache should be invalidated for PN JID (called with LID)"
    );
}

/// Test that invalidate_device_cache handles unknown users correctly.
///
/// When a user has no LID-PN mapping, we don't know if it's a LID or PN.
/// The fix invalidates BOTH types to ensure we clean up regardless.
#[tokio::test]
async fn test_invalidate_device_cache_unknown_user_invalidates_both_types() {
    use wacore_binary::jid::Jid;

    let client = create_test_client().await;
    // This user has NO LID-PN mapping in the cache
    let unknown_user = "100000000000999";

    // Create both possible JID types
    let lid_jid = Jid::lid(unknown_user);
    let pn_jid = Jid::pn(unknown_user);

    // Simulate devices being cached under the LID type
    // (this could happen if we queried usync with an @lid JID)
    let device_cache = client.get_device_cache().await;
    device_cache
        .insert(lid_jid.clone(), vec![lid_jid.clone()])
        .await;

    // Verify cache entry exists
    assert!(
        device_cache.get(&lid_jid).await.is_some(),
        "Device cache should have LID JID entry before invalidation"
    );

    // Call invalidate_device_cache with the unknown user
    client.invalidate_device_cache(unknown_user).await;

    // Verify BOTH types are invalidated (even though only LID was cached)
    assert!(
        device_cache.get(&lid_jid).await.is_none(),
        "Device cache should be invalidated for LID JID (unknown user)"
    );
    assert!(
        device_cache.get(&pn_jid).await.is_none(),
        "Device cache should be invalidated for PN JID (unknown user)"
    );

    // Test the reverse case: PN cached but we don't know the type
    let unknown_user2 = "15559998888";
    let lid_jid2 = Jid::lid(unknown_user2);
    let pn_jid2 = Jid::pn(unknown_user2);

    // Simulate devices being cached under the PN type
    device_cache
        .insert(pn_jid2.clone(), vec![pn_jid2.clone()])
        .await;

    assert!(
        device_cache.get(&pn_jid2).await.is_some(),
        "Device cache should have PN JID entry before invalidation"
    );

    client.invalidate_device_cache(unknown_user2).await;

    // Verify BOTH types are invalidated
    assert!(
        device_cache.get(&lid_jid2).await.is_none(),
        "Device cache should be invalidated for LID JID (unknown PN user)"
    );
    assert!(
        device_cache.get(&pn_jid2).await.is_none(),
        "Device cache should be invalidated for PN JID (unknown PN user)"
    );
}
