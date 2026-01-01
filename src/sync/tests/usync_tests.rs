use crate::test_utils::create_test_client;
use wacore_binary::jid::Jid;

#[tokio::test]
async fn test_device_cache_hit() {
    let client = create_test_client().await;

    let test_jid: Jid = "1234567890@s.whatsapp.net"
        .parse()
        .expect("test JID should be valid");
    let device_jid: Jid = "1234567890:1@s.whatsapp.net"
        .parse()
        .expect("test device JID should be valid");

    // Manually insert into cache
    client
        .get_device_cache()
        .await
        .insert(test_jid.clone(), vec![device_jid.clone()])
        .await;

    // Verify cache hit
    let cached = client.get_device_cache().await.get(&test_jid).await;
    assert!(cached.is_some());
    let cached_devices = cached.expect("cache should have entry");
    assert_eq!(cached_devices.len(), 1);
    assert_eq!(cached_devices[0], device_jid);
}

#[tokio::test]
async fn test_cache_size_eviction() {
    use moka::future::Cache;

    // Create a small cache
    let cache: Cache<i32, String> = Cache::builder().max_capacity(2).build();

    // Insert 3 items
    cache.insert(1, "one".to_string()).await;
    cache.insert(2, "two".to_string()).await;
    cache.insert(3, "three".to_string()).await;

    // Give time for eviction to occur
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // The cache should have at most 2 items
    let count = cache.entry_count();
    assert!(
        count <= 2,
        "Cache should have at most 2 items, has {}",
        count
    );
}
