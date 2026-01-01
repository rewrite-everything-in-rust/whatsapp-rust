use crate::auth::session::{SessionError, SessionManager};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use wacore_binary::jid::Jid;

fn make_jid(user: &str) -> Jid {
    Jid::pn(user)
}

#[tokio::test]
async fn test_ensure_sessions_empty_list() {
    let manager = SessionManager::new();
    let result = manager
        .ensure_sessions(vec![], |_| false, |_| async { Ok(()) })
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_ensure_sessions_all_have_sessions() {
    let manager = SessionManager::new();
    let jids = vec![make_jid("123"), make_jid("456")];

    let result = manager
        .ensure_sessions(
            jids,
            |_| true, // All have sessions
            |_| async { panic!("Should not fetch") },
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_ensure_sessions_fetches_for_missing() {
    let manager = SessionManager::new();
    let jids = vec![make_jid("123"), make_jid("456")];
    let fetch_count = Arc::new(AtomicUsize::new(0));
    let fetch_count_clone = fetch_count.clone();

    let result = manager
        .ensure_sessions(
            jids,
            |_| false, // None have sessions
            move |batch| {
                let count = fetch_count_clone.clone();
                async move {
                    count.fetch_add(batch.len(), Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;

    assert!(result.is_ok());
    assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_concurrent_requests_deduplicated() {
    let manager = Arc::new(SessionManager::new());
    let fetch_count = Arc::new(AtomicUsize::new(0));

    // Spawn two concurrent ensure_sessions calls for the same JID
    let jid = make_jid("123");

    let manager1 = manager.clone();
    let manager2 = manager.clone();
    let fetch_count1 = fetch_count.clone();
    let fetch_count2 = fetch_count.clone();
    let jid1 = jid.clone();
    let jid2 = jid.clone();

    let handle1 = tokio::spawn(async move {
        manager1
            .ensure_sessions(
                vec![jid1],
                |_| false,
                move |batch| {
                    let count = fetch_count1.clone();
                    async move {
                        // Simulate some processing time
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        count.fetch_add(batch.len(), Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await
    });

    // Small delay to ensure the first call starts processing
    tokio::time::sleep(Duration::from_millis(10)).await;

    let handle2 = tokio::spawn(async move {
        manager2
            .ensure_sessions(
                vec![jid2],
                |_| false,
                move |batch| {
                    let count = fetch_count2.clone();
                    async move {
                        count.fetch_add(batch.len(), Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await
    });

    let (r1, r2) = tokio::join!(handle1, handle2);
    assert!(r1.unwrap().is_ok());
    assert!(r2.unwrap().is_ok());

    // Only one fetch should have happened due to deduplication
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_batching() {
    let manager = SessionManager::new();

    // Create more JIDs than the batch size
    let jids: Vec<Jid> = (0..75).map(|i| make_jid(&i.to_string())).collect();
    let batch_count = Arc::new(AtomicUsize::new(0));
    let batch_count_clone = batch_count.clone();

    let result = manager
        .ensure_sessions(
            jids,
            |_| false,
            move |_batch| {
                let count = batch_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;

    assert!(result.is_ok());
    // 75 JIDs should be processed in 2 batches (50 + 25)
    assert_eq!(batch_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_error_propagation() {
    let manager = SessionManager::new();
    let jids = vec![make_jid("123")];

    let result = manager
        .ensure_sessions(
            jids,
            |_| false,
            |_| async { Err(anyhow::anyhow!("fetch failed")) },
        )
        .await;

    assert!(result.is_err());
    match result {
        Err(SessionError::FetchFailed(msg)) => {
            assert!(msg.contains("fetch failed"));
        }
        _ => panic!("Expected FetchFailed error"),
    }
}
