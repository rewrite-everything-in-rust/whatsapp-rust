use std::sync::Arc;

use crate::socket::noise_socket::NoiseSocket;
use wacore::aes_gcm::aead::Aead;
use wacore::aes_gcm::{Aes256Gcm, KeyInit};
use wacore::handshake::utils::generate_iv;

#[tokio::test]
async fn test_encrypt_and_send_returns_both_buffers() {
    // Create a mock transport
    let transport = Arc::new(crate::transport::mock::MockTransport);

    // Create dummy keys for testing
    let key = [0u8; 32];
    let write_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");
    let read_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");

    let socket = NoiseSocket::new(transport, write_key, read_key);

    let plaintext_buf = Vec::with_capacity(1024);
    let encrypted_buf = Vec::with_capacity(1024);
    let plaintext_capacity = plaintext_buf.capacity();

    let result = socket.encrypt_and_send(plaintext_buf, encrypted_buf).await;
    assert!(result.is_ok(), "encrypt_and_send should succeed");

    let (returned_plaintext, returned_encrypted) = result.unwrap();

    assert_eq!(
        returned_plaintext.capacity(),
        plaintext_capacity,
        "Plaintext buffer should maintain its capacity"
    );
    assert!(
        returned_encrypted.is_empty(),
        "Encrypted buffer is moved to transport (zero-copy)"
    );
    assert!(
        returned_plaintext.is_empty(),
        "Returned plaintext buffer should be cleared"
    );
}

#[tokio::test]
async fn test_concurrent_sends_maintain_order() {
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // Create a mock transport that records the order of sends by decrypting
    // the first byte (which contains the task index)
    struct RecordingTransport {
        recorded_order: Arc<Mutex<Vec<u8>>>,
        read_key: Aes256Gcm,
        counter: std::sync::atomic::AtomicU32,
    }

    #[async_trait]
    impl crate::transport::Transport for RecordingTransport {
        async fn send(&self, data: Vec<u8>) -> std::result::Result<(), anyhow::Error> {
            // Decrypt the data to extract the index (first byte of plaintext)
            if data.len() > 16 {
                // Skip the noise frame header (3 bytes for length)
                let ciphertext = &data[3..];
                let counter = self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let iv = generate_iv(counter);

                if let Ok(plaintext) = self.read_key.decrypt(iv.as_ref().into(), ciphertext)
                    && !plaintext.is_empty()
                {
                    let index = plaintext[0];
                    let mut order = self.recorded_order.lock().await;
                    order.push(index);
                }
            }
            Ok(())
        }

        async fn disconnect(&self) {}
    }

    let recorded_order = Arc::new(Mutex::new(Vec::new()));
    let key = [0u8; 32];
    let write_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");
    let read_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");

    let transport = Arc::new(RecordingTransport {
        recorded_order: recorded_order.clone(),
        read_key: Aes256Gcm::new_from_slice(&key)
            .expect("32-byte key should be valid for AES-256-GCM"),
        counter: std::sync::atomic::AtomicU32::new(0),
    });

    let socket = Arc::new(NoiseSocket::new(transport, write_key, read_key));

    // Spawn multiple concurrent sends with their indices
    let mut handles = Vec::new();
    for i in 0..10 {
        let socket = socket.clone();
        handles.push(tokio::spawn(async move {
            // Use index as the first byte of plaintext to identify this send
            let mut plaintext = vec![i as u8];
            plaintext.extend_from_slice(&[0u8; 99]);
            let out_buf = Vec::with_capacity(256);
            socket.encrypt_and_send(plaintext, out_buf).await
        }));
    }

    // Wait for all sends to complete
    for handle in handles {
        let result = handle.await.expect("task should complete");
        assert!(result.is_ok(), "All sends should succeed");
    }

    // Verify all sends completed in FIFO order (0, 1, 2, ..., 9)
    let order = recorded_order.lock().await;
    let expected: Vec<u8> = (0..10).collect();
    assert_eq!(*order, expected, "Sends should maintain FIFO order");
}

/// Tests that the encrypted buffer sizing formula (plaintext.len() + 32) is sufficient.
/// This verifies the optimization in client.rs that sizes the buffer based on payload.
#[tokio::test]
async fn test_encrypted_buffer_sizing_is_sufficient() {
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Transport that records the actual encrypted data size
    struct SizeRecordingTransport {
        last_size: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl crate::transport::Transport for SizeRecordingTransport {
        async fn send(&self, data: Vec<u8>) -> std::result::Result<(), anyhow::Error> {
            self.last_size.store(data.len(), Ordering::SeqCst);
            Ok(())
        }
        async fn disconnect(&self) {}
    }

    let last_size = Arc::new(AtomicUsize::new(0));
    let transport = Arc::new(SizeRecordingTransport {
        last_size: last_size.clone(),
    });

    let key = [0u8; 32];
    let write_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");
    let read_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");

    let socket = NoiseSocket::new(transport, write_key, read_key);

    // Test various payload sizes: tiny, small, medium, large, very large
    let test_sizes = [0, 1, 50, 100, 500, 1000, 1024, 2000, 5000, 16384, 20000];

    for size in test_sizes {
        let plaintext = vec![0xABu8; size];
        // This is the formula used in client.rs
        let buffer_capacity = plaintext.len() + 32;
        let encrypted_buf = Vec::with_capacity(buffer_capacity);

        let result = socket
            .encrypt_and_send(plaintext.clone(), encrypted_buf)
            .await;

        assert!(
            result.is_ok(),
            "encrypt_and_send should succeed for payload size {}",
            size
        );

        let actual_encrypted_size = last_size.load(Ordering::SeqCst);

        // Verify the actual encrypted size fits within our allocated capacity
        // Encrypted size = plaintext + 16 (AES-GCM tag) + 3 (frame header) = plaintext + 19
        let expected_max = size + 19;
        assert_eq!(
            actual_encrypted_size, expected_max,
            "Encrypted size for {} byte payload should be {} (got {})",
            size, expected_max, actual_encrypted_size
        );

        // Verify our buffer sizing formula provides enough capacity
        assert!(
            buffer_capacity >= actual_encrypted_size,
            "Buffer capacity {} should be >= encrypted size {} for payload size {}",
            buffer_capacity,
            actual_encrypted_size,
            size
        );
    }
}

/// Tests edge cases for buffer sizing
#[tokio::test]
async fn test_encrypted_buffer_sizing_edge_cases() {
    use async_trait::async_trait;
    use std::sync::Arc;

    struct NoOpTransport;

    #[async_trait]
    impl crate::transport::Transport for NoOpTransport {
        async fn send(&self, _data: Vec<u8>) -> std::result::Result<(), anyhow::Error> {
            Ok(())
        }
        async fn disconnect(&self) {}
    }

    let transport = Arc::new(NoOpTransport);
    let key = [0u8; 32];
    let write_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");
    let read_key =
        Aes256Gcm::new_from_slice(&key).expect("32-byte key should be valid for AES-256-GCM");

    let socket = NoiseSocket::new(transport, write_key, read_key);

    // Test empty payload
    let result = socket
        .encrypt_and_send(vec![], Vec::with_capacity(32))
        .await;
    assert!(result.is_ok(), "Empty payload should encrypt successfully");

    // Test payload at inline threshold boundary (16KB)
    let at_threshold = vec![0u8; 16 * 1024];
    let result = socket
        .encrypt_and_send(at_threshold, Vec::with_capacity(16 * 1024 + 32))
        .await;
    assert!(
        result.is_ok(),
        "Payload at inline threshold should encrypt successfully"
    );

    // Test payload just above inline threshold
    let above_threshold = vec![0u8; 16 * 1024 + 1];
    let result = socket
        .encrypt_and_send(above_threshold, Vec::with_capacity(16 * 1024 + 33))
        .await;
    assert!(
        result.is_ok(),
        "Payload above inline threshold should encrypt successfully"
    );
}
