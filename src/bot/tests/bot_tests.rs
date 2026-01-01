use crate::bot::Bot;
use anyhow::Result;
use std::sync::Arc;
use wacore::net::{HttpClient, HttpRequest, HttpResponse};
use wacore::store::traits::Backend;
use waproto::whatsapp::device_props;
use whatsapp_rust_sqlite_storage::SqliteStore;
use whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory;

// Mock HTTP client for testing
#[derive(Debug, Clone)]
struct MockHttpClient;

#[async_trait::async_trait]
impl HttpClient for MockHttpClient {
    async fn execute(&self, _request: HttpRequest) -> Result<HttpResponse> {
        // Return a mock response for version fetching
        Ok(HttpResponse {
                status_code: 200,
                body: br#"self.__swData=JSON.parse(/*BTDS*/"{\"dynamic_data\":{\"SiteData\":{\"server_revision\":1026131876,\"client_revision\":1026131876}}}");"#.to_vec(),
            })
    }
}

async fn create_test_sqlite_backend() -> Arc<dyn Backend> {
    let temp_db = format!(
        "file:memdb_bot_{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4()
    );
    Arc::new(
        SqliteStore::new(&temp_db)
            .await
            .expect("Failed to create test SqliteStore"),
    ) as Arc<dyn Backend>
}

async fn create_test_sqlite_backend_for_device(device_id: i32) -> Arc<dyn Backend> {
    let temp_db = format!(
        "file:memdb_bot_{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4()
    );
    Arc::new(
        SqliteStore::new_for_device(&temp_db, device_id)
            .await
            .expect("Failed to create test SqliteStore"),
    ) as Arc<dyn Backend>
}

#[tokio::test]
async fn test_bot_builder_single_device() {
    let backend = create_test_sqlite_backend().await;
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;

    let bot = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport)
        .with_http_client(http_client)
        .build()
        .await
        .expect("Failed to build bot");

    // Verify bot was created successfully
    let _client = bot.client();
}

#[tokio::test]
async fn test_bot_builder_multi_device() {
    // Create a backend configured for device ID 42
    let backend = create_test_sqlite_backend_for_device(42).await;
    let transport = TokioWebSocketTransportFactory::new();

    let bot = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport)
        .with_http_client(MockHttpClient)
        .build()
        .await
        .expect("Failed to build bot");

    // Verify bot was created successfully
    let _client = bot.client();
}

#[tokio::test]
async fn test_bot_builder_with_custom_backend() {
    // Create an in-memory backend for testing
    let backend = create_test_sqlite_backend().await;
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;
    let bot = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport)
        .with_http_client(http_client)
        .build()
        .await
        .expect("Failed to build bot with custom backend");

    // Verify the bot was created successfully
    let _client = bot.client();
}

#[tokio::test]
async fn test_bot_builder_with_custom_backend_specific_device() {
    // Create a backend configured for device ID 100
    let backend = create_test_sqlite_backend_for_device(100).await;
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;

    // Build a bot with the custom backend
    let bot = Bot::builder()
        .with_backend(backend)
        .with_http_client(http_client)
        .with_transport_factory(transport)
        .build()
        .await
        .expect("Failed to build bot with custom backend for specific device");

    // Verify the bot was created successfully
    let _client = bot.client();
}

#[tokio::test]
async fn test_bot_builder_missing_backend() {
    // Try to build without setting a backend
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;
    let result = Bot::builder()
        .with_transport_factory(transport)
        .with_http_client(http_client)
        .build()
        .await;

    // This should fail
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Backend is required")
    );
}

#[tokio::test]
async fn test_bot_builder_missing_transport() {
    // Try to build without setting a transport
    let backend = create_test_sqlite_backend().await;
    let http_client = MockHttpClient;
    let result = Bot::builder()
        .with_backend(backend)
        .with_http_client(http_client)
        .build()
        .await;

    // This should fail
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Transport factory is required")
    );
}

#[tokio::test]
async fn test_bot_builder_with_version_override() {
    let backend = create_test_sqlite_backend().await;
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;

    let bot = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport)
        .with_http_client(http_client)
        .with_version((2, 3000, 123456789))
        .build()
        .await
        .expect("Failed to build bot with version override");

    // Verify the bot was created successfully
    let client = bot.client();

    // Check that the override version is stored in the client
    assert_eq!(client.override_version, Some((2, 3000, 123456789)));
}

#[tokio::test]
async fn test_bot_builder_with_os_info_override() {
    let backend = create_test_sqlite_backend().await;
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;

    let custom_os = "CustomOS".to_string();
    let custom_version = device_props::AppVersion {
        primary: Some(99),
        secondary: Some(88),
        tertiary: Some(77),
        ..Default::default()
    };

    let bot = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport)
        .with_http_client(http_client)
        .with_os_info(Some(custom_os.clone()), Some(custom_version))
        .build()
        .await
        .expect("Failed to build bot with OS info override");

    let client = bot.client();
    let persistence_manager = client.persistence_manager();
    let device = persistence_manager.get_device_snapshot().await;

    // Verify the OS info was overridden
    assert_eq!(device.device_props.os, Some(custom_os));
    assert_eq!(device.device_props.version, Some(custom_version));
}

#[tokio::test]
async fn test_bot_builder_with_os_only_override() {
    let backend = create_test_sqlite_backend().await;
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;

    let custom_os = "CustomOS".to_string();

    let bot = Bot::builder()
        .with_backend(backend)
        .with_transport_factory(transport)
        .with_http_client(http_client)
        .with_os_info(Some(custom_os.clone()), None)
        .build()
        .await
        .expect("Failed to build bot with OS only override");

    let client = bot.client();
    let persistence_manager = client.persistence_manager();
    let device = persistence_manager.get_device_snapshot().await;

    // Verify only OS was overridden, version should be default
    assert_eq!(device.device_props.os, Some(custom_os));
    // Version should be the default since we didn't override it
    assert_eq!(
        device.device_props.version,
        Some(wacore::store::Device::default_device_props_version())
    );
}

#[tokio::test]
async fn test_bot_builder_with_version_only_override() {
    let backend = create_test_sqlite_backend().await;
    let transport = TokioWebSocketTransportFactory::new();
    let http_client = MockHttpClient;

    let custom_version = device_props::AppVersion {
        primary: Some(99),
        secondary: Some(88),
        tertiary: Some(77),
        ..Default::default()
    };

    let bot = Bot::builder()
        .with_backend(backend)
        .with_http_client(http_client)
        .with_transport_factory(transport)
        .with_os_info(None, Some(custom_version))
        .build()
        .await
        .expect("Failed to build bot with version only override");

    let client = bot.client();
    let persistence_manager = client.persistence_manager();
    let device = persistence_manager.get_device_snapshot().await;

    // Verify only version was overridden, OS should be default ("rust")
    assert_eq!(device.device_props.version, Some(custom_version));
    // OS should be the default since we didn't override it
    assert_eq!(
        device.device_props.os,
        Some(wacore::store::Device::default_os().to_string())
    );
}
