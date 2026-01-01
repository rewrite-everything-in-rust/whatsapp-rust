use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::Client;
use crate::http::{HttpClient, HttpRequest, HttpResponse};
use crate::store::SqliteStore;
use crate::store::persistence_manager::PersistenceManager;
use crate::store::traits::{
    AppStateSyncKey, AppSyncStore, Backend, DeviceListRecord, DeviceStore, LidPnMappingEntry,
    ProtocolStore, SignalStore,
};
use crate::transport::mock::MockTransportFactory;
use wacore::appstate::hash::HashState;
use wacore::appstate::patch_decode::{PatchList, WAPatchName};
use wacore::appstate::processor::AppStateMutationMAC;
use wacore::store::Device;
use wacore::store::error::Result as StoreResult;
use waproto::whatsapp as wa;

#[derive(Debug, Clone, Default)]
pub struct MockHttpClient;

#[async_trait::async_trait]
impl HttpClient for MockHttpClient {
    async fn execute(&self, _request: HttpRequest) -> Result<HttpResponse, anyhow::Error> {
        Ok(HttpResponse {
            status_code: 200,
            body: Vec::new(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct FailingMockHttpClient;

#[async_trait::async_trait]
impl HttpClient for FailingMockHttpClient {
    async fn execute(&self, _request: HttpRequest) -> Result<HttpResponse, anyhow::Error> {
        Err(anyhow::anyhow!("Not implemented"))
    }
}

pub async fn create_test_client() -> Arc<Client> {
    create_test_client_with_name("default").await
}

pub async fn create_test_client_with_name(name: &str) -> Arc<Client> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let db_name = format!(
        "file:memdb_{}_{}_{}?mode=memory&cache=shared",
        name,
        unique_id,
        std::process::id()
    );

    let backend = Arc::new(
        SqliteStore::new(&db_name)
            .await
            .expect("test backend should initialize"),
    ) as Arc<dyn Backend>;

    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );

    let (client, _rx) = Client::new(
        pm,
        Arc::new(MockTransportFactory::new()),
        Arc::new(MockHttpClient),
        None,
    )
    .await;

    client
}

pub async fn create_test_client_with_failing_http(name: &str) -> Arc<Client> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let unique_id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let db_name = format!(
        "file:memdb_fail_{}_{}_{}?mode=memory&cache=shared",
        name,
        unique_id,
        std::process::id()
    );

    let backend = Arc::new(
        SqliteStore::new(&db_name)
            .await
            .expect("test backend should initialize"),
    ) as Arc<dyn Backend>;

    let pm = Arc::new(
        PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize"),
    );

    let (client, _rx) = Client::new(
        pm,
        Arc::new(MockTransportFactory::new()),
        Arc::new(FailingMockHttpClient),
        None,
    )
    .await;

    client
}

pub type MockMacMap = Arc<Mutex<HashMap<(String, Vec<u8>), Vec<u8>>>>;

#[derive(Default, Clone)]
pub struct MockBackend {
    pub versions: Arc<Mutex<HashMap<String, HashState>>>,
    pub macs: MockMacMap,
    pub keys: Arc<Mutex<HashMap<Vec<u8>, AppStateSyncKey>>>,
}

impl MockBackend {
    pub async fn insert_sync_key(&self, key_id: Vec<u8>, key: AppStateSyncKey) {
        self.keys.lock().await.insert(key_id, key);
    }
}

#[async_trait::async_trait]
impl SignalStore for MockBackend {
    async fn put_identity(&self, _: &str, _: [u8; 32]) -> StoreResult<()> {
        Ok(())
    }
    async fn load_identity(&self, _: &str) -> StoreResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn delete_identity(&self, _: &str) -> StoreResult<()> {
        Ok(())
    }
    async fn get_session(&self, _: &str) -> StoreResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn put_session(&self, _: &str, _: &[u8]) -> StoreResult<()> {
        Ok(())
    }
    async fn delete_session(&self, _: &str) -> StoreResult<()> {
        Ok(())
    }
    async fn store_prekey(&self, _: u32, _: &[u8], _: bool) -> StoreResult<()> {
        Ok(())
    }
    async fn load_prekey(&self, _: u32) -> StoreResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn remove_prekey(&self, _: u32) -> StoreResult<()> {
        Ok(())
    }
    async fn store_signed_prekey(&self, _: u32, _: &[u8]) -> StoreResult<()> {
        Ok(())
    }
    async fn load_signed_prekey(&self, _: u32) -> StoreResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn load_all_signed_prekeys(&self) -> StoreResult<Vec<(u32, Vec<u8>)>> {
        Ok(vec![])
    }
    async fn remove_signed_prekey(&self, _: u32) -> StoreResult<()> {
        Ok(())
    }
    async fn put_sender_key(&self, _: &str, _: &[u8]) -> StoreResult<()> {
        Ok(())
    }
    async fn get_sender_key(&self, _: &str) -> StoreResult<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn delete_sender_key(&self, _: &str) -> StoreResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AppSyncStore for MockBackend {
    async fn get_sync_key(&self, key_id: &[u8]) -> StoreResult<Option<AppStateSyncKey>> {
        Ok(self.keys.lock().await.get(key_id).cloned())
    }
    async fn set_sync_key(&self, key_id: &[u8], key: AppStateSyncKey) -> StoreResult<()> {
        self.keys.lock().await.insert(key_id.to_vec(), key);
        Ok(())
    }
    async fn get_version(&self, name: &str) -> StoreResult<HashState> {
        Ok(self
            .versions
            .lock()
            .await
            .get(name)
            .cloned()
            .unwrap_or_default())
    }
    async fn set_version(&self, name: &str, state: HashState) -> StoreResult<()> {
        self.versions.lock().await.insert(name.to_string(), state);
        Ok(())
    }
    async fn put_mutation_macs(
        &self,
        name: &str,
        _version: u64,
        mutations: &[AppStateMutationMAC],
    ) -> StoreResult<()> {
        let mut macs = self.macs.lock().await;
        for m in mutations {
            macs.insert((name.to_string(), m.index_mac.clone()), m.value_mac.clone());
        }
        Ok(())
    }
    async fn get_mutation_mac(
        &self,
        name: &str,
        index_mac: &[u8],
    ) -> StoreResult<Option<Vec<u8>>> {
        Ok(self
            .macs
            .lock()
            .await
            .get(&(name.to_string(), index_mac.to_vec()))
            .cloned())
    }
    async fn delete_mutation_macs(&self, _: &str, _: &[Vec<u8>]) -> StoreResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl ProtocolStore for MockBackend {
    async fn get_skdm_recipients(&self, _: &str) -> StoreResult<Vec<String>> {
        Ok(vec![])
    }
    async fn add_skdm_recipients(&self, _: &str, _: &[String]) -> StoreResult<()> {
        Ok(())
    }
    async fn clear_skdm_recipients(&self, _: &str) -> StoreResult<()> {
        Ok(())
    }
    async fn get_lid_mapping(&self, _: &str) -> StoreResult<Option<LidPnMappingEntry>> {
        Ok(None)
    }
    async fn get_pn_mapping(&self, _: &str) -> StoreResult<Option<LidPnMappingEntry>> {
        Ok(None)
    }
    async fn put_lid_mapping(&self, _: &LidPnMappingEntry) -> StoreResult<()> {
        Ok(())
    }
    async fn get_all_lid_mappings(&self) -> StoreResult<Vec<LidPnMappingEntry>> {
        Ok(vec![])
    }
    async fn save_base_key(&self, _: &str, _: &str, _: &[u8]) -> StoreResult<()> {
        Ok(())
    }
    async fn has_same_base_key(&self, _: &str, _: &str, _: &[u8]) -> StoreResult<bool> {
        Ok(false)
    }
    async fn delete_base_key(&self, _: &str, _: &str) -> StoreResult<()> {
        Ok(())
    }
    async fn update_device_list(&self, _: DeviceListRecord) -> StoreResult<()> {
        Ok(())
    }
    async fn get_devices(&self, _: &str) -> StoreResult<Option<DeviceListRecord>> {
        Ok(None)
    }
    async fn mark_forget_sender_key(&self, _: &str, _: &str) -> StoreResult<()> {
        Ok(())
    }
    async fn consume_forget_marks(&self, _: &str) -> StoreResult<Vec<String>> {
        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl DeviceStore for MockBackend {
    async fn save(&self, _: &Device) -> StoreResult<()> {
        Ok(())
    }
    async fn load(&self) -> StoreResult<Option<Device>> {
        Ok(Some(Device::new()))
    }
    async fn exists(&self) -> StoreResult<bool> {
        Ok(true)
    }
    async fn create(&self) -> StoreResult<i32> {
        Ok(1)
    }
}

pub fn build_patch_list(keys: &[Vec<u8>]) -> PatchList {
    let snapshot = keys.first().map(|k| wa::SyncdSnapshot {
        version: None,
        records: Vec::new(),
        mac: None,
        key_id: Some(wa::KeyId {
            id: Some(k.clone()),
        }),
    });

    let patches = keys
        .iter()
        .skip(1)
        .map(|k| wa::SyncdPatch {
            version: None,
            mutations: Vec::new(),
            external_mutations: None,
            snapshot_mac: None,
            patch_mac: None,
            key_id: Some(wa::KeyId {
                id: Some(k.clone()),
            }),
            exit_code: None,
            device_index: None,
            client_debug_data: None,
        })
        .collect();

    PatchList {
        name: WAPatchName::Regular,
        has_more_patches: false,
        patches,
        snapshot,
        snapshot_ref: None,
    }
}

pub fn create_encrypted_mutation(
    op: wa::syncd_mutation::SyncdOperation,
    index_mac: &[u8],
    plaintext: &[u8],
    keys: &wacore::appstate::keys::ExpandedAppStateKeys,
    key_id_bytes: &[u8],
) -> wa::SyncdMutation {
    use wacore::appstate::hash::generate_content_mac;
    use wacore::libsignal::crypto::aes_256_cbc_encrypt_into;

    let iv = vec![0u8; 16];

    let mut ciphertext = Vec::new();
    aes_256_cbc_encrypt_into(plaintext, &keys.value_encryption, &iv, &mut ciphertext)
        .expect("AES-CBC encryption should succeed with valid inputs");
    let mut value_with_iv = iv;
    value_with_iv.extend_from_slice(&ciphertext);
    let value_mac = generate_content_mac(op, &value_with_iv, key_id_bytes, &keys.value_mac);
    let mut value_blob = value_with_iv;
    value_blob.extend_from_slice(&value_mac);

    wa::SyncdMutation {
        operation: Some(op as i32),
        record: Some(wa::SyncdRecord {
            index: Some(wa::SyncdIndex {
                blob: Some(index_mac.to_vec()),
            }),
            value: Some(wa::SyncdValue {
                blob: Some(value_blob),
            }),
            key_id: Some(wa::KeyId {
                id: Some(key_id_bytes.to_vec()),
            }),
        }),
    }
}
