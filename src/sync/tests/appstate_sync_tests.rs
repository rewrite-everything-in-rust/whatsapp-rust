use super::super::appstate_sync::AppStateProcessor;
use crate::store::traits::{AppStateSyncKey, AppSyncStore, Backend};
use crate::test_utils::{MockBackend, build_patch_list, create_encrypted_mutation};
use prost::Message;
use std::sync::Arc;
use wacore::appstate::WAPATCH_INTEGRITY;
use wacore::appstate::hash::HashState;
use wacore::appstate::keys::expand_app_state_keys;
use wacore::appstate::patch_decode::{PatchList, WAPatchName};
use wacore::appstate::processor::AppStateMutationMAC;
use waproto::whatsapp as wa;

#[tokio::test]
async fn test_get_missing_key_ids_reports_all_keys() {
    let backend = Arc::new(MockBackend::default());
    let backend_trait: Arc<dyn Backend> = backend.clone();
    let processor = AppStateProcessor::new(backend_trait);

    let key_a = vec![1, 2, 3];
    let key_b = vec![4, 5, 6];
    let patch_list = build_patch_list(&[key_a.clone(), key_b.clone()]);

    let missing = processor
        .get_missing_key_ids(&patch_list)
        .await
        .expect("missing key lookup should succeed");

    assert_eq!(missing, vec![key_a, key_b]);
}

#[tokio::test]
async fn test_get_missing_key_ids_skips_cached_keys() {
    let backend = Arc::new(MockBackend::default());
    backend
        .insert_sync_key(vec![1, 2, 3], AppStateSyncKey::default())
        .await;

    let backend_trait: Arc<dyn Backend> = backend.clone();
    let processor = AppStateProcessor::new(backend_trait);

    let key_existing = vec![1, 2, 3];
    let key_missing = vec![7, 8, 9];
    let patch_list = build_patch_list(&[key_existing.clone(), key_missing.clone()]);

    let missing = processor
        .get_missing_key_ids(&patch_list)
        .await
        .expect("missing key lookup should succeed");

    assert_eq!(missing, vec![key_missing]);
}

#[tokio::test]
async fn test_process_patch_list_handles_set_overwrite_correctly() {
    let backend = Arc::new(MockBackend::default());
    let backend_trait: Arc<dyn Backend> = backend.clone();
    let processor = AppStateProcessor::new(backend_trait);
    let collection_name = WAPatchName::Regular;
    let index_mac = vec![1; 32];
    let key_id_bytes = b"test_key_id".to_vec();
    let master_key = [7u8; 32];
    let keys = expand_app_state_keys(&master_key);

    let sync_key = AppStateSyncKey {
        key_data: master_key.to_vec(),
        ..Default::default()
    };
    backend
        .set_sync_key(&key_id_bytes, sync_key)
        .await
        .expect("test backend should accept sync key");

    let original_plaintext = wa::SyncActionData {
        value: Some(wa::SyncActionValue {
            timestamp: Some(1000),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let original_mutation = create_encrypted_mutation(
        wa::syncd_mutation::SyncdOperation::Set,
        &index_mac,
        &original_plaintext,
        &keys,
        &key_id_bytes,
    );

    let mut initial_state = HashState {
        version: 1,
        ..Default::default()
    };
    let (warnings, res) =
        initial_state.update_hash(std::slice::from_ref(&original_mutation), |_, _| Ok(None));
    assert!(res.is_ok() && warnings.is_empty());
    backend
        .set_version(collection_name.as_str(), initial_state.clone())
        .await
        .expect("test backend should accept app state version");

    let original_value_blob = original_mutation
        .record
        .as_ref()
        .expect("mutation should have record")
        .value
        .as_ref()
        .expect("record should have value")
        .blob
        .as_ref()
        .expect("value should have blob");
    let original_value_mac = original_value_blob[original_value_blob.len() - 32..].to_vec();
    backend
        .put_mutation_macs(
            collection_name.as_str(),
            1,
            &[AppStateMutationMAC {
                index_mac: index_mac.clone(),
                value_mac: original_value_mac.clone(),
            }],
        )
        .await
        .expect("test backend should accept mutation MACs");

    let new_plaintext = wa::SyncActionData {
        value: Some(wa::SyncActionValue {
            timestamp: Some(2000),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let overwrite_mutation = create_encrypted_mutation(
        wa::syncd_mutation::SyncdOperation::Set,
        &index_mac,
        &new_plaintext,
        &keys,
        &key_id_bytes,
    );

    let patch_list = PatchList {
        name: collection_name,
        has_more_patches: false,
        patches: vec![wa::SyncdPatch {
            mutations: vec![overwrite_mutation.clone()],
            version: Some(wa::SyncdVersion { version: Some(2) }),
            key_id: Some(wa::KeyId {
                id: Some(key_id_bytes),
            }),
            ..Default::default()
        }],
        snapshot: None,
        snapshot_ref: None,
    };

    let result = processor.process_patch_list(patch_list, false).await;

    assert!(
        result.is_ok(),
        "Processing the patch should succeed, but it failed: {:?}",
        result.err()
    );
    let (_, final_state, _) = result.expect("process_patch_list should succeed");

    let mut expected_state = initial_state.clone();
    let new_value_blob = overwrite_mutation
        .record
        .as_ref()
        .expect("mutation should have record")
        .value
        .as_ref()
        .expect("record should have value")
        .blob
        .as_ref()
        .expect("value should have blob");
    let new_value_mac = new_value_blob[new_value_blob.len() - 32..].to_vec();

    WAPATCH_INTEGRITY.subtract_then_add_in_place(
        &mut expected_state.hash,
        &[original_value_mac],
        &[new_value_mac],
    );

    assert_eq!(
        final_state.hash, expected_state.hash,
        "The final LTHash is incorrect, meaning the overwrite was not handled properly."
    );
    assert_eq!(
        final_state.version, 2,
        "The version should be updated to that of the patch."
    );
}
