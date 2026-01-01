//! E2E Session management methods for Client.
//!
//! This module contains methods for managing Signal protocol sessions,
//! including establishing new sessions and ensuring sessions exist before sending.
//!
//! Key features:
//! - Wait for offline delivery to complete before session establishment
//! - Resolve LID mappings before session establishment
//! - Batch prekey fetching and session establishment

use anyhow::Result;
use wacore_binary::jid::Jid;

use super::Client;

impl Client {
    /// Wait for offline message delivery to complete.
    /// Matches WhatsApp Web's WAWebEventsWaitForOfflineDeliveryEnd.waitForOfflineDeliveryEnd().
    /// Should be called before establishing new E2E sessions to avoid conflicts.
    pub(crate) async fn wait_for_offline_delivery_end(&self) {
        use std::sync::atomic::Ordering;

        if self.offline_sync_completed.load(Ordering::Relaxed) {
            return;
        }

        // Wait with a reasonable timeout to avoid blocking forever
        const TIMEOUT_SECS: u64 = 10;
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(TIMEOUT_SECS),
            self.offline_sync_notifier.notified(),
        )
        .await;
    }

    /// Ensure E2E sessions exist for the given device JIDs.
    /// Matches WhatsApp Web's `ensureE2ESessions` behavior.
    /// - Waits for offline delivery to complete
    /// - Resolves phone-to-LID mappings
    /// - Batches prekey fetches to avoid overwhelming the server
    pub(crate) async fn ensure_e2e_sessions(&self, device_jids: Vec<Jid>) -> Result<()> {
        use wacore::libsignal::store::SessionStore;
        use wacore::types::jid::JidExt;

        if device_jids.is_empty() {
            return Ok(());
        }

        // 1. Wait for offline sync (matches WhatsApp Web)
        self.wait_for_offline_delivery_end().await;

        // 2. Resolve LID mappings (matches WhatsApp Web)
        let resolved_jids = self.resolve_lid_mappings(&device_jids).await;

        // 3. Filter to JIDs that need sessions (pre-allocate with upper bound)
        let device_store = self.persistence_manager.get_device_arc().await;
        let mut jids_needing_sessions = Vec::with_capacity(resolved_jids.len());

        {
            let device_guard = device_store.read().await;
            for jid in resolved_jids {
                let signal_addr = jid.to_protocol_address();
                match device_guard.contains_session(&signal_addr).await {
                    Ok(true) => {
                        // Session exists, skip
                    }
                    Ok(false) => {
                        // No session, need to establish one
                        jids_needing_sessions.push(jid);
                    }
                    Err(e) => {
                        // Storage error - log and skip this JID rather than treating as missing
                        log::warn!("Failed to check session for {}: {}", jid, e);
                    }
                }
            }
        }

        if jids_needing_sessions.is_empty() {
            return Ok(());
        }

        // 4. Fetch and establish sessions (with batching)
        for batch in jids_needing_sessions.chunks(crate::session::SESSION_CHECK_BATCH_SIZE) {
            self.fetch_and_establish_sessions(batch).await?;
        }

        Ok(())
    }

    /// Fetch prekeys and establish sessions for a batch of JIDs.
    ///
    /// Returns the number of sessions successfully established.
    /// Returns an error only if the prekey fetch itself fails (network error).
    /// Individual session establishment failures are logged but don't fail the batch.
    async fn fetch_and_establish_sessions(&self, jids: &[Jid]) -> Result<usize, anyhow::Error> {
        use rand::TryRngCore;
        use wacore::libsignal::protocol::{UsePQRatchet, process_prekey_bundle};
        use wacore::types::jid::JidExt;

        if jids.is_empty() {
            return Ok(0);
        }

        let prekey_bundles = self.fetch_pre_keys(jids, Some("identity")).await?;

        let device_store = self.persistence_manager.get_device_arc().await;
        let mut adapter =
            crate::store::signal_adapter::SignalProtocolStoreAdapter::new(device_store);

        let mut success_count = 0;
        let mut missing_count = 0;
        let mut failed_count = 0;

        for jid in jids {
            if let Some(bundle) = prekey_bundles.get(jid) {
                let signal_addr = jid.to_protocol_address();
                match process_prekey_bundle(
                    &signal_addr,
                    &mut adapter.session_store,
                    &mut adapter.identity_store,
                    bundle,
                    &mut rand::rngs::OsRng.unwrap_err(),
                    UsePQRatchet::No,
                )
                .await
                {
                    Ok(_) => {
                        success_count += 1;
                        log::debug!("Successfully established session with {}", jid);
                    }
                    Err(e) => {
                        failed_count += 1;
                        log::warn!("Failed to establish session with {}: {}", jid, e);
                    }
                }
            } else {
                missing_count += 1;
                // Device 0 is critical for PDO - log at warn level
                if jid.device == 0 {
                    log::warn!(
                        "Server did not return prekeys for primary phone {} - PDO will not work",
                        jid
                    );
                } else {
                    log::debug!("Server did not return prekeys for {}", jid);
                }
            }
        }

        if missing_count > 0 || failed_count > 0 {
            log::info!(
                "Session establishment: {} succeeded, {} missing prekeys, {} failed (of {} requested)",
                success_count,
                missing_count,
                failed_count,
                jids.len()
            );
        }

        Ok(success_count)
    }

    /// Establish session with primary phone (device 0) immediately for PDO.
    ///
    /// PDO (Peer Data Operation) allows the linked device to request already-decrypted
    /// message content from the primary phone when local decryption fails. This requires
    /// a Signal session to encrypt the request.
    ///
    /// This is called during login BEFORE offline messages arrive. It does NOT wait
    /// for offline sync to complete - it establishes the session immediately so that
    /// PDO can work for any messages that fail to decrypt during offline sync.
    ///
    /// # WhatsApp Web Reference
    ///
    /// WhatsApp Web establishes sessions proactively on app bootstrap via
    /// `bootstrapDeviceCapabilities()` which sends to device 0 before any
    /// offline messages are processed.
    pub(crate) async fn establish_primary_phone_session_immediate(&self) -> Result<()> {
        let device_snapshot = self.persistence_manager.get_device_snapshot().await;

        let own_pn = device_snapshot
            .pn
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Not logged in - no phone number available"))?;

        let primary_phone_jid = own_pn.with_device(0);

        log::info!(
            "Proactively establishing session with primary phone {} on login",
            primary_phone_jid
        );

        // Directly fetch and establish session without waiting for offline sync
        let success_count = self
            .fetch_and_establish_sessions(std::slice::from_ref(&primary_phone_jid))
            .await?;

        if success_count == 0 {
            anyhow::bail!(
                "Failed to establish session with primary phone {} - PDO will not work",
                primary_phone_jid
            );
        }

        Ok(())
    }
}
