//! Device Registry methods for Client.
//!
//! Manages the device registry cache for tracking known devices per user.
//! Uses LID-first storage with bidirectional lookup support.

use anyhow::Result;
use log::{debug, info, warn};
use wacore_binary::jid::Jid;

use super::Client;

/// Result of resolving a user identifier to lookup keys.
/// This makes the LID/PN relationship explicit instead of using magic indices.
#[derive(Debug, Clone)]
enum UserLookupKeys {
    /// User is a LID with known phone number mapping.
    /// Keys: [LID, PN]
    LidWithPn { lid: String, pn: String },
    /// User is a phone number with known LID mapping.
    /// Keys: [LID, PN]
    PnWithLid { lid: String, pn: String },
    /// Unknown user - no LID-PN mapping exists.
    /// Could be either a LID or PN, we don't know.
    Unknown { user: String },
}

impl UserLookupKeys {
    /// Returns all keys to try for lookups, in preference order.
    fn all_keys(&self) -> Vec<&str> {
        match self {
            Self::LidWithPn { lid, pn } | Self::PnWithLid { lid, pn } => vec![lid, pn],
            Self::Unknown { user } => vec![user],
        }
    }

    /// Returns the canonical (preferred) key for storage.
    fn canonical_key(&self) -> &str {
        match self {
            Self::LidWithPn { lid, .. } | Self::PnWithLid { lid, .. } => lid,
            Self::Unknown { user } => user,
        }
    }
}

impl Client {
    /// Resolve a user identifier to its canonical storage key (LID preferred).
    ///
    /// This is a convenience wrapper around `resolve_lookup_keys().canonical_key()`.
    #[cfg(test)]
    pub(crate) async fn resolve_to_canonical_key(&self, user: &str) -> String {
        self.resolve_lookup_keys(user)
            .await
            .canonical_key()
            .to_string()
    }

    /// Resolve a user identifier to its lookup keys with type information.
    ///
    /// Returns a `UserLookupKeys` enum that explicitly represents:
    /// - `LidWithPn`: User is a LID with known phone number mapping
    /// - `PnWithLid`: User is a phone number with known LID mapping
    /// - `Unknown`: No LID-PN mapping exists (could be either type)
    async fn resolve_lookup_keys(&self, user: &str) -> UserLookupKeys {
        // Check if user is a LID (has a phone number mapping)
        if let Some(pn) = self.lid_pn_cache.get_phone_number(user).await {
            return UserLookupKeys::LidWithPn {
                lid: user.to_string(),
                pn,
            };
        }

        // Check if user is a PN (has a LID mapping)
        if let Some(lid) = self.lid_pn_cache.get_current_lid(user).await {
            return UserLookupKeys::PnWithLid {
                lid,
                pn: user.to_string(),
            };
        }

        // Unknown user - no mapping exists
        UserLookupKeys::Unknown {
            user: user.to_string(),
        }
    }

    /// Get all possible lookup keys for a user (for bidirectional lookup).
    /// Returns keys in order of preference: [canonical_key, fallback_key].
    ///
    /// Note: Prefer `resolve_lookup_keys` when you need type information.
    pub(crate) async fn get_lookup_keys(&self, user: &str) -> Vec<String> {
        self.resolve_lookup_keys(user)
            .await
            .all_keys()
            .into_iter()
            .map(String::from)
            .collect()
    }

    /// Check if a device exists for a user.
    /// Returns true for device_id 0 (primary device always exists).
    pub(crate) async fn has_device(&self, user: &str, device_id: u32) -> bool {
        if device_id == 0 {
            return true;
        }

        let lookup_keys = self.get_lookup_keys(user).await;

        for key in &lookup_keys {
            if let Some(record) = self.device_registry_cache.get(key).await {
                return record.devices.iter().any(|d| d.device_id == device_id);
            }
        }

        let backend = self.persistence_manager.backend();
        for key in &lookup_keys {
            match backend.get_devices(key).await {
                Ok(Some(record)) => {
                    let has_device = record.devices.iter().any(|d| d.device_id == device_id);
                    // Cache under the record's actual user key (the key it was stored under
                    // in the backend), not lookup_keys[0] which is our guessed canonical key.
                    // This ensures consistency between the in-memory cache and the backend.
                    self.device_registry_cache
                        .insert(record.user.clone(), record)
                        .await;
                    return has_device;
                }
                Ok(None) => continue,
                Err(e) => {
                    warn!("Failed to check device registry for {}: {e}", key);
                }
            }
        }

        false
    }

    /// Update the device list for a user.
    /// Stores under LID when mapping is known, otherwise under PN.
    pub(crate) async fn update_device_list(
        &self,
        mut record: wacore::store::traits::DeviceListRecord,
    ) -> Result<()> {
        use anyhow::Context;

        let original_user = record.user.clone();
        let lookup = self.resolve_lookup_keys(&original_user).await;
        let canonical_key = lookup.canonical_key().to_string();
        record.user.clone_from(&canonical_key); // More efficient: reuses allocation

        // Clone record for cache before moving to backend
        let record_for_cache = record.clone();

        // Use canonical_key directly as cache key (no extra clone)
        self.device_registry_cache
            .insert(canonical_key.clone(), record_for_cache)
            .await;

        let backend = self.persistence_manager.backend();
        backend
            .update_device_list(record)
            .await
            .context("Failed to update device list in backend")?;

        if canonical_key != original_user {
            self.device_registry_cache.invalidate(&original_user).await;
            debug!(
                "Device registry: stored under LID {} (resolved from {})",
                canonical_key, original_user
            );
        }

        Ok(())
    }

    /// Invalidate the device cache for a specific user.
    ///
    /// This invalidates both the device registry cache (keyed by string) and
    /// the device cache (keyed by JID). For unknown users, we invalidate both
    /// possible JID types (LID and PN) to ensure cleanup regardless of which
    /// type was used when the cache was populated.
    pub(crate) async fn invalidate_device_cache(&self, user: &str) {
        let lookup = self.resolve_lookup_keys(user).await;

        // Invalidate device registry cache (string keys)
        for key in lookup.all_keys() {
            self.device_registry_cache.invalidate(key).await;
        }

        // Invalidate device cache (JID keys) with proper types
        let device_cache = self.get_device_cache().await;
        match &lookup {
            UserLookupKeys::LidWithPn { lid, pn } | UserLookupKeys::PnWithLid { lid, pn } => {
                // We know the exact types - invalidate each with correct JID type
                device_cache.invalidate(&Jid::lid(lid)).await;
                device_cache.invalidate(&Jid::pn(pn)).await;
            }
            UserLookupKeys::Unknown { user } => {
                // Unknown user - invalidate BOTH types to ensure cleanup.
                // This handles the edge case where devices were cached under
                // a JID type we can no longer determine.
                device_cache.invalidate(&Jid::lid(user)).await;
                device_cache.invalidate(&Jid::pn(user)).await;
            }
        }

        debug!("Invalidated device cache for user: {} ({:?})", user, lookup);
    }

    /// Background loop placeholder for device registry cleanup.
    /// Note: Cleanup functionality was removed as part of trait simplification.
    /// Device registry entries are managed through normal update/get operations.
    pub(super) async fn device_registry_cleanup_loop(&self) {
        // Simply wait for shutdown signal
        self.shutdown_notifier.notified().await;
        debug!(
            target: "Client/DeviceRegistry",
            "Shutdown signaled, exiting cleanup loop"
        );
    }

    /// Migrate device registry entries from PN key to LID key.
    pub(crate) async fn migrate_device_registry_on_lid_discovery(&self, pn: &str, lid: &str) {
        let backend = self.persistence_manager.backend();

        match backend.get_devices(pn).await {
            Ok(Some(mut record)) => {
                info!(
                    "Migrating device registry entry from PN {} to LID {} ({} devices)",
                    pn,
                    lid,
                    record.devices.len()
                );

                record.user = lid.to_string();

                if let Err(e) = backend.update_device_list(record.clone()).await {
                    warn!("Failed to migrate device registry to LID: {}", e);
                    return;
                }

                self.device_registry_cache
                    .insert(lid.to_string(), record)
                    .await;

                // Invalidate both the string-keyed device_registry_cache AND the
                // JID-keyed device cache. Using invalidate_device_cache ensures
                // we clean up Jid::pn(pn) entries that would otherwise become stale.
                self.invalidate_device_cache(pn).await;
            }
            Ok(None) => {}
            Err(e) => {
                warn!("Failed to check for PN device registry entry: {}", e);
            }
        }
    }
}
