use crate::client::Client;
use crate::message::RetryReason;
use crate::types::events::Receipt;
use log::{info, warn};
use prost::Message;
use rand::TryRngCore;
use scopeguard;
use std::sync::Arc;
use wacore::libsignal::protocol::{
    KeyPair, PreKeyBundle, PublicKey, UsePQRatchet, process_prekey_bundle,
};
use wacore::libsignal::store::PreKeyStore;
use wacore::libsignal::store::SessionStore;
use wacore::types::jid::JidExt;
use wacore_binary::builder::NodeBuilder;
use wacore_binary::jid::JidExt as _;
use wacore_binary::node::{Node, NodeContent};

/// Helper to extract bytes content from a Node.
pub(crate) fn get_bytes_content(node: &Node) -> Option<&[u8]> {
    match &node.content {
        Some(NodeContent::Bytes(b)) => Some(b.as_slice()),
        _ => None,
    }
}

/// Helper to extract registration ID from a node (4 bytes big-endian).
pub(crate) fn extract_registration_id_from_node(node: &Node) -> Option<u32> {
    let registration_node = node.get_optional_child("registration")?;
    let bytes = get_bytes_content(registration_node)?;

    if bytes.len() >= 4 {
        Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    } else if !bytes.is_empty() {
        // Handle variable-length encoding.
        let mut arr = [0u8; 4];
        let start = 4 - bytes.len();
        arr[start..].copy_from_slice(bytes);
        Some(u32::from_be_bytes(arr))
    } else {
        None
    }
}

/// Maximum retry attempts we'll honor (matches WhatsApp Web's MAX_RETRY = 5).
/// We refuse to resend if the requester has already retried this many times.
const MAX_RETRY_COUNT: u8 = 5;

/// Minimum retry count before we include keys in retry receipts.
/// WhatsApp Web only includes keys when retryCount >= 2, giving the first
/// retry a chance to succeed without key exchange overhead.
const MIN_RETRY_COUNT_FOR_KEYS: u8 = 2;

/// Minimum retry count before we start tracking base keys.
/// WhatsApp Web saves base key on retry 2, checks on retry > 2.
const MIN_RETRY_FOR_BASE_KEY_CHECK: u8 = 2;

impl Client {
    pub(crate) async fn handle_retry_receipt(
        self: &Arc<Self>,
        receipt: &Receipt,
        node: &Node,
    ) -> Result<(), anyhow::Error> {
        let retry_child = node
            .get_optional_child("retry")
            .ok_or_else(|| anyhow::anyhow!("<retry> child missing from receipt"))?;

        let message_id = retry_child.attrs().string("id");
        let retry_count: u8 = retry_child
            .attrs()
            .optional_string("count")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        // Refuse to handle retries that have exceeded the maximum attempts.
        // This prevents infinite retry loops and matches WhatsApp Web's behavior.
        if retry_count >= MAX_RETRY_COUNT {
            warn!(
                "Refusing retry #{} for message {} from {}: exceeds max attempts ({})",
                retry_count, message_id, receipt.source.sender, MAX_RETRY_COUNT
            );
            return Ok(());
        }

        // Deduplicate retry receipts to prevent processing the same retry multiple times.
        // For groups: key is (chat, msg_id, sender) since each participant retries independently.
        // For DMs: key is (chat, msg_id) since there's only one sender.
        // Uses atomic entry API to avoid race conditions between check and insert.
        let dedupe_key = if receipt.source.chat.is_group() {
            format!(
                "{}:{}:{}",
                receipt.source.chat, message_id, receipt.source.sender
            )
        } else {
            format!("{}:{}", receipt.source.chat, message_id)
        };

        let entry = self
            .retried_group_messages
            .entry(dedupe_key.clone())
            .or_insert(())
            .await;

        if !entry.is_fresh() {
            log::debug!(
                "Ignoring duplicate retry for message {} from {}: already handled.",
                message_id,
                receipt.source.sender
            );
            return Ok(());
        }

        // Prevent concurrent retries for the same message.
        {
            let mut pending = self.pending_retries.lock().await;
            if pending.contains(&message_id) {
                log::debug!("Ignoring retry for {message_id}: a retry is already in progress.");
                return Ok(());
            }
            pending.insert(message_id.clone());
        }
        let _guard = scopeguard::guard((self.clone(), message_id.clone()), |(client, id)| {
            tokio::spawn(async move {
                client.pending_retries.lock().await.remove(&id);
            });
        });

        let original_msg = match self
            .take_recent_message(receipt.source.chat.clone(), message_id.clone())
            .await
        {
            Some(msg) => msg,
            None => {
                log::debug!(
                    "Ignoring retry for message {message_id}: already handled or not found in cache."
                );
                return Ok(());
            }
        };

        let participant_jid = receipt.source.sender.clone();

        // Device existence check (matches WhatsApp Web's WAWebApiDeviceList.hasDevice).
        // This prevents processing retry receipts from unknown/stale devices.
        let sender_device_id = participant_jid.device() as u32;
        let sender_user = participant_jid.user.clone();
        if !self.has_device(&sender_user, sender_device_id).await {
            warn!(
                "handle_retry_receipt: device not found for device={}, user={}",
                sender_device_id, sender_user
            );
            return Ok(());
        }

        // Check if this is a retry from our own device (peer).
        let device_snapshot = self.persistence_manager.get_device_snapshot().await;
        let is_peer = device_snapshot
            .pn
            .as_ref()
            .is_some_and(|our_pn| participant_jid.user == our_pn.user);

        // Process the key bundle from the retry receipt to establish a fresh session.
        // The requester includes their new prekeys so we can encrypt to them.
        // This is only done for DMs; group messages and status broadcasts use sender keys instead.
        let is_group_or_status =
            receipt.source.chat.is_group() || receipt.source.chat.is_status_broadcast();

        if !is_group_or_status {
            // Try to process key bundle if present
            let key_bundle_result = self
                .process_retry_key_bundle(node, &participant_jid, is_peer)
                .await;

            if let Err(e) = &key_bundle_result {
                warn!(
                    "Failed to process key bundle from retry receipt: {}. Checking for reg ID mismatch.",
                    e
                );

                // WhatsApp Web behavior: If no key bundle but registration ID differs from stored
                // session, delete the session to force re-establishment.
                // This handles the case where the requester reinstalled but didn't include keys.
                if let Some(received_reg_id) = extract_registration_id_from_node(node) {
                    let signal_address = participant_jid.to_protocol_address();
                    let device_store = self.persistence_manager.get_device_arc().await;
                    let device_guard = device_store.read().await;

                    if let Ok(session) = device_guard.load_session(&signal_address).await
                        && let Ok(stored_reg_id) = session.remote_registration_id()
                        && stored_reg_id != 0
                        && stored_reg_id != received_reg_id
                    {
                        drop(device_guard);
                        info!(
                            "Registration ID mismatch for {} (stored: {}, received: {}). \
                             Deleting session since no key bundle provided.",
                            signal_address, stored_reg_id, received_reg_id
                        );
                        if let Err(del_err) = device_store
                            .write()
                            .await
                            .delete_session(&signal_address)
                            .await
                        {
                            warn!("Failed to delete session for reg ID mismatch: {}", del_err);
                        }
                    }
                }
            }
        }

        if is_group_or_status {
            // For groups and status broadcasts, mark participant as needing fresh SKDM.
            // WhatsApp Web uses `markForgetSenderKey` which lazily marks participants for
            // SKDM redistribution on the next send, rather than immediately deleting
            // the sender key.
            let group_jid = receipt.source.chat.to_string();
            let participant_str = participant_jid.to_string();

            // Mark this participant as needing fresh SKDM (filters out own devices internally)
            if let Err(e) = self
                .mark_forget_sender_key(&group_jid, std::slice::from_ref(&participant_str))
                .await
            {
                log::warn!(
                    "Failed to mark sender key forget for {} in {}: {}",
                    participant_str,
                    group_jid,
                    e
                );
            } else {
                let chat_type = if receipt.source.chat.is_status_broadcast() {
                    "status broadcast"
                } else {
                    "group"
                };
                info!(
                    "Marked {} for fresh SKDM in {} {} due to retry receipt",
                    participant_str, chat_type, group_jid
                );
            }
        } else {
            // For DMs, handle base key tracking for collision detection (matches WhatsApp Web).
            // This detects when we haven't regenerated our session despite receiving retry receipts,
            // which can cause infinite retry loops where both sides are stuck with stale keys.
            let signal_address = participant_jid.to_protocol_address();
            let address_str = signal_address.to_string();
            let device_store = self.persistence_manager.get_device_arc().await;

            // Check for base key collision before deleting the session
            {
                let device_guard = device_store.read().await;
                if let Ok(session) = device_guard.load_session(&signal_address).await
                    && let Ok(current_base_key) = session.alice_base_key()
                {
                    if retry_count == MIN_RETRY_FOR_BASE_KEY_CHECK {
                        // On retry 2: Save the base key for later comparison
                        if let Err(e) = device_guard
                            .backend
                            .save_base_key(&address_str, &message_id, current_base_key)
                            .await
                        {
                            warn!("Failed to save base key for {}: {}", address_str, e);
                        } else {
                            info!(
                                "Saved base key for {} at retry #{} for collision detection",
                                address_str, retry_count
                            );
                        }
                    } else if retry_count > MIN_RETRY_FOR_BASE_KEY_CHECK {
                        // On retry > 2: Check if base key is the same (collision detection)
                        match device_guard
                            .backend
                            .has_same_base_key(&address_str, &message_id, current_base_key)
                            .await
                        {
                            Ok(true) => {
                                // Collision detected! We haven't regenerated our session.
                                warn!(
                                    "Base key collision detected for {} at retry #{}. \
                                     Session hasn't been regenerated. Forcing fresh session.",
                                    address_str, retry_count
                                );
                                // Clean up base key entry since we're deleting the session
                                let _ = device_guard
                                    .backend
                                    .delete_base_key(&address_str, &message_id)
                                    .await;
                            }
                            Ok(false) => {
                                // Base key changed, session was regenerated - good!
                                info!(
                                    "Base key changed for {} at retry #{} - session regenerated",
                                    address_str, retry_count
                                );
                                // Clean up old base key entry
                                let _ = device_guard
                                    .backend
                                    .delete_base_key(&address_str, &message_id)
                                    .await;
                            }
                            Err(e) => {
                                warn!("Failed to check base key for {}: {}", address_str, e);
                            }
                        }
                    }
                }
            }

            // Delete the old session so a fresh one is established on resend.
            if let Err(e) = device_store
                .write()
                .await
                .delete_session(&signal_address)
                .await
            {
                log::warn!("Failed to delete session for {signal_address}: {e}");
            } else {
                info!("Deleted session for {signal_address} due to retry receipt");
            }
        }

        info!(
            "Resending message {} to {} (retry #{})",
            message_id, receipt.source.chat, retry_count
        );

        self.send_message_impl(
            receipt.source.chat.clone(),
            &original_msg,
            Some(message_id),
            false,
            true, // is_retry: includes fresh SKDM for groups
            None,
            vec![], // Extra nodes not preserved on retry - caller must resend with options if needed
        )
        .await?;

        Ok(())
    }

    /// Extracts and processes the key bundle from a retry receipt.
    /// This allows us to establish a new session with the requester using their fresh prekeys.
    ///
    /// # Arguments
    /// * `node` - The retry receipt node containing the key bundle
    /// * `requester_jid` - The JID of the device requesting the retry
    /// * `is_peer` - Whether this is a peer device (our own device)
    async fn process_retry_key_bundle(
        &self,
        node: &Node,
        requester_jid: &wacore_binary::jid::Jid,
        is_peer: bool,
    ) -> Result<(), anyhow::Error> {
        let keys_node = node
            .get_optional_child("keys")
            .ok_or_else(|| anyhow::anyhow!("<keys> child missing from retry receipt"))?;

        let registration_node = node.get_optional_child("registration");

        // Extract registration ID (4 bytes big-endian).
        let registration_id = registration_node
            .and_then(get_bytes_content)
            .map(|bytes| {
                if bytes.len() >= 4 {
                    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                } else if !bytes.is_empty() {
                    // Handle variable-length encoding.
                    let mut arr = [0u8; 4];
                    let start = 4 - bytes.len();
                    arr[start..].copy_from_slice(bytes);
                    u32::from_be_bytes(arr)
                } else {
                    0
                }
            })
            .unwrap_or(0);

        if registration_id == 0 {
            return Err(anyhow::anyhow!("Invalid registration ID in retry receipt"));
        }

        let signal_address = requester_jid.to_protocol_address();

        // Check if the registration ID changed (indicates device reinstall).
        let device_store = self.persistence_manager.get_device_arc().await;
        let device_guard = device_store.read().await;
        if let Ok(session) = device_guard.load_session(&signal_address).await {
            let existing_reg_id = session.remote_registration_id()?;
            if existing_reg_id != 0 && existing_reg_id != registration_id {
                // WhatsApp Web throws an error for peer device registration ID changes.
                // This is a security measure - peer devices should maintain consistent identity.
                if is_peer {
                    return Err(anyhow::anyhow!(
                        "Registration ID changed for peer device {} (was {}, now {}). \
                         This may indicate the device was reinstalled.",
                        signal_address,
                        existing_reg_id,
                        registration_id
                    ));
                }
                info!(
                    "Registration ID changed for {} (was {}, now {}). Session will be replaced.",
                    signal_address, existing_reg_id, registration_id
                );
            }
        }
        drop(device_guard);

        // Extract identity key.
        let identity_bytes = keys_node
            .get_optional_child("identity")
            .and_then(get_bytes_content)
            .ok_or_else(|| anyhow::anyhow!("Missing identity key in retry receipt"))?;
        let identity_key = PublicKey::from_djb_public_key_bytes(identity_bytes)?;

        // Extract prekey (optional in some cases).
        let prekey_data = keys_node.get_optional_child("key").and_then(|key_node| {
            let id_bytes = key_node
                .get_optional_child("id")
                .and_then(get_bytes_content)?;
            let value_bytes = key_node
                .get_optional_child("value")
                .and_then(get_bytes_content)?;

            // PreKey ID is 3 bytes big-endian.
            let prekey_id = if id_bytes.len() >= 3 {
                u32::from_be_bytes([0, id_bytes[0], id_bytes[1], id_bytes[2]])
            } else {
                return None;
            };

            let prekey_public = PublicKey::from_djb_public_key_bytes(value_bytes).ok()?;
            Some((prekey_id.into(), prekey_public))
        });

        // Extract signed prekey.
        let skey_node = keys_node
            .get_optional_child("skey")
            .ok_or_else(|| anyhow::anyhow!("Missing signed prekey in retry receipt"))?;

        let skey_id_bytes = skey_node
            .get_optional_child("id")
            .and_then(get_bytes_content)
            .ok_or_else(|| anyhow::anyhow!("Missing signed prekey ID"))?;
        let skey_id = if skey_id_bytes.len() >= 3 {
            u32::from_be_bytes([0, skey_id_bytes[0], skey_id_bytes[1], skey_id_bytes[2]])
        } else {
            return Err(anyhow::anyhow!("Invalid signed prekey ID length"));
        };

        let skey_value_bytes = skey_node
            .get_optional_child("value")
            .and_then(get_bytes_content)
            .ok_or_else(|| anyhow::anyhow!("Missing signed prekey value"))?;
        let skey_public = PublicKey::from_djb_public_key_bytes(skey_value_bytes)?;

        let skey_sig_bytes = skey_node
            .get_optional_child("signature")
            .and_then(get_bytes_content)
            .ok_or_else(|| anyhow::anyhow!("Missing signed prekey signature"))?;
        let skey_signature: [u8; 64] = skey_sig_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid signature length"))?;

        // Build and process the prekey bundle.
        let bundle = PreKeyBundle::new(
            registration_id,
            u32::from(requester_jid.device).into(),
            prekey_data,
            skey_id.into(),
            skey_public,
            skey_signature.into(),
            identity_key.into(),
        )?;

        let device_store = self.persistence_manager.get_device_arc().await;

        let mut adapter =
            crate::store::signal_adapter::SignalProtocolStoreAdapter::new(device_store);

        process_prekey_bundle(
            &signal_address,
            &mut adapter.session_store,
            &mut adapter.identity_store,
            &bundle,
            &mut rand::rngs::OsRng.unwrap_err(),
            UsePQRatchet::No,
        )
        .await?;

        info!(
            "Processed key bundle from retry receipt for {}",
            signal_address
        );

        Ok(())
    }

    /// Sends a retry receipt to request the sender to resend a message.
    ///
    /// # Arguments
    /// * `info` - The message info for the failed message
    /// * `retry_count` - The retry attempt number (1-5). This is sent to the sender so they
    ///   know which attempt this is. The sender may use this to decide whether to resend.
    /// * `reason` - The retry reason code (matches WhatsApp Web's RetryReason enum). This helps
    ///   the sender understand why the message couldn't be decrypted.
    pub(crate) async fn send_retry_receipt(
        &self,
        info: &crate::types::message::MessageInfo,
        retry_count: u8,
        reason: RetryReason,
    ) -> Result<(), anyhow::Error> {
        let device_snapshot = self.persistence_manager.get_device_snapshot().await;

        // Bot message filtering (matches WhatsApp Web behavior):
        // Don't send retry receipts to bot accounts from non-bot accounts.
        // This prevents unnecessary retry traffic to automated systems.
        let we_are_bot = device_snapshot
            .pn
            .as_ref()
            .map(|our_pn| our_pn.is_bot())
            .unwrap_or(false);
        let sender_is_bot = info.source.sender.is_bot();

        if !we_are_bot && sender_is_bot {
            log::debug!(
                "Skipping retry receipt for message {} from bot {}: bots don't process retries",
                info.id,
                info.source.sender
            );
            return Ok(());
        }

        warn!(
            "Sending retry receipt #{} for message {} from {} (reason: {:?})",
            retry_count, info.id, info.source.sender, reason
        );

        // Build the retry element with the error code (matches WhatsApp Web's format)
        let mut retry_builder = NodeBuilder::new("retry")
            .attr("v", "1")
            .attr("id", info.id.clone())
            .attr("t", info.timestamp.timestamp().to_string())
            .attr("count", retry_count.to_string());

        // Include the error code if it's not UnknownError (matches WhatsApp Web's behavior
        // where error is only included when there's a specific reason)
        if reason != RetryReason::UnknownError {
            retry_builder = retry_builder.attr("error", (reason as u8).to_string());
        }

        let retry_node = retry_builder.build();

        let registration_id_bytes = device_snapshot.registration_id.to_be_bytes().to_vec();
        let registration_node = NodeBuilder::new("registration")
            .bytes(registration_id_bytes)
            .build();

        // WhatsApp Web only includes keys when retryCount >= 2.
        // First retry gives the sender a chance to resend without full key exchange.
        let keys_node = if retry_count >= MIN_RETRY_COUNT_FOR_KEYS {
            let device_store = self.persistence_manager.get_device_arc().await;
            let device_guard = device_store.read().await;

            let new_prekey_id = (rand::random::<u32>() % 16777215) + 1;
            let new_prekey_keypair = KeyPair::generate(&mut rand::rngs::OsRng.unwrap_err());
            let new_prekey_record = wacore::libsignal::store::record_helpers::new_pre_key_record(
                new_prekey_id,
                &new_prekey_keypair,
            );
            // This key is not uploaded to the server pool, so mark as false
            if let Err(e) = device_guard
                .store_prekey(new_prekey_id, new_prekey_record, false)
                .await
            {
                warn!("Failed to store new prekey for retry receipt: {e:?}");
            }
            drop(device_guard);

            let identity_key_bytes = device_snapshot
                .identity_key
                .public_key
                .public_key_bytes()
                .to_vec();

            let prekey_id_bytes = new_prekey_id.to_be_bytes()[1..].to_vec();
            let prekey_value_bytes = new_prekey_keypair.public_key.public_key_bytes().to_vec();

            let skey_id_bytes = 1u32.to_be_bytes()[1..].to_vec();
            let skey_value_bytes = device_snapshot
                .signed_pre_key
                .public_key
                .public_key_bytes()
                .to_vec();
            let skey_sig_bytes = device_snapshot.signed_pre_key_signature.to_vec();

            let device_identity_bytes = device_snapshot
                .account
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Missing device account info for retry receipt"))?
                .encode_to_vec();

            let type_bytes = vec![5u8];

            Some(
                NodeBuilder::new("keys")
                    .children([
                        NodeBuilder::new("type").bytes(type_bytes).build(),
                        NodeBuilder::new("identity")
                            .bytes(identity_key_bytes)
                            .build(),
                        NodeBuilder::new("key")
                            .children([
                                NodeBuilder::new("id").bytes(prekey_id_bytes).build(),
                                NodeBuilder::new("value").bytes(prekey_value_bytes).build(),
                            ])
                            .build(),
                        NodeBuilder::new("skey")
                            .children([
                                NodeBuilder::new("id").bytes(skey_id_bytes).build(),
                                NodeBuilder::new("value").bytes(skey_value_bytes).build(),
                                NodeBuilder::new("signature").bytes(skey_sig_bytes).build(),
                            ])
                            .build(),
                        NodeBuilder::new("device-identity")
                            .bytes(device_identity_bytes)
                            .build(),
                    ])
                    .build(),
            )
        } else {
            None
        };

        let receipt_to = if info.source.is_group {
            info.source.chat.to_string()
        } else {
            info.source.sender.to_string()
        };

        // Build the receipt node. For group messages, include the participant attribute
        // to identify which group member should resend. For DMs, omit it since the
        // "to" address already identifies the sender.
        let mut builder = NodeBuilder::new("receipt")
            .attr("to", receipt_to)
            .attr("id", info.id.clone())
            .attr("type", "retry");

        if info.source.is_group {
            builder = builder.attr("participant", info.source.sender.to_string());
        }

        // Handle peer vs device sync messages (matches WhatsApp Web's sendRetryReceipt):
        // WhatsApp Web checks: if (to.isUser()) { if (isMeAccount(to)) { ... } }
        // This means the category/recipient logic ONLY applies to DMs (not groups).
        // For groups, only the participant attribute is set (handled above).
        if !info.source.is_group {
            let is_from_own_account = device_snapshot
                .pn
                .as_ref()
                .is_some_and(|pn| info.source.sender.is_same_user_as(pn))
                || device_snapshot
                    .lid
                    .as_ref()
                    .is_some_and(|lid| info.source.sender.is_same_user_as(lid));

            if is_from_own_account {
                if info.category == "peer" {
                    builder = builder.attr("category", "peer");
                } else {
                    // Include recipient so the sender can look up the original message.
                    // Without this, the retry fails silently (getTargetChat returns null).
                    let recipient = info.source.recipient.as_ref().unwrap_or(&info.source.chat);
                    builder = builder.attr("recipient", recipient.to_string());
                }
            }
        }

        // Build children list - keys are only included when retryCount >= 2
        let receipt_node = if let Some(keys) = keys_node {
            builder
                .children([retry_node, registration_node, keys])
                .build()
        } else {
            builder.children([retry_node, registration_node]).build()
        };

        self.send_node(receipt_node).await?;
        Ok(())
    }
}
