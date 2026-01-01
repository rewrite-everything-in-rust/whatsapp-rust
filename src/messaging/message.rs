use crate::client::Client;
use crate::store::signal_adapter::SignalProtocolStoreAdapter;
use crate::types::events::Event;
use crate::types::message::MessageInfo;
use chrono::DateTime;
use log::{debug, warn};
use prost::Message as ProtoMessage;
use rand::TryRngCore;
use std::sync::Arc;
use wacore::libsignal::crypto::DecryptionError;
use wacore::libsignal::protocol::SenderKeyDistributionMessage;
use wacore::libsignal::protocol::group_decrypt;
use wacore::libsignal::protocol::process_sender_key_distribution_message;
use wacore::libsignal::protocol::{
    PreKeySignalMessage, SignalMessage, SignalProtocolError, UsePQRatchet, message_decrypt,
};
use wacore::libsignal::protocol::{
    PublicKey as SignalPublicKey, SENDERKEY_MESSAGE_CURRENT_VERSION,
};
use wacore::libsignal::store::sender_key_name::SenderKeyName;
use wacore::messages::MessageUtils;
use wacore::types::jid::JidExt;
use wacore_binary::jid::Jid;
use wacore_binary::jid::JidExt as _;
use wacore_binary::node::Node;
use waproto::whatsapp::{self as wa};

/// Maximum retry attempts per message (matches WhatsApp Web's MAX_RETRY = 5).
/// After this many retries, we stop sending retry receipts and rely solely on PDO.
pub(crate) const MAX_DECRYPT_RETRIES: u8 = 5;

/// Retry count threshold for logging high retry warnings.
/// WhatsApp Web logs metrics when retry count exceeds this value.
pub(crate) const HIGH_RETRY_COUNT_THRESHOLD: u8 = 3;

/// Retry reason codes matching WhatsApp Web's RetryReason enum.
/// These are included in the retry receipt to help the sender understand
/// why the message couldn't be decrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[allow(dead_code)] // All variants defined for WhatsApp Web compatibility
pub(crate) enum RetryReason {
    /// Unknown or unspecified error
    UnknownError = 0,
    /// No session exists with the sender (SessionNotFound)
    NoSession = 1,
    /// Invalid key in the message
    InvalidKey = 2,
    /// PreKey ID not found (InvalidPreKeyId)
    InvalidKeyId = 3,
    /// Invalid message format or content (InvalidMessage)
    InvalidMessage = 4,
    /// Invalid signature
    InvalidSignature = 5,
    /// Message from the future (timestamp issue)
    FutureMessage = 6,
    /// MAC verification failed (bad MAC)
    BadMac = 7,
    /// Invalid session state
    InvalidSession = 8,
    /// Invalid message key
    InvalidMsgKey = 9,
}

impl Client {
    /// Dispatches an `UndecryptableMessage` event to notify consumers that a message
    /// could not be decrypted. This is called when decryption fails and we need to
    /// show a placeholder to the user (like "Waiting for this message...").
    ///
    /// # Arguments
    /// * `info` - The message info for the undecryptable message
    /// * `decrypt_fail_mode` - Whether to show or hide the placeholder (matches WhatsApp Web's `hideFail`)
    fn dispatch_undecryptable_event(
        &self,
        info: &MessageInfo,
        decrypt_fail_mode: crate::types::events::DecryptFailMode,
    ) {
        self.core.event_bus.dispatch(&Event::UndecryptableMessage(
            crate::types::events::UndecryptableMessage {
                info: info.clone(),
                is_unavailable: false,
                unavailable_type: crate::types::events::UnavailableType::Unknown,
                decrypt_fail_mode,
            },
        ));
    }

    /// Handles a decryption failure by dispatching an undecryptable event and spawning a retry receipt.
    ///
    /// This is a convenience method that combines the common pattern of:
    /// 1. Dispatching an UndecryptableMessage event
    /// 2. Spawning a retry receipt to request re-encryption
    ///
    /// Returns `true` to be assigned to `dispatched_undecryptable` flag.
    fn handle_decrypt_failure(self: &Arc<Self>, info: &MessageInfo, reason: RetryReason) -> bool {
        self.dispatch_undecryptable_event(info, crate::types::events::DecryptFailMode::Show);
        self.spawn_retry_receipt(info, reason);
        true
    }

    /// Atomically increments the retry count for a message and returns the new count.
    /// Returns `None` if max retries have been reached.
    ///
    /// Uses moka's `and_compute_with` for truly atomic read-modify-write operations,
    /// preventing race conditions where concurrent calls could exceed MAX_DECRYPT_RETRIES.
    pub(crate) async fn increment_retry_count(&self, cache_key: &str) -> Option<u8> {
        use moka::ops::compute::Op;

        let result = self
            .message_retry_counts
            .entry_by_ref(cache_key)
            .and_compute_with(|maybe_entry| {
                let op = if let Some(entry) = maybe_entry {
                    let current = entry.into_value();
                    if current >= MAX_DECRYPT_RETRIES {
                        // Max retries reached, don't increment
                        Op::Nop
                    } else {
                        // Increment the counter
                        Op::Put(current + 1)
                    }
                } else {
                    // No entry exists, insert initial count of 1
                    Op::Put(1_u8)
                };
                std::future::ready(op)
            })
            .await;

        // Extract the new count from the result
        match result {
            moka::ops::compute::CompResult::Inserted(entry) => Some(entry.into_value()),
            moka::ops::compute::CompResult::ReplacedWith(entry) => Some(entry.into_value()),
            moka::ops::compute::CompResult::Unchanged(_) => None, // Max retries reached
            moka::ops::compute::CompResult::StillNone(_) => None, // Should not happen
            moka::ops::compute::CompResult::Removed(_) => None,   // Should not happen
        }
    }

    /// Spawns a task that sends a retry receipt for a failed decryption.
    ///
    /// This is used when sessions are not found or invalid to request the sender to resend
    /// the message with a PreKeySignalMessage to re-establish the session.
    ///
    /// # Retry Count Tracking
    ///
    /// This method tracks retry counts per message (keyed by `{chat}:{msg_id}:{sender}`)
    /// and stops sending retry receipts after `MAX_DECRYPT_RETRIES` (5) attempts to prevent
    /// infinite retry loops. This matches WhatsApp Web's behavior.
    ///
    /// # PDO Backup
    ///
    /// A PDO (Peer Data Operation) request is spawned only on the FIRST retry attempt.
    /// This asks our primary phone to share the already-decrypted message content.
    /// PDO is NOT spawned on subsequent retries to avoid duplicate requests.
    ///
    /// When max retries is reached, an immediate PDO request is sent as a last resort.
    ///
    /// # Arguments
    /// * `info` - The message info for the failed message
    /// * `reason` - The retry reason code (matches WhatsApp Web's RetryReason enum)
    pub(crate) fn spawn_retry_receipt(self: &Arc<Self>, info: &MessageInfo, reason: RetryReason) {
        let cache_key = format!("{}:{}:{}", info.source.chat, info.id, info.source.sender);
        let client = Arc::clone(self);
        let info = info.clone();

        tokio::spawn(async move {
            // Atomically increment retry count and check if we should continue
            let Some(retry_count) = client.increment_retry_count(&cache_key).await else {
                // Max retries reached
                log::info!(
                    "Max retries ({}) reached for message {} from {} [{:?}]. Sending immediate PDO request.",
                    MAX_DECRYPT_RETRIES,
                    info.id,
                    info.source.sender,
                    reason
                );
                // Send PDO request immediately (no delay) as last resort
                client.spawn_pdo_request_with_options(&info, true);
                return;
            };

            // Log warning for high retry counts (like WhatsApp Web's MessageHighRetryCount)
            if retry_count > HIGH_RETRY_COUNT_THRESHOLD {
                log::warn!(
                    "High retry count ({}) for message {} from {} [{:?}]",
                    retry_count,
                    info.id,
                    info.source.sender,
                    reason
                );
            }

            // Send the retry receipt with the actual retry count and reason
            match client.send_retry_receipt(&info, retry_count, reason).await {
                Ok(()) => {
                    debug!(
                        "Sent retry receipt #{} for message {} from {} [{:?}]",
                        retry_count, info.id, info.source.sender, reason
                    );
                }
                Err(e) => {
                    log::error!(
                        "Failed to send retry receipt #{} for message {} [{:?}]: {:?}",
                        retry_count,
                        info.id,
                        reason,
                        e
                    );
                }
            }

            // Only spawn PDO on the FIRST retry to avoid duplicate requests.
            // The PDO cache also provides deduplication, but this reduces unnecessary work.
            if retry_count == 1 {
                client.spawn_pdo_request(&info);
            }
        });
    }

    pub(crate) async fn handle_encrypted_message(self: Arc<Self>, node: Arc<Node>) {
        let info = match self.parse_message_info(&node).await {
            Ok(info) => Arc::new(info),
            Err(e) => {
                log::warn!("Failed to parse message info: {e:?}");
                return;
            }
        };

        // Determine the JID to use for end-to-end decryption.
        //
        // CRITICAL: WhatsApp Web ALWAYS uses LID-based addresses for Signal sessions when
        // a LID mapping is known. This is implemented in WAWebSignalAddress.toString():
        //
        //   var n = o("WAWebWidFactory").asUserWidOrThrow(this.wid);
        //   var a = !n.isLid() && n.isUser();  // true if PN
        //   var i = a ? o("WAWebApiContact").getCurrentLid(n) : n;  // Get LID if PN
        //   if (i == null) {
        //     return [this.wid.user, t, "@c.us"].join("");  // No LID, use PN
        //   } else {
        //     return [i.user, t, "@lid"].join("");  // Use LID
        //   }
        //
        // This means sessions are stored under the LID address, not the PN address.
        // When we receive a PN-addressed message, we must look up the session using
        // the LID address (if a LID mapping is known) to match WhatsApp Web's behavior.
        let sender_encryption_jid = {
            let sender = &info.source.sender;
            let alt = info.source.sender_alt.as_ref();
            let pn_server = wacore_binary::jid::DEFAULT_USER_SERVER;
            let lid_server = wacore_binary::jid::HIDDEN_USER_SERVER;

            if sender.server == lid_server {
                // Sender is already LID - use it directly for session lookup.
                // Also cache the LID-to-PN mapping if PN alt is available.
                if let Some(alt_jid) = alt
                    && alt_jid.server == pn_server
                {
                    if let Err(err) = self
                        .add_lid_pn_mapping(
                            &sender.user,
                            &alt_jid.user,
                            crate::lid_pn_cache::LearningSource::PeerLidMessage,
                        )
                        .await
                    {
                        warn!(
                            "Failed to persist LID-to-PN mapping {} -> {}: {err}",
                            sender.user, alt_jid.user
                        );
                    }
                    debug!(
                        "Cached LID-to-PN mapping: {} -> {}",
                        sender.user, alt_jid.user
                    );
                }
                sender.clone()
            } else if sender.server == pn_server {
                // Sender is PN - check if we have a LID mapping.
                // WhatsApp Web uses LID for sessions when available.

                // First, cache/update the mapping if sender_lid attribute is present
                if let Some(alt_jid) = alt
                    && alt_jid.server == lid_server
                {
                    if let Err(err) = self
                        .add_lid_pn_mapping(
                            &alt_jid.user,
                            &sender.user,
                            crate::lid_pn_cache::LearningSource::PeerPnMessage,
                        )
                        .await
                    {
                        warn!(
                            "Failed to persist PN-to-LID mapping {} -> {}: {err}",
                            sender.user, alt_jid.user
                        );
                    }
                    debug!(
                        "Cached PN-to-LID mapping: {} -> {}",
                        sender.user, alt_jid.user
                    );

                    // Use the LID from the message attribute for session lookup
                    let lid_jid = Jid {
                        user: alt_jid.user.clone(),
                        server: lid_server.to_string(),
                        device: sender.device,
                        agent: sender.agent,
                        integrator: sender.integrator,
                    };
                    log::debug!(
                        "Using LID {} for session lookup (sender was PN {})",
                        lid_jid,
                        sender
                    );
                    lid_jid
                } else if let Some(lid_user) = self.lid_pn_cache.get_current_lid(&sender.user).await
                {
                    // No sender_lid attribute, but we have a cached LID mapping
                    let lid_jid = Jid {
                        user: lid_user.clone(),
                        server: lid_server.to_string(),
                        device: sender.device,
                        agent: sender.agent,
                        integrator: sender.integrator,
                    };
                    log::debug!(
                        "Using cached LID {} for session lookup (sender was PN {})",
                        lid_jid,
                        sender
                    );
                    lid_jid
                } else {
                    // No LID mapping known - use PN address
                    log::debug!("No LID mapping for {}, using PN for session lookup", sender);
                    sender.clone()
                }
            } else {
                // Other server type (bot, hosted, group, broadcast, etc.) - use as-is
                // Note: Group senders will be handled specially below (skipped for session processing)
                sender.clone()
            }
        };

        log::debug!(
            "Message from {} (sender: {}, encryption JID: {}, is_from_me: {})",
            info.source.chat,
            info.source.sender,
            sender_encryption_jid,
            info.source.is_from_me
        );

        let mut all_enc_nodes = Vec::new();

        let direct_enc_nodes = node.get_children_by_tag("enc");
        all_enc_nodes.extend(direct_enc_nodes);

        let participants = node.get_optional_child_by_tag(&["participants"]);
        if let Some(participants_node) = participants {
            let to_nodes = participants_node.get_children_by_tag("to");
            for to_node in to_nodes {
                let to_jid = to_node.attrs().string("jid");
                let own_jid = self.get_pn().await;

                if let Some(our_jid) = own_jid
                    && to_jid == our_jid.to_string()
                {
                    let enc_children = to_node.get_children_by_tag("enc");
                    all_enc_nodes.extend(enc_children);
                }
            }
        }

        if all_enc_nodes.is_empty() {
            log::warn!("Received message without <enc> child: {}", node.tag);
            return;
        }

        let mut session_enc_nodes = Vec::with_capacity(all_enc_nodes.len());
        let mut group_content_enc_nodes = Vec::with_capacity(all_enc_nodes.len());

        for &enc_node in &all_enc_nodes {
            let enc_type = enc_node.attrs().string("type");

            if let Some(handler) = self.custom_enc_handlers.get(&enc_type) {
                let handler_clone = handler.clone();
                let client_clone = self.clone();
                let info_arc = Arc::clone(&info);
                let enc_node_clone = Arc::new(enc_node.clone());

                tokio::spawn(async move {
                    if let Err(e) = handler_clone
                        .handle(client_clone, &enc_node_clone, &info_arc)
                        .await
                    {
                        log::warn!("Custom handler for enc type '{}' failed: {e:?}", enc_type);
                    }
                });
                continue;
            }

            // Fall back to built-in handlers
            match enc_type.as_str() {
                "pkmsg" | "msg" => session_enc_nodes.push(enc_node),
                "skmsg" => group_content_enc_nodes.push(enc_node),
                _ => log::warn!("Unknown enc type: {enc_type}"),
            }
        }

        log::debug!(
            "Starting PASS 1: Processing {} session establishment messages (pkmsg/msg)",
            session_enc_nodes.len()
        );

        // Skip session processing for group senders (@c.us, @g.us, @broadcast)
        // Groups don't use 1:1 Signal Protocol sessions
        let is_group_sender = sender_encryption_jid.server.contains(".us")
            || sender_encryption_jid.server.contains("broadcast");

        let (
            session_decrypted_successfully,
            session_had_duplicates,
            session_dispatched_undecryptable,
        ) = if !is_group_sender && !session_enc_nodes.is_empty() {
            self.clone()
                .process_session_enc_batch(&session_enc_nodes, &info, &sender_encryption_jid)
                .await
        } else {
            if is_group_sender && !session_enc_nodes.is_empty() {
                log::debug!(
                    "Skipping {} session messages from group sender {}",
                    session_enc_nodes.len(),
                    sender_encryption_jid
                );
            }
            (false, false, false)
        };

        log::debug!(
            "Starting PASS 2: Processing {} group content messages (skmsg)",
            group_content_enc_nodes.len()
        );

        // Only process group content if:
        // 1. There were no session messages (session already exists), OR
        // 2. Session messages were successfully decrypted, OR
        // 3. Session messages were duplicates (already processed, so session exists)
        // 4. It's a status@broadcast (we might have sender key cached from previous status)
        // Skip only if session messages FAILED to decrypt (not duplicates, not absent)
        if !group_content_enc_nodes.is_empty() {
            // For status broadcasts, always try skmsg even if pkmsg failed.
            // WhatsApp Web does this too - the pkmsg contains the SKDM which might fail,
            // but if we already have the sender key cached from a previous status,
            // we can still decrypt the skmsg content.
            let should_process_skmsg = session_enc_nodes.is_empty()
                || session_decrypted_successfully
                || session_had_duplicates
                || info.source.chat.is_status_broadcast();

            if should_process_skmsg {
                if let Err(e) = self
                    .clone()
                    .process_group_enc_batch(
                        &group_content_enc_nodes,
                        &info,
                        &sender_encryption_jid,
                    )
                    .await
                {
                    log::warn!("Batch group decrypt encountered error (continuing): {e:?}");
                }
            } else {
                // Only show warning if session messages actually FAILED (not duplicates)
                if !session_had_duplicates {
                    warn!(
                        "Skipping skmsg decryption for message {} from {} because the initial session/senderkey message failed to decrypt. This prevents a retry loop.",
                        info.id, info.source.sender
                    );
                    // Still dispatch an UndecryptableMessage event so the user knows
                    // But only if we haven't already dispatched one in process_session_enc_batch
                    if !session_dispatched_undecryptable {
                        self.dispatch_undecryptable_event(
                            &info,
                            crate::types::events::DecryptFailMode::Show,
                        );
                    }

                    // Do NOT send a delivery receipt for undecryptable messages.
                    // Per whatsmeow's implementation, delivery receipts are only sent for
                    // successfully decrypted/handled messages. Sending a receipt here would
                    // tell the server we processed it, incrementing the offline counter.
                    // The transport <ack> is sufficient for acknowledgment.
                }
                // If session_had_duplicates is true, we silently skip (no warning, no event)
                // because the message was already processed in a previous session
            }
        } else if !session_decrypted_successfully
            && !session_had_duplicates
            && !session_enc_nodes.is_empty()
        {
            // Edge case: message with only msg/pkmsg that failed to decrypt, no skmsg
            warn!(
                "Message {} from {} failed to decrypt and has no group content. Dispatching UndecryptableMessage event.",
                info.id, info.source.sender
            );
            // Dispatch UndecryptableMessage event for messages that failed to decrypt
            // (This should not cause double-dispatching since process_session_enc_batch
            // already returned dispatched_undecryptable=false for this case)
            self.dispatch_undecryptable_event(&info, crate::types::events::DecryptFailMode::Show);
            // Do NOT send delivery receipt - transport ack is sufficient
        }
    }

    pub(crate) async fn process_session_enc_batch(
        self: Arc<Self>,
        enc_nodes: &[&wacore_binary::node::Node],
        info: &MessageInfo,
        sender_encryption_jid: &Jid,
    ) -> (bool, bool, bool) {
        // Returns (any_success, any_duplicate, dispatched_undecryptable)
        use wacore::libsignal::protocol::CiphertextMessage;
        if enc_nodes.is_empty() {
            return (false, false, false);
        }

        // Acquire a per-sender session lock to prevent race conditions when
        // multiple messages from the same sender are processed concurrently.
        // Use the full Signal protocol address string as the lock key so it matches
        // the SignalProtocolStoreAdapter's per-session locks (prevents ratchet counter races).
        let signal_addr_str = sender_encryption_jid.to_protocol_address().to_string();

        let session_mutex = self
            .session_locks
            .get_with(signal_addr_str.clone(), async {
                std::sync::Arc::new(tokio::sync::Mutex::new(()))
            })
            .await;
        let _session_guard = session_mutex.lock().await;

        let mut adapter =
            SignalProtocolStoreAdapter::new(self.persistence_manager.get_device_arc().await);
        let rng = rand::rngs::OsRng;
        let mut any_success = false;
        let mut any_duplicate = false;
        let mut dispatched_undecryptable = false;

        for enc_node in enc_nodes {
            let ciphertext: &[u8] = match &enc_node.content {
                Some(wacore_binary::node::NodeContent::Bytes(b)) => b,
                _ => {
                    log::warn!("Enc node has no byte content (batch session)");
                    continue;
                }
            };
            let enc_type = enc_node.attrs().string("type");
            let padding_version = enc_node.attrs().optional_u64("v").unwrap_or(2) as u8;

            let parsed_message = if enc_type == "pkmsg" {
                match PreKeySignalMessage::try_from(ciphertext) {
                    Ok(m) => CiphertextMessage::PreKeySignalMessage(m),
                    Err(e) => {
                        log::error!("Failed to parse PreKeySignalMessage: {e:?}");
                        continue;
                    }
                }
            } else {
                match SignalMessage::try_from(ciphertext) {
                    Ok(m) => CiphertextMessage::SignalMessage(m),
                    Err(e) => {
                        log::error!("Failed to parse SignalMessage: {e:?}");
                        continue;
                    }
                }
            };

            let signal_address = sender_encryption_jid.to_protocol_address();

            let decrypt_res = message_decrypt(
                &parsed_message,
                &signal_address,
                &mut adapter.session_store,
                &mut adapter.identity_store,
                &mut adapter.pre_key_store,
                &adapter.signed_pre_key_store,
                &mut rng.unwrap_err(),
                UsePQRatchet::No,
            )
            .await;

            match decrypt_res {
                Ok(padded_plaintext) => {
                    any_success = true;
                    if let Err(e) = self
                        .clone()
                        .handle_decrypted_plaintext(
                            &enc_type,
                            &padded_plaintext,
                            padding_version,
                            info,
                        )
                        .await
                    {
                        log::warn!("Failed processing plaintext (batch session): {e:?}");
                    }
                }
                Err(e) => {
                    // Handle DuplicatedMessage: This is expected when messages are redelivered during reconnection
                    if let SignalProtocolError::DuplicatedMessage(chain, counter) = e {
                        log::debug!(
                            "Skipping already-processed message from {} (chain {}, counter {}). This is normal during reconnection.",
                            info.source.sender,
                            chain,
                            counter
                        );
                        // Mark that we saw a duplicate so we can skip skmsg without showing error
                        any_duplicate = true;
                        continue;
                    }
                    // Handle UntrustedIdentity: This happens when a user re-installs WhatsApp or changes devices.
                    // The Signal Protocol's security policy rejects messages from new identity keys by default.
                    // We handle this by clearing the old identity (to trust the new one), then retrying decryption.
                    // IMPORTANT: We do NOT delete the session! When the PreKeySignalMessage is processed,
                    // libsignal's `promote_state` will archive the old session as a "previous state".
                    // This allows us to decrypt any in-flight messages that were encrypted with the old session.
                    if let SignalProtocolError::UntrustedIdentity(ref address) = e {
                        log::warn!(
                            "[msg:{}] Received message from untrusted identity: {}. This typically means the sender re-installed WhatsApp or changed their device. Clearing old identity to trust new key (keeping session for in-flight messages).",
                            info.id,
                            address
                        );

                        // Extract backend handle and address while holding the lock,
                        // then drop the lock before the async I/O to avoid lock contention.
                        let backend = {
                            let device_arc = self.persistence_manager.get_device_arc().await;
                            let device = device_arc.read().await;
                            Arc::clone(&device.backend)
                        };

                        // Delete the old, untrusted identity using the backend.
                        // Use the full protocol address string (including device ID) as the key.
                        // NOTE: We intentionally do NOT delete the session here. The session will be
                        // archived (not deleted) when the new PreKeySignalMessage is processed,
                        // allowing decryption of any in-flight messages encrypted with the old session.
                        let address_str = address.to_string();
                        if let Err(err) = backend.delete_identity(&address_str).await {
                            log::warn!("Failed to delete old identity for {}: {:?}", address, err);
                        } else {
                            log::info!("Successfully cleared old identity for {}", address);
                        }

                        // Re-attempt decryption with the new identity
                        log::info!(
                            "[msg:{}] Retrying message decryption for {} after clearing untrusted identity",
                            info.id,
                            address
                        );

                        let retry_decrypt_res = message_decrypt(
                            &parsed_message,
                            &signal_address,
                            &mut adapter.session_store,
                            &mut adapter.identity_store,
                            &mut adapter.pre_key_store,
                            &adapter.signed_pre_key_store,
                            &mut rng.unwrap_err(),
                            UsePQRatchet::No,
                        )
                        .await;

                        match retry_decrypt_res {
                            Ok(padded_plaintext) => {
                                log::info!(
                                    "[msg:{}] Successfully decrypted message from {} after handling untrusted identity",
                                    info.id,
                                    address
                                );
                                any_success = true;
                                if let Err(e) = self
                                    .clone()
                                    .handle_decrypted_plaintext(
                                        &enc_type,
                                        &padded_plaintext,
                                        padding_version,
                                        info,
                                    )
                                    .await
                                {
                                    log::warn!(
                                        "Failed processing plaintext after identity retry: {e:?}"
                                    );
                                }
                            }
                            Err(retry_err) => {
                                // Handle DuplicatedMessage in retry path: This commonly happens during reconnection
                                // when the same message is redelivered by the server after we already processed it.
                                // The first attempt triggered UntrustedIdentity, we cleared the session, but meanwhile
                                // another message from the same sender re-established the session and consumed the counter.
                                // This is benign - the message was already successfully processed.
                                if let SignalProtocolError::DuplicatedMessage(chain, counter) =
                                    retry_err
                                {
                                    log::debug!(
                                        "Message from {} was already processed (chain {}, counter {}) - detected during untrusted identity retry. This is normal during reconnection.",
                                        address,
                                        chain,
                                        counter
                                    );
                                    any_duplicate = true;
                                } else if matches!(retry_err, SignalProtocolError::InvalidPreKeyId)
                                {
                                    // InvalidPreKeyId after identity change means the sender is using
                                    // an old prekey that we no longer have. This typically happens when:
                                    // 1. The sender reinstalled WhatsApp and cached our old prekey bundle
                                    // 2. The prekey they're using has been consumed or rotated out
                                    //
                                    // Solution: Send a retry receipt with a fresh prekey so the sender
                                    // can establish a new session and resend the message.
                                    log::warn!(
                                        "[msg:{}] Decryption failed for {} due to InvalidPreKeyId after identity change. \
                                         The sender is using an old prekey we no longer have. \
                                         Sending retry receipt with fresh keys.",
                                        info.id,
                                        address
                                    );

                                    // Send retry receipt so the sender fetches our new prekey bundle
                                    dispatched_undecryptable = self
                                        .handle_decrypt_failure(info, RetryReason::InvalidKeyId);
                                } else {
                                    log::error!(
                                        "[msg:{}] Decryption failed even after clearing untrusted identity for {}: {:?}",
                                        info.id,
                                        address,
                                        retry_err
                                    );
                                    // Send retry receipt so the sender resends with a PreKeySignalMessage
                                    // to establish a new session with the new identity
                                    dispatched_undecryptable =
                                        self.handle_decrypt_failure(info, RetryReason::InvalidKey);
                                }
                            }
                        }
                        continue;
                    }
                    // Handle SessionNotFound gracefully - send retry receipt to request session establishment
                    if let SignalProtocolError::SessionNotFound(_) = e {
                        warn!(
                            "[msg:{}] No session found for {} message from {}. Sending retry receipt to request session establishment.",
                            info.id, enc_type, info.source.sender
                        );
                        // Send retry receipt so the sender resends with a PreKeySignalMessage
                        dispatched_undecryptable =
                            self.handle_decrypt_failure(info, RetryReason::NoSession);
                        continue;
                    } else if matches!(e, SignalProtocolError::InvalidMessage(_, _)) {
                        // InvalidMessage typically means MAC verification failed or session is out of sync.
                        // This happens when the sender's session state diverged from ours (e.g., they reinstalled).
                        // We need to:
                        // 1. Delete the stale session so a new one can be established
                        // 2. Send a retry receipt so the sender resends with a PreKeySignalMessage
                        log::warn!(
                            "[msg:{}] Decryption failed for {} message from {} due to InvalidMessage (likely MAC failure). \
                             Deleting stale session and sending retry receipt.",
                            info.id,
                            enc_type,
                            info.source.sender
                        );

                        // Delete the stale session
                        let device_arc = self.persistence_manager.get_device_arc().await;
                        let device_guard = device_arc.write().await;
                        let address_str = signal_address.to_string();
                        if let Err(err) = device_guard.backend.delete_session(&address_str).await {
                            log::warn!(
                                "Failed to delete stale session for {}: {:?}",
                                signal_address,
                                err
                            );
                        } else {
                            log::info!(
                                "Deleted stale session for {} to allow re-establishment",
                                signal_address
                            );
                        }
                        drop(device_guard);

                        // Send retry receipt so the sender resends with a PreKeySignalMessage
                        dispatched_undecryptable =
                            self.handle_decrypt_failure(info, RetryReason::InvalidMessage);
                        continue;
                    } else if matches!(e, SignalProtocolError::InvalidPreKeyId) {
                        // InvalidPreKeyId means the sender is using a PreKey ID that we don't have.
                        // This typically happens when:
                        // 1. We were offline for a long time
                        // 2. The sender established a session with us using a prekey from the server
                        // 3. We never received the initial session-establishing message
                        // 4. Now we're receiving messages with counters 3, 4, 5... referencing that prekey
                        //
                        // The sender thinks they have a valid session, but we never had it.
                        // We need to send a retry receipt with fresh prekeys so the sender can:
                        // 1. Delete their old session
                        // 2. Fetch our new prekeys from the retry receipt
                        // 3. Create a NEW session and resend with counter 0
                        log::warn!(
                            "[msg:{}] Decryption failed for {} message from {} due to InvalidPreKeyId. \
                             Sender is using a prekey we don't have (likely session established while offline). \
                             Sending retry receipt with fresh prekeys.",
                            info.id,
                            enc_type,
                            info.source.sender
                        );

                        // Send retry receipt with fresh prekeys
                        dispatched_undecryptable =
                            self.handle_decrypt_failure(info, RetryReason::InvalidKeyId);
                        continue;
                    } else {
                        // For other unexpected errors, just log them
                        log::error!(
                            "[msg:{}] Batch session decrypt failed (type: {}) from {}: {:?}",
                            info.id,
                            enc_type,
                            info.source.sender,
                            e
                        );
                        continue;
                    }
                }
            }
        }
        (any_success, any_duplicate, dispatched_undecryptable)
    }

    async fn process_group_enc_batch(
        self: Arc<Self>,
        enc_nodes: &[&wacore_binary::node::Node],
        info: &MessageInfo,
        _sender_encryption_jid: &Jid,
    ) -> Result<(), DecryptionError> {
        if enc_nodes.is_empty() {
            return Ok(());
        }
        let device_arc = self.persistence_manager.get_device_arc().await;

        for enc_node in enc_nodes {
            let ciphertext: &[u8] = match &enc_node.content {
                Some(wacore_binary::node::NodeContent::Bytes(b)) => b,
                _ => {
                    log::warn!("Enc node has no byte content (batch group)");
                    continue;
                }
            };
            let padding_version = enc_node.attrs().optional_u64("v").unwrap_or(2) as u8;

            // CRITICAL: Use info.source.sender (display JID) for sender key operations, NOT sender_encryption_jid.
            // The sender key is stored under the sender's display JID (e.g., LID), while sender_encryption_jid
            // is the phone number used for E2E session decryption only.
            // Using sender_encryption_jid here causes "No sender key state" errors for self-sent LID messages.
            let sender_address = info.source.sender.to_protocol_address();
            let sender_key_name =
                SenderKeyName::new(info.source.chat.to_string(), sender_address.to_string());

            log::debug!(
                "Looking up sender key for group {} with sender address {} (from sender JID: {})",
                info.source.chat,
                sender_address,
                info.source.sender
            );

            let decrypt_result = {
                let mut device_guard = device_arc.write().await;
                group_decrypt(ciphertext, &mut *device_guard, &sender_key_name).await
            };

            match decrypt_result {
                Ok(padded_plaintext) => {
                    if let Err(e) = self
                        .clone()
                        .handle_decrypted_plaintext(
                            "skmsg",
                            &padded_plaintext,
                            padding_version,
                            info,
                        )
                        .await
                    {
                        log::warn!("Failed processing group plaintext (batch): {e:?}");
                    }
                }
                Err(SignalProtocolError::DuplicatedMessage(iteration, counter)) => {
                    log::debug!(
                        "Skipping already-processed sender key message from {} in group {} (iteration {}, counter {}). This is normal during reconnection.",
                        info.source.sender,
                        info.source.chat,
                        iteration,
                        counter
                    );
                    // This is expected when messages are redelivered, just continue silently
                }
                Err(SignalProtocolError::NoSenderKeyState(msg)) => {
                    warn!(
                        "No sender key state for batched group message from {}: {}. Sending retry receipt.",
                        info.source.sender, msg
                    );
                    // Use spawn_retry_receipt which has retry count tracking
                    // NoSenderKeyState is similar to NoSession - we need the SKDM
                    self.spawn_retry_receipt(info, RetryReason::NoSession);
                }
                Err(e) => {
                    log::error!(
                        "Group batch decrypt failed for group {} sender {}: {:?}",
                        sender_key_name.group_id(),
                        sender_key_name.sender_id(),
                        e
                    );
                }
            }
        }
        Ok(())
    }

    async fn handle_decrypted_plaintext(
        self: Arc<Self>,
        enc_type: &str,
        padded_plaintext: &[u8],
        padding_version: u8,
        info: &MessageInfo,
    ) -> Result<(), anyhow::Error> {
        // Send delivery receipt immediately in the background.
        // This should not block further message processing.
        let client_clone = self.clone();
        let info_clone = info.clone();
        tokio::spawn(async move {
            client_clone.send_delivery_receipt(&info_clone).await;
        });

        let plaintext_slice = MessageUtils::unpad_message_ref(padded_plaintext, padding_version)?;
        log::info!(
            "Successfully decrypted message from {}: {} bytes (type: {}) [batch path]",
            info.source.sender,
            plaintext_slice.len(),
            enc_type
        );

        if enc_type == "skmsg" {
            match wa::Message::decode(plaintext_slice) {
                Ok(group_msg) => {
                    self.core
                        .event_bus
                        .dispatch(&Event::Message(Box::new(group_msg), info.clone()));
                }
                Err(e) => log::warn!("Failed to unmarshal decrypted skmsg plaintext: {e}"),
            }
        } else {
            match wa::Message::decode(plaintext_slice) {
                Ok(mut original_msg) => {
                    if let Some(skdm) = &original_msg.sender_key_distribution_message
                        && let Some(axolotl_bytes) = &skdm.axolotl_sender_key_distribution_message
                    {
                        self.handle_sender_key_distribution_message(
                            &info.source.chat,
                            &info.source.sender,
                            axolotl_bytes,
                        )
                        .await;
                    }

                    if let Some(protocol_msg) = &original_msg.protocol_message
                        && let Some(keys) = &protocol_msg.app_state_sync_key_share
                    {
                        self.handle_app_state_sync_key_share(keys).await;
                    }

                    // Handle PDO (Peer Data Operation) responses from our primary phone
                    if let Some(protocol_msg) = &original_msg.protocol_message
                        && let Some(pdo_response) =
                            &protocol_msg.peer_data_operation_request_response_message
                    {
                        self.handle_pdo_response(pdo_response, info).await;
                    }

                    // Take ownership of history_sync_notification to avoid cloning large inline payload
                    let history_sync_taken = original_msg
                        .protocol_message
                        .as_mut()
                        .and_then(|pm| pm.history_sync_notification.take());

                    if let Some(history_sync) = history_sync_taken {
                        log::info!(
                            "Received HistorySyncNotification, dispatching for download and processing."
                        );
                        let client_clone = self.clone();
                        let msg_id = info.id.clone();
                        tokio::spawn(async move {
                            // Enqueue history sync task to dedicated worker
                            // history_sync is moved, not cloned - avoids copying large inline payload
                            client_clone.handle_history_sync(msg_id, history_sync).await;
                        });
                    }

                    self.core
                        .event_bus
                        .dispatch(&Event::Message(Box::new(original_msg), info.clone()));
                }
                Err(e) => log::warn!("Failed to unmarshal decrypted pkmsg/msg plaintext: {e}"),
            }
        }
        Ok(())
    }

    pub(crate) async fn parse_message_info(
        &self,
        node: &Node,
    ) -> Result<MessageInfo, anyhow::Error> {
        let mut attrs = node.attrs();
        let device_snapshot = self.persistence_manager.get_device_snapshot().await;
        let own_jid = device_snapshot.pn.clone().unwrap_or_default();
        let own_lid = device_snapshot.lid.clone();
        let from = attrs.jid("from");

        let mut source = if from.server == wacore_binary::jid::BROADCAST_SERVER {
            // This is the new logic block for handling all broadcast messages, including status.
            let participant = attrs.jid("participant");
            let is_from_me = participant.matches_user_or_lid(&own_jid, own_lid.as_ref());

            crate::types::message::MessageSource {
                chat: from.clone(),
                sender: participant.clone(),
                is_from_me,
                is_group: true, // Treat as group-like for session handling
                broadcast_list_owner: if from.user != wacore_binary::jid::STATUS_BROADCAST_USER {
                    Some(participant.clone())
                } else {
                    None
                },
                ..Default::default()
            }
        } else if from.is_group() {
            let sender = attrs.jid("participant");
            let sender_alt = if let Some(addressing_mode) = attrs
                .optional_string("addressing_mode")
                .map(|s| s.to_ascii_lowercase())
            {
                match addressing_mode.as_str() {
                    "lid" => attrs.optional_jid("participant_pn"),
                    _ => attrs.optional_jid("participant_lid"),
                }
            } else {
                None
            };

            let is_from_me = sender.matches_user_or_lid(&own_jid, own_lid.as_ref());

            crate::types::message::MessageSource {
                chat: from.clone(),
                sender: sender.clone(),
                is_from_me,
                is_group: true,
                sender_alt,
                ..Default::default()
            }
        } else if from.matches_user_or_lid(&own_jid, own_lid.as_ref()) {
            // DM from self (either via PN or LID)
            // Note: peer_recipient_pn contains the RECIPIENT's PN, not sender's.
            // For self-sent messages, we don't set sender_alt here - the decryption
            // logic will use our own PN via the is_from_me fallback path.
            // We store the original `recipient` attribute for retry receipts - this is needed
            // because device sync messages may have a different recipient than our device,
            // and the sender needs this to look up the original message.
            let recipient = attrs.optional_jid("recipient");
            // chat uses non-AD format for session routing, recipient keeps original for retry receipts
            let chat = recipient
                .as_ref()
                .map(|r| r.to_non_ad())
                .unwrap_or_else(|| from.to_non_ad());
            crate::types::message::MessageSource {
                chat,
                sender: from.clone(),
                is_from_me: true,
                recipient,
                // sender_alt stays None - decryption uses own PN for self-sent messages
                ..Default::default()
            }
        } else {
            // DM from someone else
            // Look for alternate JID attribute based on sender type:
            // - For LID senders: look for sender_pn to get their phone number
            // - For PN senders: look for sender_lid to get their LID
            // This is needed because sessions may be stored under either format
            // depending on how the session was originally established.
            let sender_alt = if from.server == wacore_binary::jid::HIDDEN_USER_SERVER {
                // Sender is LID, look for their phone number
                attrs.optional_jid("sender_pn")
            } else {
                // Sender is phone number, look for their LID
                attrs.optional_jid("sender_lid")
            };

            crate::types::message::MessageSource {
                chat: from.to_non_ad(),
                sender: from.clone(),
                is_from_me: false,
                sender_alt,
                ..Default::default()
            }
        };

        source.addressing_mode = attrs
            .optional_string("addressing_mode")
            .map(|s| s.to_ascii_lowercase())
            .and_then(|s| match s.as_str() {
                "pn" => Some(crate::types::message::AddressingMode::Pn),
                "lid" => Some(crate::types::message::AddressingMode::Lid),
                _ => None,
            });

        // Parse the category attribute - this is used for peer device messages ("peer")
        // and is critical for proper retry receipt handling.
        let category = attrs
            .optional_string("category")
            .map(|s| s.to_string())
            .unwrap_or_default();

        Ok(MessageInfo {
            source,
            id: attrs.string("id"),
            push_name: attrs
                .optional_string("notify")
                .map(|s| s.to_string())
                .unwrap_or_default(),
            timestamp: DateTime::from_timestamp(attrs.unix_time("t"), 0)
                .unwrap_or_else(chrono::Utc::now),
            category,
            ..Default::default()
        })
    }

    pub(crate) async fn handle_app_state_sync_key_share(
        &self,
        keys: &wa::message::AppStateSyncKeyShare,
    ) {
        struct KeyComponents<'a> {
            key_id: &'a [u8],
            data: &'a [u8],
            fingerprint_bytes: Vec<u8>,
            timestamp: i64,
        }

        /// Extract components from an AppStateSyncKey for storage.
        fn extract_key_components(key: &wa::message::AppStateSyncKey) -> Option<KeyComponents<'_>> {
            let key_id = key.key_id.as_ref()?.key_id.as_ref()?;
            let key_data = key.key_data.as_ref()?;
            let fingerprint = key_data.fingerprint.as_ref()?;
            let data = key_data.key_data.as_ref()?;
            Some(KeyComponents {
                key_id,
                data,
                fingerprint_bytes: fingerprint.encode_to_vec(),
                timestamp: key_data.timestamp(),
            })
        }

        let device_snapshot = self.persistence_manager.get_device_snapshot().await;
        let key_store = device_snapshot.backend.clone();

        let mut stored_count = 0;
        let mut failed_count = 0;

        for key in &keys.keys {
            if let Some(components) = extract_key_components(key) {
                let new_key = crate::store::traits::AppStateSyncKey {
                    key_data: components.data.to_vec(),
                    fingerprint: components.fingerprint_bytes,
                    timestamp: components.timestamp,
                };

                if let Err(e) = key_store.set_sync_key(components.key_id, new_key).await {
                    log::error!(
                        "Failed to store app state sync key {:?}: {:?}",
                        hex::encode(components.key_id),
                        e
                    );
                    failed_count += 1;
                } else {
                    stored_count += 1;
                }
            }
        }

        if stored_count > 0 || failed_count > 0 {
            log::info!(
                target: "Client/AppState",
                "Processed app state key share: {} stored, {} failed.",
                stored_count,
                failed_count
            );
        }

        // Notify any waiters (initial full sync) that at least one key share was processed.
        if stored_count > 0
            && !self
                .initial_app_state_keys_received
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            // First time setting; notify any waiters
            self.initial_keys_synced_notifier.notify_waiters();
        }
    }

    async fn handle_sender_key_distribution_message(
        self: &Arc<Self>,
        group_jid: &Jid,
        sender_jid: &Jid,
        axolotl_bytes: &[u8],
    ) {
        let skdm = match SenderKeyDistributionMessage::try_from(axolotl_bytes) {
            Ok(msg) => msg,
            Err(e1) => match wa::SenderKeyDistributionMessage::decode(axolotl_bytes) {
                Ok(go_msg) => {
                    let (Some(signing_key), Some(id), Some(iteration), Some(chain_key)) = (
                        go_msg.signing_key.as_ref(),
                        go_msg.id,
                        go_msg.iteration,
                        go_msg.chain_key.as_ref(),
                    ) else {
                        log::warn!(
                            "Go SKDM from {} missing required fields (signing_key={}, id={}, iteration={}, chain_key={})",
                            sender_jid,
                            go_msg.signing_key.is_some(),
                            go_msg.id.is_some(),
                            go_msg.iteration.is_some(),
                            go_msg.chain_key.is_some()
                        );
                        return;
                    };
                    match SignalPublicKey::from_djb_public_key_bytes(signing_key) {
                        Ok(pub_key) => {
                            match SenderKeyDistributionMessage::new(
                                SENDERKEY_MESSAGE_CURRENT_VERSION,
                                id,
                                iteration,
                                chain_key.clone(),
                                pub_key,
                            ) {
                                Ok(skdm) => skdm,
                                Err(e) => {
                                    log::error!(
                                        "Failed to construct SKDM from Go format from {}: {:?} (original parse error: {:?})",
                                        sender_jid,
                                        e,
                                        e1
                                    );
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to parse public key from Go SKDM for {}: {:?} (original parse error: {:?})",
                                sender_jid,
                                e,
                                e1
                            );
                            return;
                        }
                    }
                }
                Err(e2) => {
                    log::error!(
                        "Failed to parse SenderKeyDistributionMessage (standard and Go fallback) from {}: primary: {:?}, fallback: {:?}",
                        sender_jid,
                        e1,
                        e2
                    );
                    return;
                }
            },
        };

        let device_arc = self.persistence_manager.get_device_arc().await;
        let mut device_guard = device_arc.write().await;

        let sender_address = sender_jid.to_protocol_address();

        let sender_key_name = SenderKeyName::new(group_jid.to_string(), sender_address.to_string());

        if let Err(e) =
            process_sender_key_distribution_message(&sender_key_name, &skdm, &mut *device_guard)
                .await
        {
            log::error!(
                "Failed to process SenderKeyDistributionMessage from {}: {:?}",
                sender_jid,
                e
            );
        } else {
            log::info!(
                "Successfully processed sender key distribution for group {} from {}",
                group_jid,
                sender_jid
            );
        }
    }
}
