use super::traits::StanzaHandler;
use crate::client::Client;
use crate::types::events::Event;
use async_trait::async_trait;
use log::{debug, info, warn};
use std::sync::Arc;
use wacore::store::traits::{DeviceInfo, DeviceListRecord};
use wacore::types::events::{DeviceListUpdate, DeviceListUpdateType};
use wacore_binary::jid::{Jid, JidExt};
use wacore_binary::{jid::SERVER_JID, node::Node};

/// Extract device IDs from child `<device>` elements of a node.
pub(crate) fn extract_device_ids(node: &Node) -> Vec<u32> {
    node.children()
        .map(|device_nodes| {
            device_nodes
                .iter()
                .filter(|n| n.tag == "device")
                .filter_map(|n| n.attrs().optional_u64("id").map(|id| id as u32))
                .collect()
        })
        .unwrap_or_default()
}

/// Handler for `<notification>` stanzas.
///
/// Processes various notification types including:
/// - Encrypt notifications (key upload requests)
/// - Server sync notifications
/// - Account sync notifications (push name updates)
/// - Device notifications (device add/remove/update)
#[derive(Default)]
pub struct NotificationHandler;

#[async_trait]
impl StanzaHandler for NotificationHandler {
    fn tag(&self) -> &'static str {
        "notification"
    }

    async fn handle(&self, client: Arc<Client>, node: Arc<Node>, _cancelled: &mut bool) -> bool {
        handle_notification_impl(&client, &node).await;
        true
    }
}

async fn handle_notification_impl(client: &Arc<Client>, node: &Node) {
    let notification_type = node.attrs().optional_string("type").unwrap_or_default();

    match notification_type {
        "encrypt" => {
            if node.attrs().optional_string("from") == Some(SERVER_JID) {
                let client_clone = client.clone();
                tokio::spawn(async move {
                    if let Err(e) = client_clone.upload_pre_keys().await {
                        warn!("Failed to upload pre-keys after notification: {:?}", e);
                    }
                });
            }
        }
        "server_sync" => {
            // Server sync notifications inform us of app state changes from other devices.
            // For bot use case, we don't need to sync these (pins, mutes, archives, etc.).
            // Just acknowledge without syncing.
            if let Some(children) = node.children() {
                for collection_node in children.iter().filter(|c| c.tag == "collection") {
                    let name = collection_node.attrs().string("name");
                    let version = collection_node.attrs().optional_u64("version").unwrap_or(0);
                    debug!(
                        target: "Client/AppState",
                        "Received server_sync for collection '{}' version {} (not syncing)",
                        name, version
                    );
                }
            }
        }
        "account_sync" => {
            // Handle push name updates
            if let Some(new_push_name) = node.attrs().optional_string("pushname") {
                client
                    .clone()
                    .update_push_name_and_notify(new_push_name.to_string())
                    .await;
            }

            // Handle device list updates (when a new device is paired)
            // Matches WhatsApp Web's handleAccountSyncNotification for DEVICES type
            if let Some(devices_node) = node.get_optional_child_by_tag(&["devices"]) {
                handle_account_sync_devices(client, node, devices_node).await;
            }
        }
        "devices" => {
            // Handle device list change notifications (WhatsApp Web: handleDevicesNotification)
            // These are sent when a user adds, removes, or updates a device
            handle_devices_notification(client, node).await;
        }
        "link_code_companion_reg" => {
            // Handle pair code notification (stage 2 of pair code authentication)
            // This is sent when the user enters the code on their phone
            crate::pair_code::handle_pair_code_notification(client, node).await;
        }
        _ => {
            warn!(target: "Client", "TODO: Implement handler for <notification type='{notification_type}'>");
            client
                .core
                .event_bus
                .dispatch(&Event::Notification(node.clone()));
        }
    }
}

/// Handle device list change notifications.
/// Matches WhatsApp Web's WAWebHandleDeviceNotification.handleDevicesNotification().
///
/// Device notifications have the structure:
/// ```xml
/// <notification type="devices" from="user@s.whatsapp.net">
///   <add> or <remove> or <update hash="...">
///     <device id="1" />
///     <device id="2" />
///   </add/remove/update>
/// </notification>
/// ```
async fn handle_devices_notification(client: &Arc<Client>, node: &Node) {
    // Extract user JID from the "from" attribute
    let from_jid = match node.attrs().optional_jid("from") {
        Some(jid) => jid,
        None => {
            warn!(target: "Client", "Device notification missing 'from' attribute");
            return;
        }
    };

    let user = from_jid.user.clone();

    // Determine update type and extract device list
    let Some(children) = node.children() else {
        warn!(target: "Client", "Device notification has no children");
        return;
    };

    for child in children.iter() {
        let (update_type, hash) = match child.tag.as_str() {
            "add" => (DeviceListUpdateType::Add, None),
            "remove" => (DeviceListUpdateType::Remove, None),
            "update" => {
                let hash = child.attrs().optional_string("hash").map(|s| s.to_string());
                (DeviceListUpdateType::Update, hash)
            }
            _ => continue,
        };

        let devices = extract_device_ids(child);

        debug!(
            target: "Client",
            "Device notification: user={}, type={:?}, devices={:?}, hash={:?}",
            user, update_type, devices, hash
        );

        // Invalidate the device cache for this user
        // This ensures the next lookup fetches fresh data
        client.invalidate_device_cache(&user).await;

        // Dispatch event to notify application layer
        let event = Event::DeviceListUpdate(DeviceListUpdate {
            user: from_jid.clone(),
            update_type,
            devices,
            hash,
        });
        client.core.event_bus.dispatch(&event);
    }
}

/// Parsed device info from account_sync notification
pub(crate) struct AccountSyncDevice {
    pub jid: Jid,
    pub key_index: Option<u32>,
}

/// Parse devices from account_sync notification's <devices> child.
///
/// Example structure:
/// ```xml
/// <devices dhash="2:FnEWjS13">
///   <device jid="15551234567@s.whatsapp.net"/>
///   <device jid="15551234567:64@s.whatsapp.net" key-index="2"/>
///   <key-index-list ts="1766612162"><!-- bytes --></key-index-list>
/// </devices>
/// ```
pub(crate) fn parse_account_sync_device_list(devices_node: &Node) -> Vec<AccountSyncDevice> {
    let Some(children) = devices_node.children() else {
        return Vec::new();
    };

    children
        .iter()
        .filter(|n| n.tag == "device")
        .filter_map(|n| {
            let jid = n.attrs().optional_jid("jid")?;
            let key_index = n.attrs().optional_u64("key-index").map(|v| v as u32);
            Some(AccountSyncDevice { jid, key_index })
        })
        .collect()
}

/// Handle account_sync notification with <devices> child.
///
/// This is sent when devices are added/removed from OUR account (e.g., pairing a new WhatsApp Web).
/// Matches WhatsApp Web's `handleAccountSyncNotification` for `AccountSyncType.DEVICES`.
///
/// Key behaviors:
/// 1. Check if notification is for our own account (isSameAccountAndAddressingMode)
/// 2. Parse device list from notification
/// 3. Update device registry with new device list
/// 4. Does NOT trigger app state sync (that's handled by server_sync)
async fn handle_account_sync_devices(client: &Arc<Client>, node: &Node, devices_node: &Node) {
    // Extract the "from" JID - this is the account the notification is about
    let from_jid = match node.attrs().optional_jid("from") {
        Some(jid) => jid,
        None => {
            warn!(target: "Client/AccountSync", "account_sync devices missing 'from' attribute");
            return;
        }
    };

    // Get our own JIDs (PN and LID) to verify this is about our account
    let device_snapshot = client.persistence_manager.get_device_snapshot().await;
    let own_pn = device_snapshot.pn.as_ref();
    let own_lid = device_snapshot.lid.as_ref();

    // Check if notification is about our own account
    // Matches WhatsApp Web's isSameAccountAndAddressingMode check
    let is_own_account = own_pn.is_some_and(|pn| pn.is_same_user_as(&from_jid))
        || own_lid.is_some_and(|lid| lid.is_same_user_as(&from_jid));

    if !is_own_account {
        // WhatsApp Web logs "wid-is-not-self" error in this case
        warn!(
            target: "Client/AccountSync",
            "Received account_sync devices for non-self user: {} (our PN: {:?}, LID: {:?})",
            from_jid,
            own_pn.map(|j| j.user.as_str()),
            own_lid.map(|j| j.user.as_str())
        );
        return;
    }

    // Parse device list from notification
    let devices = parse_account_sync_device_list(devices_node);
    if devices.is_empty() {
        debug!(target: "Client/AccountSync", "account_sync devices list is empty");
        return;
    }

    // Extract dhash (device hash) for cache validation
    let dhash = devices_node
        .attrs()
        .optional_string("dhash")
        .map(String::from);

    // Get timestamp from notification
    let timestamp = node.attrs().optional_u64("t").unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }) as i64;

    // Build DeviceListRecord for storage
    // Note: update_device_list() will automatically store under LID if mapping is known
    let device_list = DeviceListRecord {
        user: from_jid.user.clone(),
        devices: devices
            .iter()
            .map(|d| DeviceInfo {
                device_id: d.jid.device as u32,
                key_index: d.key_index,
            })
            .collect(),
        timestamp,
        phash: dhash,
    };

    if let Err(e) = client.update_device_list(device_list).await {
        warn!(
            target: "Client/AccountSync",
            "Failed to update device list from account_sync: {}",
            e
        );
        return;
    }

    info!(
        target: "Client/AccountSync",
        "Updated own device list from account_sync: {} devices (user: {})",
        devices.len(),
        from_jid.user
    );

    // Log individual devices at debug level
    for device in &devices {
        debug!(
            target: "Client/AccountSync",
            "  Device: {} (key-index: {:?})",
            device.jid,
            device.key_index
        );
    }
}
