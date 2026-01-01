use crate::client::Client;
use crate::types::events::{Event, Receipt};
use crate::types::presence::ReceiptType;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use wacore_binary::builder::NodeBuilder;
use wacore_binary::jid::JidExt as _;

use wacore_binary::node::Node;

impl Client {
    pub(crate) async fn handle_receipt(self: &Arc<Self>, node: Arc<Node>) {
        let mut attrs = node.attrs();
        let from = attrs.jid("from");
        let id = attrs.string("id");
        let receipt_type_str = attrs.optional_string("type").unwrap_or("delivery");
        let participant = attrs.optional_jid("participant");

        let receipt_type = ReceiptType::from(receipt_type_str.to_string());

        info!("Received receipt type '{receipt_type:?}' for message {id} from {from}");

        let from_clone = from.clone();
        let sender = if from.is_group() {
            if let Some(participant) = participant {
                participant
            } else {
                from_clone
            }
        } else {
            from.clone()
        };

        let receipt = Receipt {
            message_ids: vec![id.clone()],
            source: crate::types::message::MessageSource {
                chat: from.clone(),
                sender: sender.clone(),
                ..Default::default()
            },
            timestamp: chrono::Utc::now(),
            r#type: receipt_type.clone(),
            message_sender: sender.clone(),
        };

        if receipt_type == ReceiptType::Retry {
            let client_clone = Arc::clone(self);
            // Arc clone is cheap - just reference count increment
            let node_clone = Arc::clone(&node);
            tokio::spawn(async move {
                if let Err(e) = client_clone
                    .handle_retry_receipt(&receipt, &node_clone)
                    .await
                {
                    log::warn!(
                        "Failed to handle retry receipt for {}: {:?}",
                        receipt.message_ids[0],
                        e
                    );
                }
            });
        } else {
            self.core.event_bus.dispatch(&Event::Receipt(receipt));
        }
    }

    /// Sends a delivery receipt to the sender of a message.
    ///
    /// This function handles:
    /// - Direct messages (DMs) - sends receipt to the sender's JID.
    /// - Group messages - sends receipt to the group JID with the sender as a participant.
    /// - It correctly skips sending receipts for self-sent messages, status broadcasts, or messages without an ID.
    pub(crate) async fn send_delivery_receipt(&self, info: &crate::types::message::MessageInfo) {
        use wacore_binary::jid::STATUS_BROADCAST_USER;

        // Don't send receipts for our own messages, status broadcasts, or if ID is missing.
        if info.source.is_from_me
            || info.id.is_empty()
            || info.source.chat.user == STATUS_BROADCAST_USER
        {
            return;
        }

        let mut attrs = HashMap::new();
        attrs.insert("id".to_string(), info.id.clone());
        // The 'to' attribute is always the JID from which the message originated (the chat JID for groups).
        attrs.insert("to".to_string(), info.source.chat.to_string());
        attrs.insert("type".to_string(), "delivery".to_string());

        // For group messages, the 'participant' attribute is required to identify the sender.
        if info.source.is_group {
            attrs.insert("participant".to_string(), info.source.sender.to_string());
        }

        let receipt_node = NodeBuilder::new("receipt").attrs(attrs).build();

        info!(target: "Client/Receipt", "Sending delivery receipt for message {} to {}", info.id, info.source.sender);

        if let Err(e) = self.send_node(receipt_node).await {
            log::warn!(target: "Client/Receipt", "Failed to send delivery receipt for message {}: {:?}", info.id, e);
        }
    }
}
