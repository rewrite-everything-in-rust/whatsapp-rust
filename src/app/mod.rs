use crate::app::bot_logic::handle_message;
use log::{debug, error, info};
use wacore::types::events::Event;
use whatsapp_rust::bot::MessageContext;

pub mod bot_logic;
pub mod commands;

pub async fn handle_event(event: Event, client: std::sync::Arc<whatsapp_rust::client::Client>) {
    match event {
        Event::Message(msg, info) => {
            let ctx = MessageContext {
                message: msg,
                info,
                client,
            };
            handle_message(ctx).await;
        }
        Event::Connected(_) => {
            info!("Bot connected successfully!");
        }
        Event::Receipt(receipt) => {
            debug!(
                "Got receipt for message(s) {:?}, type: {:?}",
                receipt.message_ids, receipt.r#type
            );
        }
        Event::LoggedOut(_) => {
            error!("Bot was logged out!");
        }
        Event::PairingQrCode { code, timeout } => {
            info!("----------------------------------------");
            info!(
                "QR code received (valid for {} seconds):",
                timeout.as_secs()
            );
            qr2term::print_qr(&code).unwrap_or_else(|e| {
                error!("Failed to render QR code: {}", e);
            });
            info!("\n{}\n", code);
            info!("----------------------------------------");
        }
        Event::PairingCode { code, timeout } => {
            info!("========================================");
            info!("PAIR CODE (valid for {} seconds):", timeout.as_secs());
            info!("Enter this code on your phone:");
            info!("WhatsApp > Linked Devices > Link a Device");
            info!("> Link with phone number instead");
            info!("");
            info!("    >>> {} <<<", code);
            info!("");
            info!("========================================");
        }
        _ => {}
    }
}
