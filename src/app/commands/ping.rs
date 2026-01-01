use chrono::Utc;
use log::{error, info};
use waproto::whatsapp as wa;
use whatsapp_rust::bot::MessageContext;

pub async fn run(ctx: &MessageContext) {
    info!("Received text ping, sending pong...");

    // Send reaction to the ping message
    let message_key = wa::MessageKey {
        remote_jid: Some(ctx.info.source.chat.to_string()),
        id: Some(ctx.info.id.clone()),
        from_me: Some(ctx.info.source.is_from_me),
        participant: if ctx.info.source.is_group {
            Some(ctx.info.source.sender.to_string())
        } else {
            None
        },
    };

    let reaction_emoji = "🏓".to_string();

    let reaction_message = wa::message::ReactionMessage {
        key: Some(message_key),
        text: Some(reaction_emoji),
        sender_timestamp_ms: Some(Utc::now().timestamp_millis()),
        ..Default::default()
    };

    let final_message_to_send = wa::Message {
        reaction_message: Some(reaction_message),
        ..Default::default()
    };

    if let Err(e) = ctx.send_message(final_message_to_send).await {
        error!("Failed to send reaction: {}", e);
    }

    let start = std::time::Instant::now();

    // Determine participant JID
    let participant_jid = if ctx.info.source.is_from_me {
        ctx.client.get_pn().await.unwrap_or_default().to_string()
    } else {
        ctx.info.source.sender.to_string()
    };

    // Construct ContextInfo for quoting
    let context_info = wa::ContextInfo {
        stanza_id: Some(ctx.info.id.clone()),
        participant: Some(participant_jid),
        quoted_message: Some(ctx.message.clone()),
        ..Default::default()
    };

    // Create the initial quoted reply message
    let reply_message = wa::Message {
        extended_text_message: Some(Box::new(wa::message::ExtendedTextMessage {
            text: Some("🏓 Pong!🏓 ".to_string()),
            context_info: Some(Box::new(context_info.clone())),
            ..Default::default()
        })),
        ..Default::default()
    };

    // 1. Send the initial message and get its ID
    let sent_msg_id = match ctx.send_message(reply_message).await {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to send initial pong message: {}", e);
            return;
        }
    };

    // 2. Calculate the duration
    let duration = start.elapsed();
    let duration_str = format!("{:.2?}", duration);

    info!(
        "Send took {}. Editing message {}...",
        duration_str, &sent_msg_id
    );

    // 3. Create the new content for the message
    let updated_content = wa::Message {
        extended_text_message: Some(Box::new(wa::message::ExtendedTextMessage {
            text: Some(format!("🏓 Pong!\n`{}`", duration_str)),
            context_info: Some(Box::new(context_info)),
            ..Default::default()
        })),
        ..Default::default()
    };

    // 4. Edit the original message with the new content
    if let Err(e) = ctx.edit_message(sent_msg_id.clone(), updated_content).await {
        error!("Failed to edit message {}: {}", sent_msg_id, e);
    } else {
        info!("Successfully sent edit for message {}.", sent_msg_id);
    }
}
