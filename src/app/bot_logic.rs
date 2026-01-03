use log::{error, info};
use std::io::Cursor;
use wacore::download::{Downloadable, MediaType};
use wacore::proto_helpers::MessageExt;
use waproto::whatsapp as wa;
use whatsapp_rust::bot::MessageContext;
use whatsapp_rust::upload::UploadResponse;

use crate::app::commands;

pub async fn handle_message(ctx: MessageContext) {
    if let Some(text) = ctx.message.text_content() {
        if commands::handle_command(&ctx, text).await {
            return;
        }
    }

    if let Some(media_ping_request) = get_pingable_media(&ctx.message) {
        handle_media_ping(&ctx, media_ping_request).await;
    }
}

trait MediaPing: Downloadable {
    fn media_type(&self) -> MediaType;

    fn build_pong_reply(&self, upload: UploadResponse) -> wa::Message;
}

impl MediaPing for wa::message::ImageMessage {
    fn media_type(&self) -> MediaType {
        MediaType::Image
    }

    fn build_pong_reply(&self, upload: UploadResponse) -> wa::Message {
        wa::Message {
            image_message: Some(Box::new(wa::message::ImageMessage {
                mimetype: self.mimetype.clone(),
                caption: Some("pong".to_string()),
                url: Some(upload.url),
                direct_path: Some(upload.direct_path),
                media_key: Some(upload.media_key),
                file_enc_sha256: Some(upload.file_enc_sha256),
                file_sha256: Some(upload.file_sha256),
                file_length: Some(upload.file_length),
                ..Default::default()
            })),
            ..Default::default()
        }
    }
}

impl MediaPing for wa::message::VideoMessage {
    fn media_type(&self) -> MediaType {
        MediaType::Video
    }

    fn build_pong_reply(&self, upload: UploadResponse) -> wa::Message {
        wa::Message {
            video_message: Some(Box::new(wa::message::VideoMessage {
                mimetype: self.mimetype.clone(),
                caption: Some("pong".to_string()),
                url: Some(upload.url),
                direct_path: Some(upload.direct_path),
                media_key: Some(upload.media_key),
                file_enc_sha256: Some(upload.file_enc_sha256),
                file_sha256: Some(upload.file_sha256),
                file_length: Some(upload.file_length),
                gif_playback: self.gif_playback,
                height: self.height,
                width: self.width,
                seconds: self.seconds,
                gif_attribution: self.gif_attribution,
                ..Default::default()
            })),
            ..Default::default()
        }
    }
}

impl MediaPing for wa::message::StickerMessage {
    fn media_type(&self) -> MediaType {
        MediaType::Sticker
    }

    fn build_pong_reply(&self, upload: UploadResponse) -> wa::Message {
        wa::Message {
            sticker_message: Some(Box::new(wa::message::StickerMessage {
                url: Some(upload.url),
                direct_path: Some(upload.direct_path),
                media_key: Some(upload.media_key),
                file_enc_sha256: Some(upload.file_enc_sha256),
                file_sha256: Some(upload.file_sha256),
                file_length: Some(upload.file_length),
                mimetype: self.mimetype.clone(),
                height: self.height,
                width: self.width,
                is_animated: self.is_animated,
                ..Default::default()
            })),
            ..Default::default()
        }
    }
}

fn get_pingable_media<'a>(message: &'a wa::Message) -> Option<&'a (dyn MediaPing + 'a)> {
    let base_message = message.get_base_message();

    if let Some(msg) = &base_message.image_message
        && msg.caption.as_deref() == Some("ping")
    {
        return Some(&**msg);
    }
    if let Some(msg) = &base_message.video_message
        && msg.caption.as_deref() == Some("ping")
    {
        return Some(&**msg);
    }
    if let Some(msg) = &base_message.sticker_message {
        return Some(&**msg);
    }

    None
}

async fn handle_media_ping(ctx: &MessageContext, media: &(dyn MediaPing + '_)) {
    info!(
        "Received {:?} ping from {}",
        media.media_type(),
        ctx.info.source.sender
    );

    let mut data_buffer = Cursor::new(Vec::new());
    if let Err(e) = ctx.client.download_to_file(media, &mut data_buffer).await {
        error!("Failed to download media: {}", e);
        let _ = ctx
            .send_message(wa::Message {
                conversation: Some("Failed to download your media.".to_string()),
                ..Default::default()
            })
            .await;
        return;
    }

    info!(
        "Successfully downloaded media. Size: {} bytes. Now uploading...",
        data_buffer.get_ref().len()
    );
    let plaintext_data = data_buffer.into_inner();
    let upload_response = match ctx.client.upload(plaintext_data, media.media_type()).await {
        Ok(resp) => resp,
        Err(e) => {
            error!("Failed to upload media: {}", e);
            let _ = ctx
                .send_message(wa::Message {
                    conversation: Some("Failed to re-upload the media.".to_string()),
                    ..Default::default()
                })
                .await;
            return;
        }
    };

    info!("Successfully uploaded media. Constructing reply message...");
    let reply_msg = media.build_pong_reply(upload_response);

    if let Err(e) = ctx.send_message(reply_msg).await {
        error!("Failed to send media pong reply: {}", e);
    } else {
        info!("Media pong reply sent successfully.");
    }
}
