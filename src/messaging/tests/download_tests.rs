use super::super::download::*;
use std::io::{Cursor, Write};

#[test]
fn process_downloaded_media_ok() {
    let data = b"Hello media test";
    let enc =
        wacore::upload::encrypt_media(data, MediaType::Image).expect("encryption should succeed");
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let plaintext =
        DownloadUtils::verify_and_decrypt(&enc.data_to_upload, &enc.media_key, MediaType::Image)
            .expect("decryption should succeed");
    cursor.write_all(&plaintext).expect("write should succeed");
    assert_eq!(cursor.into_inner(), data);
}

#[test]
fn process_downloaded_media_bad_mac() {
    let data = b"Tamper";
    let mut enc =
        wacore::upload::encrypt_media(data, MediaType::Image).expect("encryption should succeed");
    let last = enc.data_to_upload.len() - 1;
    enc.data_to_upload[last] ^= 0x01;

    let err =
        DownloadUtils::verify_and_decrypt(&enc.data_to_upload, &enc.media_key, MediaType::Image)
            .unwrap_err();

    assert!(err.to_string().to_lowercase().contains("invalid mac"));
}
