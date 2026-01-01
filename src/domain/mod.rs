pub mod entities;
pub mod events;
pub mod value_objects;

// Re-export commonly used domain types
pub use entities::{
    ContactInfo, DeviceSentMeta, EditAttribute, GroupMetadata, GroupParticipant,
    IsOnWhatsAppResult, MessageInfo, MessageSource, MsgBotInfo, MsgMetaInfo, ProfilePicture,
    UserInfo,
};

pub use events::Event;

pub use value_objects::{
    AddressingMode, ChatPresence, ChatPresenceMedia, Jid, JidExt, ReceiptType,
};
