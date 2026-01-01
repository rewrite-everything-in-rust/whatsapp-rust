pub use wacore::types::events::*;

pub mod message {
    pub use wacore::types::events::{
        DecryptFailMode, Receipt, UnavailableType, UndecryptableMessage,
    };
    pub use wacore::types::presence::ReceiptType;
}

pub mod connection {
    pub use wacore::types::events::{
        ConnectFailure, ConnectFailureReason, Connected, Disconnected, LoggedOut, StreamError,
        StreamReplaced,
    };
}

pub mod pairing {
    pub use wacore::types::events::{
        ClientOutdated, PairError, PairSuccess, QrScannedWithoutMultidevice,
    };
}

pub mod presence {
    pub use wacore::types::events::{ChatPresenceUpdate, PresenceUpdate};
}

pub mod profile {
    pub use wacore::types::events::{
        PictureUpdate, PushNameUpdate, SelfPushNameUpdated, UserAboutUpdate,
    };
}

pub mod chat {
    pub use wacore::types::events::{
        ArchiveUpdate, ContactUpdate, MarkChatAsReadUpdate, MuteUpdate, PinUpdate,
    };
}

pub mod sync {
    pub use wacore::types::events::{LazyConversation, OfflineSyncCompleted, OfflineSyncPreview};
    pub use waproto::whatsapp::HistorySync;
}

pub mod device {
    pub use wacore::types::events::{DeviceListUpdate, DeviceListUpdateType};
}

pub mod ban {
    pub use wacore::types::events::{TempBanReason, TemporaryBan};
}
