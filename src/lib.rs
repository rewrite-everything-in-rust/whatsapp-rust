pub use wacore::{proto_helpers, store::traits};
pub use wacore_binary::builder::NodeBuilder;
pub use waproto;

pub mod domain;
pub mod types;

pub mod client;
pub use client::Client;
pub mod auth;
pub use auth::{handshake, pair, pair_code, prekeys, session};

pub mod sync;
pub use sync::{appstate_sync, history_sync, sync_task, usync};

pub mod messaging;
pub use messaging::{download, message, receipt, send, upload};
pub use send::SendOptions;

pub mod net;
pub use net::{keepalive, mediaconn, request, retry, transport};

pub mod handlers;
pub mod pdo;
pub mod socket;
pub mod store;

pub mod utils;
pub use utils::{http, jid_utils, logger, version};

pub mod features;
pub use features::{
    Blocking, BlocklistEntry, ChatStateType, Chatstate, Contacts, Groups, Mex, MexError,
    MexErrorExtensions, MexGraphQLError, MexRequest, MexResponse, Presence,
};

// Re-export domain types for convenience
pub use domain::{
    AddressingMode, ContactInfo, Event, GroupMetadata, GroupParticipant, IsOnWhatsAppResult,
    MessageInfo, MessageSource, ProfilePicture, UserInfo,
};

pub mod bot;
pub mod lid_pn_cache;
pub mod spam_report;

pub use spam_report::{SpamFlow, SpamReportRequest, SpamReportResult};

#[cfg(test)]
pub mod test_utils;
