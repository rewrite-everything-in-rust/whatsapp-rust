mod blocking;
mod chatstate;
mod contacts;
mod groups;
mod mex;
mod presence;

#[cfg(test)]
mod tests;

pub use blocking::{Blocking, BlocklistEntry};

pub use chatstate::{ChatStateType, Chatstate};

pub use contacts::Contacts;

pub use groups::Groups;

pub use mex::{Mex, MexError, MexErrorExtensions, MexGraphQLError, MexRequest, MexResponse};

pub use presence::Presence;

// Re-export domain entities that are used by features
pub use crate::domain::{
    ContactInfo, GroupMetadata, GroupParticipant, IsOnWhatsAppResult, ProfilePicture, UserInfo,
};

// PresenceStatus needs to stay here for now until we fully refactor presence
pub use presence::PresenceStatus;
