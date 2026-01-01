use crate::domain::value_objects::AddressingMode;
use wacore_binary::jid::Jid;

#[derive(Debug, Clone)]
pub struct GroupMetadata {
    pub id: Jid,
    pub subject: String,
    pub participants: Vec<GroupParticipant>,
    pub addressing_mode: AddressingMode,
}

#[derive(Debug, Clone)]
pub struct GroupParticipant {
    pub jid: Jid,
    pub phone_number: Option<Jid>,
    pub is_admin: bool,
}

impl GroupMetadata {
    pub fn new(id: Jid, subject: String, addressing_mode: AddressingMode) -> Self {
        Self {
            id,
            subject,
            participants: Vec::new(),
            addressing_mode,
        }
    }

    pub fn add_participant(&mut self, participant: GroupParticipant) {
        self.participants.push(participant);
    }

    pub fn is_participant(&self, jid: &Jid) -> bool {
        self.participants.iter().any(|p| &p.jid == jid)
    }

    pub fn get_participant(&self, jid: &Jid) -> Option<&GroupParticipant> {
        self.participants.iter().find(|p| &p.jid == jid)
    }

    pub fn admins(&self) -> impl Iterator<Item = &GroupParticipant> {
        self.participants.iter().filter(|p| p.is_admin)
    }

    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }
}

impl GroupParticipant {
    pub fn new(jid: Jid, is_admin: bool) -> Self {
        Self {
            jid,
            phone_number: None,
            is_admin,
        }
    }

    pub fn with_phone_number(jid: Jid, phone_number: Jid, is_admin: bool) -> Self {
        Self {
            jid,
            phone_number: Some(phone_number),
            is_admin,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_metadata_new() {
        let jid: Jid = "123456789@g.us".parse().unwrap();
        let metadata =
            GroupMetadata::new(jid.clone(), "Test Group".to_string(), AddressingMode::Pn);

        assert_eq!(metadata.id, jid);
        assert_eq!(metadata.subject, "Test Group");
        assert_eq!(metadata.participants.len(), 0);
        assert_eq!(metadata.addressing_mode, AddressingMode::Pn);
    }

    #[test]
    fn test_group_metadata_add_participant() {
        let jid: Jid = "123456789@g.us".parse().unwrap();
        let participant_jid: Jid = "1234567890@s.whatsapp.net".parse().unwrap();

        let mut metadata = GroupMetadata::new(jid, "Test".to_string(), AddressingMode::Pn);
        metadata.add_participant(GroupParticipant::new(participant_jid.clone(), true));

        assert_eq!(metadata.participants.len(), 1);
        assert!(metadata.participants[0].is_admin);
    }

    #[test]
    fn test_group_metadata_is_participant() {
        let jid: Jid = "123456789@g.us".parse().unwrap();
        let participant_jid: Jid = "1234567890@s.whatsapp.net".parse().unwrap();
        let non_participant_jid: Jid = "9999999999@s.whatsapp.net".parse().unwrap();

        let mut metadata = GroupMetadata::new(jid, "Test".to_string(), AddressingMode::Pn);
        metadata.add_participant(GroupParticipant::new(participant_jid.clone(), false));

        assert!(metadata.is_participant(&participant_jid));
        assert!(!metadata.is_participant(&non_participant_jid));
    }

    #[test]
    fn test_group_metadata_admins() {
        let jid: Jid = "123456789@g.us".parse().unwrap();
        let admin_jid: Jid = "1111111111@s.whatsapp.net".parse().unwrap();
        let member_jid: Jid = "2222222222@s.whatsapp.net".parse().unwrap();

        let mut metadata = GroupMetadata::new(jid, "Test".to_string(), AddressingMode::Pn);
        metadata.add_participant(GroupParticipant::new(admin_jid.clone(), true));
        metadata.add_participant(GroupParticipant::new(member_jid, false));

        let admins: Vec<_> = metadata.admins().collect();
        assert_eq!(admins.len(), 1);
        assert_eq!(admins[0].jid, admin_jid);
    }

    #[test]
    fn test_group_participant_new() {
        let jid: Jid = "1234567890@s.whatsapp.net".parse().unwrap();
        let participant = GroupParticipant::new(jid.clone(), true);

        assert_eq!(participant.jid, jid);
        assert!(participant.is_admin);
        assert!(participant.phone_number.is_none());
    }

    #[test]
    fn test_group_participant_with_phone_number() {
        let lid: Jid = "12345678@lid".parse().unwrap();
        let pn: Jid = "1234567890@s.whatsapp.net".parse().unwrap();

        let participant = GroupParticipant::with_phone_number(lid.clone(), pn.clone(), false);

        assert_eq!(participant.jid, lid);
        assert_eq!(participant.phone_number, Some(pn));
        assert!(!participant.is_admin);
    }
}
