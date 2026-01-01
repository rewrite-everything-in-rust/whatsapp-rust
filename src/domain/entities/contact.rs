use wacore_binary::jid::Jid;

#[derive(Debug, Clone)]
pub struct IsOnWhatsAppResult {
    pub jid: Jid,
    pub is_registered: bool,
}

#[derive(Debug, Clone)]
pub struct ContactInfo {
    pub jid: Jid,
    pub lid: Option<Jid>,
    pub is_registered: bool,
    pub is_business: bool,
    pub status: Option<String>,
    pub picture_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ProfilePicture {
    pub id: String,
    pub url: String,
    pub direct_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UserInfo {
    pub jid: Jid,
    pub lid: Option<Jid>,
    pub status: Option<String>,
    pub picture_id: Option<String>,
    pub is_business: bool,
}

impl ContactInfo {
    pub fn new(jid: Jid, is_registered: bool) -> Self {
        Self {
            jid,
            lid: None,
            is_registered,
            is_business: false,
            status: None,
            picture_id: None,
        }
    }

    pub fn display_jid(&self) -> &Jid {
        self.lid.as_ref().unwrap_or(&self.jid)
    }
}

impl UserInfo {
    pub fn new(jid: Jid) -> Self {
        Self {
            jid,
            lid: None,
            status: None,
            picture_id: None,
            is_business: false,
        }
    }

    pub fn display_jid(&self) -> &Jid {
        self.lid.as_ref().unwrap_or(&self.jid)
    }
}

impl ProfilePicture {
    pub fn new(id: String, url: String) -> Self {
        Self {
            id,
            url,
            direct_path: None,
        }
    }

    pub fn with_direct_path(id: String, url: String, direct_path: String) -> Self {
        Self {
            id,
            url,
            direct_path: Some(direct_path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contact_info_new() {
        let jid: Jid = "1234567890@s.whatsapp.net".parse().unwrap();
        let info = ContactInfo::new(jid.clone(), true);

        assert_eq!(info.jid, jid);
        assert!(info.is_registered);
        assert!(!info.is_business);
        assert!(info.lid.is_none());
        assert!(info.status.is_none());
        assert!(info.picture_id.is_none());
    }

    #[test]
    fn test_contact_info_display_jid() {
        let jid: Jid = "1234567890@s.whatsapp.net".parse().unwrap();
        let lid: Jid = "12345678@lid".parse().unwrap();

        let mut info = ContactInfo::new(jid.clone(), true);
        assert_eq!(info.display_jid(), &jid);

        info.lid = Some(lid.clone());
        assert_eq!(info.display_jid(), &lid);
    }

    #[test]
    fn test_user_info_new() {
        let jid: Jid = "1234567890@s.whatsapp.net".parse().unwrap();
        let info = UserInfo::new(jid.clone());

        assert_eq!(info.jid, jid);
        assert!(!info.is_business);
        assert!(info.lid.is_none());
        assert!(info.status.is_none());
        assert!(info.picture_id.is_none());
    }

    #[test]
    fn test_profile_picture_new() {
        let pic = ProfilePicture::new("123".to_string(), "https://example.com/pic.jpg".to_string());

        assert_eq!(pic.id, "123");
        assert_eq!(pic.url, "https://example.com/pic.jpg");
        assert!(pic.direct_path.is_none());
    }

    #[test]
    fn test_profile_picture_with_direct_path() {
        let pic = ProfilePicture::with_direct_path(
            "123".to_string(),
            "https://example.com/pic.jpg".to_string(),
            "/v/pic.jpg".to_string(),
        );

        assert_eq!(pic.id, "123");
        assert_eq!(pic.url, "https://example.com/pic.jpg");
        assert_eq!(pic.direct_path, Some("/v/pic.jpg".to_string()));
    }

    #[test]
    fn test_is_on_whatsapp_result() {
        let jid: Jid = "1234567890@s.whatsapp.net".parse().unwrap();
        let result = IsOnWhatsAppResult {
            jid: jid.clone(),
            is_registered: true,
        };

        assert_eq!(result.jid, jid);
        assert!(result.is_registered);
    }
}
