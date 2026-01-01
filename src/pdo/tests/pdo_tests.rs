#[cfg(test)]
mod tests {
    use wacore_binary::jid::{DEFAULT_USER_SERVER, Jid, JidExt};

    #[test]
    fn test_pdo_primary_phone_jid_is_device_0() {
        // PDO sends to device 0 (primary phone)
        let own_pn = Jid::pn("559999999999");
        let primary_phone_jid = own_pn.with_device(0);

        assert_eq!(primary_phone_jid.device, 0);
        assert!(!primary_phone_jid.is_ad()); // Device 0 is NOT an additional device
    }

    #[test]
    fn test_pdo_primary_phone_jid_preserves_user() {
        let own_pn = Jid::pn("559999999999");
        let primary_phone_jid = own_pn.with_device(0);

        assert_eq!(primary_phone_jid.user, "559999999999");
        assert_eq!(primary_phone_jid.server, DEFAULT_USER_SERVER);
    }

    #[test]
    fn test_pdo_primary_phone_jid_from_linked_device() {
        // Even if we're device 33, PDO should send to device 0
        let own_pn = Jid::pn_device("559999999999", 33);
        let primary_phone_jid = own_pn.with_device(0);

        assert_eq!(primary_phone_jid.user, "559999999999");
        assert_eq!(primary_phone_jid.device, 0);
    }
}
