use wacore_binary::jid::{DEFAULT_USER_SERVER, HIDDEN_USER_SERVER, Jid, JidExt};

#[test]
fn test_primary_phone_jid_creation_from_pn() {
    let own_pn = Jid::pn("559999999999");
    let primary_phone_jid = own_pn.with_device(0);

    assert_eq!(primary_phone_jid.user, "559999999999");
    assert_eq!(primary_phone_jid.server, DEFAULT_USER_SERVER);
    assert_eq!(primary_phone_jid.device, 0);
    assert_eq!(primary_phone_jid.agent, 0);
    assert_eq!(primary_phone_jid.to_string(), "559999999999@s.whatsapp.net");
}

#[test]
fn test_primary_phone_jid_overwrites_existing_device() {
    // Edge case: pn with device ID should still produce device 0
    let own_pn = Jid::pn_device("559999999999", 33);
    let primary_phone_jid = own_pn.with_device(0);

    assert_eq!(primary_phone_jid.user, "559999999999");
    assert_eq!(primary_phone_jid.server, DEFAULT_USER_SERVER);
    assert_eq!(primary_phone_jid.device, 0);
}

#[test]
fn test_primary_phone_jid_is_not_ad() {
    let primary_phone_jid = Jid::pn("559999999999").with_device(0);
    assert!(!primary_phone_jid.is_ad()); // device 0 is NOT an additional device
}

#[test]
fn test_linked_device_is_ad() {
    let linked_device_jid = Jid::pn_device("559999999999", 33);
    assert!(linked_device_jid.is_ad()); // device > 0 IS an additional device
}

#[test]
fn test_primary_phone_jid_from_lid() {
    let own_lid = Jid::lid("100000000000001");
    let primary_phone_jid = own_lid.with_device(0);

    assert_eq!(primary_phone_jid.user, "100000000000001");
    assert_eq!(primary_phone_jid.server, HIDDEN_USER_SERVER);
    assert_eq!(primary_phone_jid.device, 0);
    assert!(!primary_phone_jid.is_ad());
}

#[test]
fn test_primary_phone_jid_roundtrip() {
    let own_pn = Jid::pn("559999999999");
    let primary_phone_jid = own_pn.with_device(0);

    let jid_string = primary_phone_jid.to_string();
    assert_eq!(jid_string, "559999999999@s.whatsapp.net");

    let parsed: Jid = jid_string.parse().expect("JID should be parseable");
    assert_eq!(parsed.user, "559999999999");
    assert_eq!(parsed.server, DEFAULT_USER_SERVER);
    assert_eq!(parsed.device, 0);
}

#[test]
fn test_with_device_preserves_identity() {
    let pn = Jid::pn("1234567890");
    let pn_device_0 = pn.with_device(0);
    let pn_device_5 = pn.with_device(5);

    assert_eq!(pn_device_0.user, pn_device_5.user);
    assert_eq!(pn_device_0.server, pn_device_5.server);
    assert_eq!(pn_device_0.device, 0);
    assert_eq!(pn_device_5.device, 5);

    let lid = Jid::lid("100000012345678");
    let lid_device_0 = lid.with_device(0);
    let lid_device_33 = lid.with_device(33);

    assert_eq!(lid_device_0.user, lid_device_33.user);
    assert_eq!(lid_device_0.server, lid_device_33.server);
    assert_eq!(lid_device_0.device, 0);
    assert_eq!(lid_device_33.device, 33);
}

#[test]
fn test_primary_phone_vs_companion_devices() {
    let user = "559999999999";
    let primary = Jid::pn(user).with_device(0);
    let companion_web = Jid::pn_device(user, 33);
    let companion_desktop = Jid::pn_device(user, 34);

    // All share the same user
    assert_eq!(primary.user, companion_web.user);
    assert_eq!(primary.user, companion_desktop.user);

    // But have different device IDs
    assert_eq!(primary.device, 0);
    assert_eq!(companion_web.device, 33);
    assert_eq!(companion_desktop.device, 34);

    // Primary is NOT AD, companions ARE AD
    assert!(!primary.is_ad());
    assert!(companion_web.is_ad());
    assert!(companion_desktop.is_ad());
}
