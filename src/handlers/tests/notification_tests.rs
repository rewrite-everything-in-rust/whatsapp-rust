use crate::handlers::notification::{extract_device_ids, parse_account_sync_device_list};
use wacore::types::events::DeviceListUpdateType;
use wacore_binary::builder::NodeBuilder;

/// Helper to parse device notification and extract update info
fn parse_device_notification_info(
    node: &wacore_binary::node::Node,
) -> Vec<(DeviceListUpdateType, Vec<u32>, Option<String>)> {
    let Some(children) = node.children() else {
        return vec![];
    };

    let mut results = vec![];
    for child in children.iter() {
        let (update_type, hash) = match child.tag.as_str() {
            "add" => (DeviceListUpdateType::Add, None),
            "remove" => (DeviceListUpdateType::Remove, None),
            "update" => {
                let hash = child.attrs().optional_string("hash").map(|s| s.to_string());
                (DeviceListUpdateType::Update, hash)
            }
            _ => continue,
        };

        let devices = extract_device_ids(child);

        results.push((update_type, devices, hash));
    }
    results
}

#[test]
fn test_parse_device_add_notification() {
    let node = NodeBuilder::new("notification")
        .attr("type", "devices")
        .attr("from", "1234567890@s.whatsapp.net")
        .children([NodeBuilder::new("add")
            .children([
                NodeBuilder::new("device").attr("id", "1").build(),
                NodeBuilder::new("device").attr("id", "2").build(),
            ])
            .build()])
        .build();

    let results = parse_device_notification_info(&node);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, DeviceListUpdateType::Add);
    assert_eq!(results[0].1, vec![1, 2]);
    assert_eq!(results[0].2, None);
}

#[test]
fn test_parse_device_remove_notification() {
    let node = NodeBuilder::new("notification")
        .attr("type", "devices")
        .attr("from", "1234567890@s.whatsapp.net")
        .children([NodeBuilder::new("remove")
            .children([NodeBuilder::new("device").attr("id", "3").build()])
            .build()])
        .build();

    let results = parse_device_notification_info(&node);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, DeviceListUpdateType::Remove);
    assert_eq!(results[0].1, vec![3]);
}

#[test]
fn test_parse_device_update_notification_with_hash() {
    let node = NodeBuilder::new("notification")
        .attr("type", "devices")
        .attr("from", "1234567890@s.whatsapp.net")
        .children([NodeBuilder::new("update")
            .attr("hash", "2:abcdef123456")
            .children([NodeBuilder::new("device").attr("id", "0").build()])
            .build()])
        .build();

    let results = parse_device_notification_info(&node);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, DeviceListUpdateType::Update);
    assert_eq!(results[0].1, vec![0]);
    assert_eq!(results[0].2, Some("2:abcdef123456".to_string()));
}

#[test]
fn test_parse_empty_device_notification() {
    let node = NodeBuilder::new("notification")
        .attr("type", "devices")
        .attr("from", "1234567890@s.whatsapp.net")
        .build();

    let results = parse_device_notification_info(&node);
    assert!(results.is_empty());
}

#[test]
fn test_parse_multiple_device_operations() {
    let node = NodeBuilder::new("notification")
        .attr("type", "devices")
        .attr("from", "1234567890@s.whatsapp.net")
        .children([
            NodeBuilder::new("add")
                .children([NodeBuilder::new("device").attr("id", "5").build()])
                .build(),
            NodeBuilder::new("remove")
                .children([NodeBuilder::new("device").attr("id", "2").build()])
                .build(),
        ])
        .build();

    let results = parse_device_notification_info(&node);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, DeviceListUpdateType::Add);
    assert_eq!(results[0].1, vec![5]);
    assert_eq!(results[1].0, DeviceListUpdateType::Remove);
    assert_eq!(results[1].1, vec![2]);
}

// Tests for account_sync device parsing

#[test]
fn test_parse_account_sync_device_list_basic() {
    let devices_node = NodeBuilder::new("devices")
        .attr("dhash", "2:FnEWjS13")
        .children([
            NodeBuilder::new("device")
                .attr("jid", "15551234567@s.whatsapp.net")
                .build(),
            NodeBuilder::new("device")
                .attr("jid", "15551234567:64@s.whatsapp.net")
                .attr("key-index", "2")
                .build(),
        ])
        .build();

    let devices = parse_account_sync_device_list(&devices_node);
    assert_eq!(devices.len(), 2);

    // Primary device (device 0)
    assert_eq!(devices[0].jid.user, "15551234567");
    assert_eq!(devices[0].jid.device, 0);
    assert_eq!(devices[0].key_index, None);

    // Companion device (device 64)
    assert_eq!(devices[1].jid.user, "15551234567");
    assert_eq!(devices[1].jid.device, 64);
    assert_eq!(devices[1].key_index, Some(2));
}

#[test]
fn test_parse_account_sync_device_list_with_key_index_list() {
    // Real-world structure includes <key-index-list> which should be ignored
    let devices_node = NodeBuilder::new("devices")
        .attr("dhash", "2:FnEWjS13")
        .children([
            NodeBuilder::new("device")
                .attr("jid", "15551234567@s.whatsapp.net")
                .build(),
            NodeBuilder::new("device")
                .attr("jid", "15551234567:77@s.whatsapp.net")
                .attr("key-index", "15")
                .build(),
            NodeBuilder::new("key-index-list")
                .attr("ts", "1766612162")
                .bytes(vec![0x01, 0x02, 0x03]) // Simulated signed bytes
                .build(),
        ])
        .build();

    let devices = parse_account_sync_device_list(&devices_node);
    // Should only parse <device> tags, not <key-index-list>
    assert_eq!(devices.len(), 2);
    assert_eq!(devices[0].jid.device, 0);
    assert_eq!(devices[1].jid.device, 77);
    assert_eq!(devices[1].key_index, Some(15));
}

#[test]
fn test_parse_account_sync_device_list_empty() {
    let devices_node = NodeBuilder::new("devices")
        .attr("dhash", "2:FnEWjS13")
        .build();

    let devices = parse_account_sync_device_list(&devices_node);
    assert!(devices.is_empty());
}

#[test]
fn test_parse_account_sync_device_list_multiple_devices() {
    let devices_node = NodeBuilder::new("devices")
        .attr("dhash", "2:XYZ123")
        .children([
            NodeBuilder::new("device")
                .attr("jid", "1234567890@s.whatsapp.net")
                .build(),
            NodeBuilder::new("device")
                .attr("jid", "1234567890:1@s.whatsapp.net")
                .attr("key-index", "1")
                .build(),
            NodeBuilder::new("device")
                .attr("jid", "1234567890:2@s.whatsapp.net")
                .attr("key-index", "5")
                .build(),
            NodeBuilder::new("device")
                .attr("jid", "1234567890:3@s.whatsapp.net")
                .attr("key-index", "10")
                .build(),
        ])
        .build();

    let devices = parse_account_sync_device_list(&devices_node);
    assert_eq!(devices.len(), 4);

    // Verify device IDs are correctly parsed
    assert_eq!(devices[0].jid.device, 0);
    assert_eq!(devices[1].jid.device, 1);
    assert_eq!(devices[2].jid.device, 2);
    assert_eq!(devices[3].jid.device, 3);

    // Verify key indexes
    assert_eq!(devices[0].key_index, None);
    assert_eq!(devices[1].key_index, Some(1));
    assert_eq!(devices[2].key_index, Some(5));
    assert_eq!(devices[3].key_index, Some(10));
}
