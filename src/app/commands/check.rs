use log::error;
use sysinfo::System;
use waproto::whatsapp as wa;
use whatsapp_rust::bot::MessageContext;

pub async fn run(ctx: &MessageContext) {
    let mut sys = System::new_all();
    sys.refresh_all();

    let sender = &ctx.info.source.sender;
    let chat = &ctx.info.source.chat;
    let is_group = ctx.info.source.is_group;

    let who = if is_group {
        format!("User: {}\nChat: {}", sender.user, chat)
    } else {
        format!("User: {}", sender.user)
    };

    let hostname = System::host_name().unwrap_or_else(|| "Unknown".to_string());
    let os_version = System::long_os_version().unwrap_or_else(|| "Unknown".to_string());

    // Memory Logic: Try CompContainer limits first
    let mut memory_total_mb = sys.total_memory() / 1024 / 1024;
    let mut memory_used_mb = sys.used_memory() / 1024 / 1024;

    if let Some(limit) = get_cgroup_memory_limit() {
        memory_total_mb = limit / 1024 / 1024;
    }
    if let Some(usage) = get_cgroup_memory_usage() {
        memory_used_mb = usage / 1024 / 1024;
    }

    // CPU Logic
    let mut cpus = sys.cpus().len() as f64;
    if let Some(quota_cpus) = get_cgroup_cpu_limit() {
        cpus = quota_cpus;
    }

    let cpu_usage = sys.global_cpu_usage();

    // Determine participant JID for quoting (ContextInfo)
    let participant_jid = if ctx.info.source.is_from_me {
        if let Some(pn) = ctx.client.get_pn().await {
            pn.to_string()
        } else {
            "".to_string()
        }
    } else {
        ctx.info.source.sender.to_string()
    };

    // Safety fallback if get_pn fails or something, though sender_jid is usually fine.
    let participant_jid = if participant_jid.is_empty() {
        ctx.info.source.sender.to_string()
    } else {
        participant_jid
    };

    let reply = format!(
        "*System Check*\n\n\
        👤 *Initiated By:*\n{}\n\n\
        💻 *Environment:*\n\
        Host: {}\n\
        OS: {}\n\n\
        ⚙️ *Specs & Usage:*\n\
        CPU: {:.2} cores (Load: {:.1}%)\n\
        RAM: {}MB / {}MB",
        who, hostname, os_version, cpus, cpu_usage, memory_used_mb, memory_total_mb
    );

    // ContextInfo for quoting the original message
    let context_info = wa::ContextInfo {
        stanza_id: Some(ctx.info.id.clone()),
        participant: Some(participant_jid),
        quoted_message: Some(ctx.message.clone()),
        ..Default::default()
    };

    let message = wa::Message {
        extended_text_message: Some(Box::new(wa::message::ExtendedTextMessage {
            text: Some(reply),
            context_info: Some(Box::new(context_info)),
            ..Default::default()
        })),
        ..Default::default()
    };

    if let Err(e) = ctx.send_message(message).await {
        error!("Failed to send check response: {}", e);
    }
}

// Helpers for Control Groups (Docker limits)
fn get_cgroup_memory_limit() -> Option<u64> {
    // Try cgroup v2
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        if let Ok(val) = contents.trim().parse::<u64>() {
            if val != 0 && val != u64::MAX {
                return Some(val);
            }
        }
    }
    // Try cgroup v1
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(val) = contents.trim().parse::<u64>() {
            // A very large number usually means no limit (PAGE_COUNTER_MAX approx 9e18)
            if val < 9_000_000_000_000_000_000 {
                return Some(val);
            }
        }
    }
    None
}

fn get_cgroup_memory_usage() -> Option<u64> {
    // Try cgroup v2
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory.current") {
        if let Ok(val) = contents.trim().parse::<u64>() {
            return Some(val);
        }
    }
    // Try cgroup v1
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.usage_in_bytes") {
        if let Ok(val) = contents.trim().parse::<u64>() {
            return Some(val);
        }
    }
    // Fallback to simpler usage file commonly found in some setups
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.usage") {
        if let Ok(val) = contents.trim().parse::<u64>() {
            return Some(val);
        }
    }
    None
}

fn get_cgroup_cpu_limit() -> Option<f64> {
    // Check v2 (cpu.max)
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let parts: Vec<&str> = contents.trim().split_whitespace().collect();
        if parts.len() == 2 {
            if let (Ok(quota), Ok(period)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>()) {
                if quota > 0.0 && period > 0.0 {
                    return Some(quota / period);
                }
            }
        }
    }

    // Check v1
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok());
    let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok());

    if let (Some(q), Some(p)) = (quota, period) {
        if q > 0.0 && p > 0.0 {
            return Some(q / p);
        }
    }
    None
}
