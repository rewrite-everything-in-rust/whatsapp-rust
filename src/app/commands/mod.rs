pub mod check;
pub mod ping;

use whatsapp_rust::bot::MessageContext;

pub async fn handle_command(ctx: &MessageContext, text: &str) -> bool {
    let mut parts = text.split_whitespace();
    let command_with_prefix = parts.next().unwrap_or("");

    // Prefix
    let command = if let Some(cmd) = command_with_prefix.strip_prefix('.') {
        cmd
    } else if let Some(cmd) = command_with_prefix.strip_prefix('!') {
        cmd
    } else if let Some(cmd) = command_with_prefix.strip_prefix('/') {
        cmd
    } else {
        return false;
    };

    let command = command.to_lowercase();
    // args could be used for other commands
    // let args: Vec<&str> = parts.collect();

    match command.as_str() {
        "check" => {
            check::run(ctx).await;
            true
        }
        "ping" => {
            ping::run(ctx).await;
            true
        }
        _ => false,
    }
}
