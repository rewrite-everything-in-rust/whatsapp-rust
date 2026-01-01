use super::traits::StanzaHandler;
use crate::client::Client;
use std::collections::HashMap;
use std::sync::Arc;
use wacore_binary::node::Node;

/// Central router for dispatching XML stanzas to their appropriate handlers.
///
/// The router maintains a registry of handlers keyed by XML tag and efficiently
/// dispatches incoming nodes to the correct handler based on the node's tag.
pub struct StanzaRouter {
    /// Map of XML tag -> handler for fast lookups
    handlers: HashMap<&'static str, Arc<dyn StanzaHandler>>,
}

impl StanzaRouter {
    /// Create a new empty router.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a handler for a specific XML tag.
    ///
    /// # Arguments
    /// * `handler` - The handler implementation to register
    ///
    /// # Panics
    /// Panics if a handler is already registered for the same tag to prevent
    /// accidental overwrites during initialization.
    pub fn register(&mut self, handler: Arc<dyn StanzaHandler>) {
        let tag = handler.tag();
        if self.handlers.insert(tag, handler).is_some() {
            panic!("Handler for tag '{}' already registered", tag);
        }
    }

    /// Dispatch a node to its appropriate handler.
    ///
    /// # Arguments
    /// * `client` - Arc reference to the client instance
    /// * `node` - Arc-wrapped owned Node (avoids cloning)
    ///
    /// # Returns
    /// Returns `true` if a handler was found and successfully processed the node,
    /// `false` if no handler was registered for the node's tag or the handler
    /// indicated it couldn't process the node.
    pub async fn dispatch(
        &self,
        client: Arc<Client>,
        node: Arc<Node>,
        cancelled: &mut bool,
    ) -> bool {
        if let Some(handler) = self.handlers.get(node.tag.as_str()) {
            handler.handle(client, node, cancelled).await
        } else {
            false
        }
    }

    /// Get the number of registered handlers (useful for testing).
    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }
}

impl Default for StanzaRouter {
    fn default() -> Self {
        Self::new()
    }
}
