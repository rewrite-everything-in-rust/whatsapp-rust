use crate::client::Client;
use crate::jid_utils::server_jid;
use crate::request::InfoQuery;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use wacore_binary::builder::NodeBuilder;
use wacore_binary::node::{Node, NodeContent};

#[derive(Debug, Error)]
pub enum MexError {
    #[error("MEX payload parsing error: {0}")]
    PayloadParsing(String),

    #[error("MEX extension error: code={code}, message='{message}'")]
    ExtensionError { code: i32, message: String },

    #[error("IQ request failed: {0}")]
    Request(#[from] Box<crate::request::IqError>),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct MexRequest<'a> {
    pub doc_id: &'a str,

    pub variables: Value,
}

#[derive(Serialize)]
pub(crate) struct MexPayload<'a> {
    pub(crate) variables: &'a Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MexResponse {
    pub data: Option<Value>,

    pub errors: Option<Vec<MexGraphQLError>>,
}

impl MexResponse {
    #[inline]
    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    #[inline]
    pub fn has_errors(&self) -> bool {
        self.errors.as_ref().is_some_and(|e| !e.is_empty())
    }

    pub fn fatal_error(&self) -> Option<&MexGraphQLError> {
        self.errors.as_ref()?.iter().find(|e| {
            e.extensions
                .as_ref()
                .is_some_and(|ext| ext.is_summary == Some(true))
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MexGraphQLError {
    pub message: String,

    pub extensions: Option<MexErrorExtensions>,
}

impl MexGraphQLError {
    #[inline]
    pub fn error_code(&self) -> Option<i32> {
        self.extensions.as_ref()?.error_code
    }

    #[inline]
    pub fn is_fatal(&self) -> bool {
        self.extensions
            .as_ref()
            .is_some_and(|ext| ext.is_summary == Some(true))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MexErrorExtensions {
    pub error_code: Option<i32>,

    pub is_summary: Option<bool>,

    #[serde(default)]
    pub is_retryable: Option<bool>,

    pub severity: Option<String>,
}

pub struct Mex<'a> {
    client: &'a Client,
}

impl<'a> Mex<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    #[inline]
    pub async fn query(&self, request: MexRequest<'_>) -> Result<MexResponse, MexError> {
        self.execute(request).await
    }

    #[inline]
    pub async fn mutate(&self, request: MexRequest<'_>) -> Result<MexResponse, MexError> {
        self.execute(request).await
    }

    async fn execute(&self, request: MexRequest<'_>) -> Result<MexResponse, MexError> {
        let payload = MexPayload {
            variables: &request.variables,
        };
        let payload_bytes = serde_json::to_vec(&payload)?;

        let query_node = NodeBuilder::new("query")
            .attr("query_id", request.doc_id)
            .bytes(payload_bytes)
            .build();

        let iq = InfoQuery::get(
            "w:mex",
            server_jid(),
            Some(NodeContent::Nodes(vec![query_node])),
        );

        let response_node = self.client.send_iq(iq).await.map_err(Box::new)?;

        Self::parse_response(&response_node)
    }

    fn parse_response(node: &Node) -> Result<MexResponse, MexError> {
        let result_node = node
            .get_optional_child("result")
            .ok_or_else(|| MexError::PayloadParsing("Missing <result> node".into()))?;

        let result_bytes = match &result_node.content {
            Some(NodeContent::Bytes(bytes)) => bytes,
            _ => return Err(MexError::PayloadParsing("Result not binary".into())),
        };

        let response: MexResponse = serde_json::from_slice(result_bytes)?;

        if let Some(fatal) = response.fatal_error() {
            let code = fatal.error_code().unwrap_or(500);
            return Err(MexError::ExtensionError {
                code,
                message: fatal.message.clone(),
            });
        }

        Ok(response)
    }
}

impl Client {
    #[inline]
    pub fn mex(&self) -> Mex<'_> {
        Mex::new(self)
    }
}
