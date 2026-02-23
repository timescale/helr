use serde::{Deserialize, Serialize};

/// Query param value: string or number in YAML (e.g. limit: 20 or limit: "20").
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryParamValue {
    String(String),
    Int(i64),
}
impl QueryParamValue {
    pub fn to_param_value(&self) -> String {
        match self {
            QueryParamValue::String(s) => s.clone(),
            QueryParamValue::Int(n) => n.to_string(),
        }
    }
}

/// HTTP method for the source request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum PaginationConfig {
    LinkHeader {
        #[serde(default = "default_rel")]
        rel: String,
        #[serde(default)]
        max_pages: Option<u32>,
    },
    Cursor {
        cursor_param: String,
        cursor_path: String, // JSONPath or simple key
        #[serde(default)]
        max_pages: Option<u32>,
    },
    PageOffset {
        page_param: String,
        limit_param: String,
        limit: u32,
        #[serde(default)]
        max_pages: Option<u32>,
    },
    Offset {
        offset_param: String,
        limit_param: String,
        limit: u32,
        #[serde(default)]
        max_pages: Option<u32>,
    },
}

fn default_rel() -> String {
    "next".to_string()
}
