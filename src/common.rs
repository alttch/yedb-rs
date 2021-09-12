use rmp_serde;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use std::collections::HashMap;
use std::convert::TryInto;

use log::{debug, error};

fn se_checksum<S>(checksum: &Option<[u8; 32]>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match checksum {
        Some(v) => s.serialize_str(hex::encode(v).as_str()),
        None => s.serialize_none(),
    }
}

fn de_checksum<'de, D>(deserializer: D) -> Result<Option<[u8; 32]>, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer) {
        Ok(v) => match v {
            Value::String(s) => match hex::decode(s) {
                Ok(c) => {
                    let result: [u8; 32] = match c.try_into() {
                        Ok(value) => value,
                        Err(_) => return Err(serde::de::Error::custom("sha256 length error")),
                    };
                    Ok(Some(result))
                }
                Err(e) => Err(serde::de::Error::custom(e)),
            },
            Value::Null => Ok(None),
            _ => Err(serde::de::Error::custom("sha256 should be string")),
        },
        Err(e) => Err(e),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyExplained {
    pub value: Value,
    pub schema: Option<String>,
    pub len: Option<u64>,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub tp: String,
    pub mtime: u64,
    pub size: u64,
    #[serde(serialize_with = "se_checksum", deserialize_with = "de_checksum")]
    pub sha256: Option<[u8; 32]>,
    pub stime: Option<u64>,
    pub file: String,
}

fn default_auto_bak() -> u64 {
    0
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DBInfo {
    pub repair_recommended: bool,
    pub cached_keys: usize,
    pub cache_size: usize,
    #[serde(default = "default_auto_bak")]
    pub auto_bak: u64,
    pub strict_schema: bool,
    pub auto_flush: bool,
    pub path: String,
    pub lock_path: String,
    pub server: (String, String),
    pub fmt: String,
    pub checksums: bool,
    pub created: u64,
    pub version: u8,
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum ErrorKind {
    IOError,
    DataError,
    TimeoutError,
    KeyNotFound,
    FieldNotFound,
    SchemaValidationError,
    UnsupportedFormat,
    UnsupportedVersion,
    NotOpened,
    Busy,
    NotInitialized,
    RequestError,
    ProtocolError,
    MethodNotFound,
    InvalidParameter,
    Other,
    Eof,
}

impl ErrorKind {
    fn convert_to_string(self) -> String {
        (match self {
            ErrorKind::IOError => "I/O Error",
            ErrorKind::DataError => "Data error",
            ErrorKind::TimeoutError => "Timeout error",
            ErrorKind::KeyNotFound => "Key not found",
            ErrorKind::FieldNotFound => "Field not found",
            ErrorKind::SchemaValidationError => "Schema validation error",
            ErrorKind::UnsupportedFormat => "Unsupported format",
            ErrorKind::UnsupportedVersion => "Unsupported version",
            ErrorKind::NotOpened => "Not opened",
            ErrorKind::Busy => "Database is busy",
            ErrorKind::NotInitialized => "Not initialized",
            ErrorKind::RequestError => "Request error",
            ErrorKind::ProtocolError => "Protocol error",
            ErrorKind::MethodNotFound => "Method not found",
            ErrorKind::InvalidParameter => "Invalid parameter",
            ErrorKind::Eof => "EOF",
            ErrorKind::Other => "Error",
        })
        .to_owned()
    }
}

#[allow(clippy::unnecessary_wraps)]
fn de_errorkind<'de, D>(deserializer: D) -> Result<ErrorKind, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(if let Ok(n) = i32::deserialize(deserializer) {
        match n {
            -32001 => ErrorKind::KeyNotFound,
            -32002 => ErrorKind::DataError,
            -32003 => ErrorKind::SchemaValidationError,
            -32004 => ErrorKind::IOError,
            -32601 => ErrorKind::MethodNotFound,
            -32602 => ErrorKind::InvalidParameter,
            -32681 => ErrorKind::FieldNotFound,
            _ => ErrorKind::Other,
        }
    } else {
        ErrorKind::Other
    })
}

impl Serialize for ErrorKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let code = match self {
            ErrorKind::KeyNotFound => -32001,
            ErrorKind::DataError => -32002,
            ErrorKind::SchemaValidationError => -32003,
            ErrorKind::IOError => -32004,
            ErrorKind::MethodNotFound => -32601,
            ErrorKind::InvalidParameter => -32602,
            ErrorKind::FieldNotFound => -32681,
            _ => -32000,
        };
        serializer.serialize_i32(code)
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.convert_to_string())
    }
}

impl std::fmt::Debug for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.convert_to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Error {
    #[serde(
        rename(serialize = "code", deserialize = "code"),
        deserialize_with = "de_errorkind"
    )]
    error_kind: ErrorKind,
    message: String,
}

impl Error {
    pub fn new<E: std::fmt::Display>(kind: ErrorKind, error: E) -> Self {
        if kind == ErrorKind::KeyNotFound {
            debug!("{} {}", kind, error);
        } else {
            error!("error {} {}", kind, error);
        }
        Self {
            error_kind: kind,
            message: format!("{}", error),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.error_kind
    }

    pub fn get_message(&self) -> String {
        self.message.clone()
    }

    pub fn err_invalid_parameter() -> Self {
        Self {
            error_kind: ErrorKind::InvalidParameter,
            message: "Invalid method parameter".to_owned(),
        }
    }

    pub fn err_method_not_found() -> Self {
        Self {
            error_kind: ErrorKind::MethodNotFound,
            message: "Method not found".to_owned(),
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: {}", self.error_kind, self.message)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JSONRpcRequest {
    jsonrpc: String,
    pub id: Value,
    pub method: String,
    pub params: HashMap<String, serde_json::Value>,
}

impl JSONRpcRequest {
    pub fn new(id: u64, method: &str) -> Self {
        Self {
            id: Value::from(id),
            jsonrpc: "2.0".to_owned(),
            method: method.to_owned(),
            params: HashMap::new(),
        }
    }

    pub fn set_param(&mut self, name: &str, value: Value) {
        self.params.insert(name.to_owned(), value);
    }

    /// # Errors
    ///
    /// Will return errors on serialization errors
    pub fn pack(&self) -> Result<Vec<u8>, Error> {
        match rmp_serde::to_vec_named(&self) {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::new(ErrorKind::RequestError, e)),
        }
    }

    pub fn respond<T: Serialize>(&self, result: T) -> JSONRpcResponse<T> {
        JSONRpcResponse {
            jsonrpc: self.jsonrpc.clone(),
            id: self.id.clone(),
            result: Some(result),
            error: None,
        }
    }

    pub fn respond_ok(&self) -> JSONRpcResponse<Value> {
        self.respond(serde_json::json!({"ok": true }))
    }

    pub fn is_valid(&self) -> bool {
        self.jsonrpc == "2.0"
    }

    pub fn error(&self, err: Error) -> JSONRpcResponse<Value> {
        JSONRpcResponse {
            jsonrpc: self.jsonrpc.clone(),
            id: self.id.clone(),
            result: None,
            error: Some(err),
        }
    }

    pub fn params_valid(&self, params: Vec<&str>) -> bool {
        let keys = self.params.keys();
        for k in keys {
            if !params.contains(&k.as_str()) {
                return false;
            }
        }
        for k in params {
            if !self.params.contains_key(k) {
                return false;
            }
        }
        true
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JSONRpcResponse<T: Serialize> {
    jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

#[derive(Serialize)]
pub struct JSONRpcError {
    code: i16,
    message: String,
}
