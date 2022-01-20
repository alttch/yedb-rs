use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};

use std::collections::HashMap;
use std::convert::TryInto;

use log::{debug, error};

const ERR_CODE_KEY_NOT_FOUND: i16 = -32001;
const ERR_CODE_DATA: i16 = -32002;
const ERR_CODE_SCHEMA_VALIDATION: i16 = -32003;
const ERR_CODE_IO: i16 = -32004;
const ERR_CODE_FIELD_NOT_FOUND: i16 = -32005;
const ERR_CODE_TIMEOUT: i16 = -32006;
const ERR_CODE_UNSUPPORTED_FORMAT: i16 = -32007;
const ERR_CODE_UNSUPPORTED_VERSION: i16 = -32008;
const ERR_CODE_NOT_OPENED: i16 = -32009;
const ERR_CODE_BUSY: i16 = -32010;
const ERR_CODE_NOT_INITIALIZED: i16 = -32011;
const ERR_CODE_PROTO: i16 = -32012;
const ERR_CODE_EOF: i16 = -32013;

const ERR_CODE_REQUEST: i16 = -32600;
const ERR_CODE_METHOD_NOT_FOUND: i16 = -32601;
const ERR_CODE_INVALID_PARAMS: i16 = -32602;
const ERR_CODE_OTHER: i16 = -32603;

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
    #[serde(rename = "type")]
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

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Copy, Clone, Debug)]
#[repr(i16)]
pub enum ErrorKind {
    IOError = ERR_CODE_IO,
    DataError = ERR_CODE_DATA,
    TimeoutError = ERR_CODE_TIMEOUT,
    KeyNotFound = ERR_CODE_KEY_NOT_FOUND,
    FieldNotFound = ERR_CODE_FIELD_NOT_FOUND,
    SchemaValidationError = ERR_CODE_SCHEMA_VALIDATION,
    UnsupportedFormat = ERR_CODE_UNSUPPORTED_FORMAT,
    UnsupportedVersion = ERR_CODE_UNSUPPORTED_VERSION,
    NotOpened = ERR_CODE_NOT_OPENED,
    Busy = ERR_CODE_BUSY,
    NotInitialized = ERR_CODE_NOT_INITIALIZED,
    RequestError = ERR_CODE_REQUEST,
    ProtocolError = ERR_CODE_PROTO,
    Eof = ERR_CODE_EOF,
    MethodNotFound = ERR_CODE_METHOD_NOT_FOUND,
    InvalidParameter = ERR_CODE_INVALID_PARAMS,
    Other = ERR_CODE_OTHER,
}

impl From<i16> for ErrorKind {
    fn from(code: i16) -> Self {
        match code {
            ERR_CODE_IO => ErrorKind::IOError,
            ERR_CODE_DATA => ErrorKind::DataError,
            ERR_CODE_TIMEOUT => ErrorKind::TimeoutError,
            ERR_CODE_KEY_NOT_FOUND => ErrorKind::KeyNotFound,
            ERR_CODE_FIELD_NOT_FOUND => ErrorKind::FieldNotFound,
            ERR_CODE_SCHEMA_VALIDATION => ErrorKind::SchemaValidationError,
            ERR_CODE_UNSUPPORTED_FORMAT => ErrorKind::UnsupportedFormat,
            ERR_CODE_UNSUPPORTED_VERSION => ErrorKind::UnsupportedVersion,
            ERR_CODE_NOT_OPENED => ErrorKind::NotOpened,
            ERR_CODE_BUSY => ErrorKind::Busy,
            ERR_CODE_NOT_INITIALIZED => ErrorKind::NotInitialized,
            ERR_CODE_REQUEST => ErrorKind::RequestError,
            ERR_CODE_PROTO => ErrorKind::ProtocolError,
            ERR_CODE_EOF => ErrorKind::Eof,
            ERR_CODE_METHOD_NOT_FOUND => ErrorKind::MethodNotFound,
            ERR_CODE_INVALID_PARAMS => ErrorKind::InvalidParameter,
            _ => ErrorKind::Other,
        }
    }
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
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
            }
        )
    }
}

impl Default for ErrorKind {
    fn default() -> Self {
        ErrorKind::Other
    }
}

fn ok_or_default<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + Default,
    D: Deserializer<'de>,
{
    let v: Value = Deserialize::deserialize(deserializer)?;
    Ok(T::deserialize(v).unwrap_or_default())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Error {
    #[serde(rename = "code", deserialize_with = "ok_or_default")]
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

    pub fn get_message(&self) -> &str {
        &self.message
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

#[cfg(feature = "client-elbus-async")]
impl From<elbus::rpc::RpcError> for Error {
    fn from(err: elbus::rpc::RpcError) -> Error {
        let code = err.code();
        Error::new(
            code.into(),
            format!(
                "(code: {}) {}",
                code,
                if let Some(data) = err.data() {
                    std::str::from_utf8(data).unwrap_or_default()
                } else {
                    ""
                }
            ),
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JSONRpcRequest {
    jsonrpc: String,
    pub id: Value,
    pub method: String,
    pub params: HashMap<String, Value>,
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
    pub fn with_params(id: u64, method: &str, params: HashMap<String, Value>) -> Self {
        Self {
            id: Value::from(id),
            jsonrpc: "2.0".to_owned(),
            method: method.to_owned(),
            params,
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
