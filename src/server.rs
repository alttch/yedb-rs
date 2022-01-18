use crate::common::{JSONRpcRequest, JSONRpcResponse};
use crate::Database;
use crate::Error;
use log::trace;
use serde_json::{json, Value};
#[cfg(feature = "elbus-rpc")]
use std::collections::HashMap;
#[cfg(feature = "elbus-rpc")]
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "elbus-rpc")]
use elbus::rpc::{RpcError, RpcEvent, RpcHandlers, RpcResult};
#[cfg(feature = "elbus-rpc")]
use elbus::Frame;

#[cfg(feature = "elbus-rpc")]
pub struct ElbusApi {
    db: Arc<RwLock<Database>>,
}

#[cfg(feature = "elbus-rpc")]
impl ElbusApi {
    pub fn new(db: Arc<RwLock<Database>>) -> Self {
        Self { db }
    }
}

#[cfg(feature = "elbus-rpc")]
#[async_trait::async_trait]
impl RpcHandlers for ElbusApi {
    async fn handle_notification(&self, _event: RpcEvent) {}
    async fn handle_frame(&self, _frame: Frame) {}
    async fn handle_call(&self, event: RpcEvent) -> RpcResult {
        let method = event.parse_method()?;
        let payload = event.payload();
        let params: HashMap<String, Value> = if payload.is_empty() {
            HashMap::new()
        } else {
            rmp_serde::from_read_ref(event.payload())?
        };
        let id = event.id();
        let request = JSONRpcRequest::with_params(id.into(), method, params);
        match process_request(&self.db, request).await {
            Ok(v) => {
                if id != 0 {
                    if let Some(e) = v.error {
                        Err(RpcError::new(
                            e.kind() as i16,
                            Some(RpcError::convert_data(e.get_message())),
                        ))
                    } else if let Some(payload) = v.result {
                        Ok(Some(rmp_serde::to_vec_named(&payload)?))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Err(_) => Err(RpcError::internal(None)),
        }
    }
}

#[macro_export]
macro_rules! parse_jsonrpc_request_param {
    ($r:expr, $k:expr, $p:path) => {
        if let Some($p(v)) = $r.params.get($k) {
            Some(v)
        } else {
            None
        }
    };
}

#[derive(Debug, Eq, PartialEq)]
pub enum YedbServerErrorKind {
    Critical,
    #[allow(dead_code)]
    Other,
}

/// # Panics
///
/// Should not panic
#[allow(clippy::too_many_lines)]
pub async fn process_request(
    db: &RwLock<Database>,
    request: JSONRpcRequest,
) -> Result<JSONRpcResponse<Value>, YedbServerErrorKind> {
    macro_rules! invalid_param {
        () => {
            request.error(Error::err_invalid_parameter())
        };
    }
    macro_rules! run_request {
        ($params: expr, $then: block) => {
            if request.params_valid($params)
                $then
            else {
                invalid_param!()
            }
        }
    }
    macro_rules! respond {
        ($result: expr) => {
            match $result {
                Ok(v) => request.respond(json!(v)),
                Err(e) => request.error(e),
            }
        };
    }
    Ok(match request.method.as_str() {
        "test" => {
            trace!("API request: test");
            run_request!(vec![], { request.respond(json!(crate::ServerInfo::new())) })
        }
        "info" => {
            trace!("API request: info");
            run_request!(vec![], { respond!(db.write().await.info()) })
        }
        "server_set" => run_request!(vec!["name", "value"], {
            match parse_jsonrpc_request_param!(request, "name", Value::String) {
                Some(name) => {
                    let value = request.params.get("value").unwrap();
                    trace!("API request: server_set {}={}", name, value);
                    respond!(db.write().await.server_set(name, value.clone()))
                }
                None => invalid_param!(),
            }
        }),
        "key_get" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    trace!("API request: key_get {}", v);
                    respond!(db.write().await.key_get(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_get_field" => run_request!(vec!["key", "field"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let field = parse_jsonrpc_request_param!(request, "field", Value::String);
            if key.is_some() && field.is_some() {
                let k = key.unwrap();
                let f = field.unwrap();
                trace!("API request: key_get_field {}:{}", k, f);
                respond!(db.write().await.key_get_field(k, f))
            } else {
                invalid_param!()
            }
        }),
        "key_get_recursive" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    trace!("API request: key_get_recursive {}", v);
                    respond!(db.write().await.key_get_recursive(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_explain" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    trace!("API request: key_explain {}", v);
                    respond!(db.write().await.key_explain(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_list" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    trace!("API request: key_list {}", v);
                    respond!(db.write().await.key_list(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_list_all" => run_request!(vec!["key"], {
            match parse_jsonrpc_request_param!(request, "key", Value::String) {
                Some(v) => {
                    trace!("API request: key_list_all {}", v);
                    respond!(db.write().await.key_list_all(v))
                }
                None => invalid_param!(),
            }
        }),
        "key_set" => run_request!(vec!["key", "value"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let value = request.params.get("value").cloned();
            if key.is_some() && value.is_some() {
                let k = key.unwrap();
                trace!("API request: key_set {}", k);
                respond!(db.write().await.key_set(k, value.unwrap()))
            } else {
                invalid_param!()
            }
        }),
        "key_set_field" => run_request!(vec!["key", "field", "value"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let field = parse_jsonrpc_request_param!(request, "field", Value::String);
            let value = request.params.get("value").cloned();
            if key.is_some() && field.is_some() && value.is_some() {
                let k = key.unwrap();
                let f = field.unwrap();
                trace!("API request: key_set_field {}:{}", k, f);
                respond!(db.write().await.key_set_field(k, f, value.unwrap()))
            } else {
                invalid_param!()
            }
        }),
        "key_delete_field" => run_request!(vec!["key", "field"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let field = parse_jsonrpc_request_param!(request, "field", Value::String);
            if key.is_some() && field.is_some() {
                let k = key.unwrap();
                let f = field.unwrap();
                trace!("API request: key_delete_field {}:{}", k, f);
                respond!(db.write().await.key_delete_field(k, f))
            } else {
                invalid_param!()
            }
        }),
        "key_increment" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                trace!("API request: key_get {}", v);
                respond!(db.write().await.key_increment(v))
            } else {
                invalid_param!()
            }
        }),
        "key_decrement" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                trace!("API request: key_get {}", v);
                respond!(db.write().await.key_decrement(v))
            } else {
                invalid_param!()
            }
        }),
        "key_copy" => run_request!(vec!["key", "dst_key"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let dst_key = parse_jsonrpc_request_param!(request, "dst_key", Value::String);
            if key.is_some() && dst_key.is_some() {
                let k = key.unwrap();
                let dk = dst_key.unwrap();
                trace!("API request: key_copy {} -> {}", k, dk);
                respond!(db.write().await.key_copy(k, dk))
            } else {
                invalid_param!()
            }
        }),
        "key_rename" => run_request!(vec!["key", "dst_key"], {
            let key = parse_jsonrpc_request_param!(request, "key", Value::String);
            let dst_key = parse_jsonrpc_request_param!(request, "dst_key", Value::String);
            if key.is_some() && dst_key.is_some() {
                let k = key.unwrap();
                let dk = dst_key.unwrap();
                trace!("API request: key_rename {} -> {}", k, dk);
                respond!(db.write().await.key_rename(k, dk))
            } else {
                invalid_param!()
            }
        }),
        "key_delete" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                trace!("API request: key_delete {}", v);
                respond!(db.write().await.key_delete(v))
            } else {
                invalid_param!()
            }
        }),
        "key_delete_recursive" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                trace!("API request: key_delete_recursive {}", v);
                respond!(db.write().await.key_delete_recursive(v))
            } else {
                invalid_param!()
            }
        }),
        "check" => {
            trace!("API request: check");
            run_request!(vec![], { respond!(db.write().await.check()) })
        }
        "repair" => {
            trace!("API request: repair");
            run_request!(vec![], { respond!(db.write().await.repair()) })
        }
        "purge" => {
            trace!("API request: purge");
            run_request!(vec![], { respond!(db.write().await.purge()) })
        }
        "purge_cache" => {
            trace!("API request: purge_cache");
            run_request!(vec![], { respond!(db.write().await.purge_cache()) })
        }
        "safe_purge" => {
            trace!("API request: safe_purge");
            run_request!(vec![], { respond!(db.write().await.safe_purge()) })
        }
        "key_dump" => run_request!(vec!["key"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "key", Value::String) {
                trace!("API request: key_dump {}", v);
                respond!(db.write().await.key_dump(v))
            } else {
                invalid_param!()
            }
        }),
        "key_load" => run_request!(vec!["data"], {
            if let Some(v) = parse_jsonrpc_request_param!(request, "data", Value::Array) {
                trace!("API request: key_load");
                respond!(db.write().await.key_load_from_serialized(v))
            } else {
                invalid_param!()
            }
        }),
        _ => request.error(Error::err_method_not_found()),
    })
}
