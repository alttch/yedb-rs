use serde_json::Value;

use super::common::{DBInfo, Error, KeyExplained};

use elbus::rpc::Rpc;
use elbus::QoS;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::YedbClientAsyncExt;

#[allow(clippy::module_name_repetitions)]
pub struct YedbClientElbusAsync<R>
where
    R: Rpc + Send + Sync,
{
    rpc: Arc<R>,
    target: String,
    qos: QoS,
}

macro_rules! do_call {
    ($self: expr, $method: expr, $payload: expr) => {{
        let result = $self
            .rpc
            .call(&$self.target, $method, $payload, $self.qos)
            .await?;
        rmp_serde::from_read_ref(&result.payload()).map_err(Into::<Error>::into)
    }};
}

macro_rules! call {
    ($self: expr, $method: expr, $params: expr) => {{
        let mut params = BTreeMap::new();
        dbg!($params);
        while let Some((k, v)) = $params.pop() {
            dbg!(&k, &v);
            params.insert(k, v);
        }
        dbg!(&params);
        do_call!($self, $method, rmp_serde::to_vec_named(&params)?.into())
    }};
    ($self: expr, $method: expr) => {{
        do_call!($self, $method, elbus::empty_payload!())
    }};
}

impl<R> YedbClientElbusAsync<R>
where
    R: Rpc + Send + Sync,
{
    pub fn new(rpc: Arc<R>, target: &str, qos: QoS) -> Self {
        Self {
            rpc,
            target: target.to_owned(),
            qos,
        }
    }
}

#[async_trait::async_trait]
impl<R> YedbClientAsyncExt for YedbClientElbusAsync<R>
where
    R: Rpc + Send + Sync,
{
    async fn key_list(&mut self, key: &str) -> Result<Vec<String>, Error> {
        call!(self, "key_list", vec![("key", Value::from(key))])
    }
    async fn key_list_all(&mut self, key: &str) -> Result<Vec<String>, Error> {
        call!(self, "key_list_all", vec![("key", Value::from(key))])
    }
    async fn key_get(&mut self, key: &str) -> Result<Value, Error> {
        call!(self, "key_get", vec![("key", Value::from(key))])
    }
    async fn key_get_field(&mut self, key: &str, field: &str) -> Result<Value, Error> {
        call!(
            self,
            "key_get_field",
            vec![("key", Value::from(key)), ("field", Value::from(field))]
        )
    }
    async fn key_get_recursive(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        call!(self, "key_get_recursive", vec![("key", Value::from(key))])
    }
    async fn key_copy(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        call!(
            self,
            "key_copy",
            vec![("key", Value::from(key)), ("dst_key", Value::from(dst_key))]
        )
    }
    async fn key_rename(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        call!(
            self,
            "key_rename",
            vec![("key", Value::from(key)), ("dst_key", Value::from(dst_key))]
        )
    }
    async fn key_explain(&mut self, key: &str) -> Result<KeyExplained, Error> {
        call!(self, "key_explain", vec![("key", Value::from(key))])
    }
    async fn key_set(&mut self, key: &str, value: Value) -> Result<(), Error> {
        let k = Value::from(key);
        call!(self, "key_set", vec![("key", &k), ("value", &value)])
    }
    async fn key_set_field(&mut self, key: &str, field: &str, value: Value) -> Result<(), Error> {
        let k = Value::from(key);
        let f = Value::from(field);
        call!(
            self,
            "key_set_field",
            vec![("key", &k), ("field", &f), ("value", &value)]
        )
    }
    async fn key_delete_field(&mut self, key: &str, field: &str) -> Result<(), Error> {
        call!(
            self,
            "key_delete_field",
            vec![("key", Value::from(key)), ("field", Value::from(field))]
        )
    }
    async fn key_increment(&mut self, key: &str) -> Result<i64, Error> {
        call!(self, "key_increment", vec![("key", Value::from(key))])
    }
    async fn key_decrement(&mut self, key: &str) -> Result<i64, Error> {
        call!(self, "key_decrement", vec![("key", Value::from(key))])
    }
    async fn key_delete(&mut self, key: &str) -> Result<(), Error> {
        call!(self, "key_delete", vec![("key", Value::from(key))])
    }
    async fn key_delete_recursive(&mut self, key: &str) -> Result<(), Error> {
        call!(
            self,
            "key_delete_recursive",
            vec![("key", Value::from(key))]
        )
    }
    async fn server_set(&mut self, name: &str, value: Value) -> Result<(), Error> {
        let n = Value::from(name);
        call!(self, "server_set", vec![("name", &n), ("value", &value)])
    }
    async fn info(&mut self) -> Result<DBInfo, Error> {
        call!(self, "info")
    }
    async fn test(&mut self) -> Result<(), Error> {
        let _result: BTreeMap<String, Value> = call!(self, "test")?;
        Ok(())
    }
    async fn check(&mut self) -> Result<Vec<String>, Error> {
        call!(self, "check")
    }
    async fn repair(&mut self) -> Result<Vec<(String, bool)>, Error> {
        call!(self, "repair")
    }
    async fn purge(&mut self) -> Result<Vec<String>, Error> {
        call!(self, "purge")
    }
    async fn purge_cache(&mut self) -> Result<(), Error> {
        call!(self, "purge_cache")
    }
    async fn safe_purge(&mut self) -> Result<(), Error> {
        call!(self, "safe_purge")
    }
    async fn key_dump(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        call!(self, "key_dump", vec![("key", Value::from(key))])
    }
    async fn key_load(&mut self, data: Vec<(String, Value)>) -> Result<(), Error> {
        let data_to_load: Vec<Value> = data
            .into_iter()
            .map(|v| Value::from(vec![Value::from(v.0), v.1]))
            .collect();
        let v = Value::from(data_to_load);
        call!(self, "key_load", vec![("data", &v)])
    }
}
