use serde_json::Value;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::UnixStream;

use std::time::Duration;

use super::common::{DBInfo, Error, ErrorKind, JSONRpcRequest, JSONRpcResponse, KeyExplained};
use simple_pool::{ResourcePool, ResourcePoolGuard};

pub struct YedbClientPoolAsyncBuilder {}

pub struct YedbClientPoolAsync {
    pool: ResourcePool<YedbClientAsync>,
    size: usize,
    path: String,
    retries: u8,
    timeout: Duration,
}

impl Default for YedbClientPoolAsync {
    fn default() -> Self {
        Self::create()
    }
}

impl YedbClientPoolAsync {
    pub fn create() -> Self {
        Self {
            pool: ResourcePool::new(),
            size: 5,
            path: String::new(),
            retries: 3,
            timeout: Duration::from_secs(30),
        }
    }
    pub fn size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }
    pub fn path(mut self, path: &str) -> Self {
        self.path = path.to_owned();
        self
    }
    pub fn retries(mut self, retries: u8) -> Self {
        self.retries = retries;
        self
    }
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    pub fn build(self) -> Self {
        {
            let mut holder = self.pool.holder.lock().unwrap();
            for _ in 0..self.size {
                let mut client = YedbClientAsync::new(&self.path);
                client.retries = self.retries;
                client.timeout = self.timeout;
                holder.resources.push(client);
            }
        }
        self
    }
    pub async fn get(&self) -> ResourcePoolGuard<YedbClientAsync> {
        self.pool.get().await
    }
}

const INVALID_SERVER_VALUE: &str = "Invalid value received from the server";

macro_rules! error_invalid_value_received {
    () => {
        Err(Error::new(ErrorKind::ProtocolError, INVALID_SERVER_VALUE))
    };
}

macro_rules! safe_unwrap_opt {
    ( $opt:expr ) => {
        if let Some(x) = $opt {
            x
        } else {
            return error_invalid_value_received!();
        }
    };
}

macro_rules! result_ok {
    ( $s:expr, $r:expr ) => {
        match $s.call(&$r).await? {
            _ => Ok(()),
        }
    };
}

macro_rules! result_i64 {
    ( $s:expr, $r:expr ) => {
        match $s.call(&$r).await? {
            Value::Number(v) => Ok(safe_unwrap_opt!(v.as_i64())),
            _ => error_invalid_value_received!(),
        }
    };
}

enum ClientStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl ClientStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        match self {
            ClientStream::Tcp(v) => v.read_exact(buf).await,
            ClientStream::Unix(v) => v.read_exact(buf).await,
        }
    }
    async fn write(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        match self {
            ClientStream::Tcp(v) => v.write_all(buf).await,
            ClientStream::Unix(v) => v.write_all(buf).await,
        }
    }
}

pub struct YedbClientAsync {
    stream: Option<ClientStream>,
    request_id: u64,
    pub path: String,
    pub retries: u8,
    pub timeout: Duration,
}

impl YedbClientAsync {
    pub fn new(path: &str) -> Self {
        Self {
            stream: None,
            request_id: 0,
            retries: 3,
            path: path.to_owned(),
            timeout: Duration::from_secs(30),
        }
    }

    fn gen_id(&mut self) -> u64 {
        if self.request_id == std::u64::MAX {
            self.request_id = 0;
        }
        self.request_id += 1;
        self.request_id
    }

    async fn get_stream(&mut self) -> Result<&mut ClientStream, std::io::Error> {
        match self.stream {
            Some(ref mut v) => Ok(v),
            None => {
                let stream: ClientStream;
                if self.path.starts_with("tcp://") {
                    stream = ClientStream::Tcp(TcpStream::connect(&self.path[6..]).await?);
                } else {
                    stream = ClientStream::Unix(UnixStream::connect(&self.path).await?);
                }
                self.stream = Some(stream);
                Ok(self.stream.as_mut().unwrap())
            }
        }
    }

    pub async fn call(&mut self, req: &JSONRpcRequest) -> Result<Value, Error> {
        let mut attempt = 0;
        let started = std::time::Instant::now();
        loop {
            match tokio::time::timeout(self.timeout - started.elapsed(), self._call(req)).await {
                Ok(v) => match v {
                    Ok(v) => return Ok(v),
                    Err(e) if e.kind() == ErrorKind::ProtocolError => {
                        attempt += 1;
                        if attempt > self.retries {
                            return Err(e);
                        } else {
                            self.stream = None;
                        }
                    }
                    Err(e) => return Err(e),
                },
                Err(_) => return Err(Error::new(ErrorKind::TimeoutError, "timed out")),
            }
        }
    }

    async fn _call(&mut self, req: &JSONRpcRequest) -> Result<Value, Error> {
        let mut frame = vec![1u8, 2u8];
        let buf = req.pack()?;
        frame.extend(&(buf.len() as u32).to_le_bytes());
        frame.extend(&buf);
        let stream = self.get_stream().await?;
        stream.write(&frame).await?;
        let mut buf = [0u8; 6];
        stream.read(&mut buf).await?;
        if buf[0] != 1 || buf[1] != 2 {
            return Err(Error::new(
                ErrorKind::ProtocolError,
                "binary protocol error",
            ));
        }
        let frame_len = u32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]) as usize;
        let mut buf = vec![0u8; frame_len];
        stream.read(&mut buf).await?;
        let response: JSONRpcResponse<Value> = rmp_serde::from_read_ref(&buf)?;
        if response.id != req.id {
            return Err(Error::new(ErrorKind::ProtocolError, "invalid response id"));
        }
        match response.result {
            Some(value) => Ok(value),
            None => match response.error {
                Some(v) => Err(v),
                None => Ok(Value::Null),
            },
        }
    }
    pub async fn key_list(&mut self, key: &str) -> Result<Vec<String>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_list");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn key_list_all(&mut self, key: &str) -> Result<Vec<String>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_list_all");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn key_get(&mut self, key: &str) -> Result<Value, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_get");
        req.set_param("key", Value::from(key));
        self.call(&req).await
    }

    pub async fn key_get_field(&mut self, key: &str, field: &str) -> Result<Value, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_get_field");
        req.set_param("key", Value::from(key));
        req.set_param("field", Value::from(field));
        self.call(&req).await
    }

    pub async fn key_get_recursive(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_get_recursive");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn key_copy(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_copy");
        req.set_param("key", Value::from(key));
        req.set_param("dst_key", Value::from(dst_key));
        result_ok!(self, req)
    }

    pub async fn key_rename(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_rename");
        req.set_param("key", Value::from(key));
        req.set_param("dst_key", Value::from(dst_key));
        result_ok!(self, req)
    }

    pub async fn key_explain(&mut self, key: &str) -> Result<KeyExplained, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_explain");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn key_set(&mut self, key: &str, value: Value) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_set");
        req.set_param("key", Value::from(key));
        req.set_param("value", value);
        result_ok!(self, req)
    }

    pub async fn key_set_field(
        &mut self,
        key: &str,
        field: &str,
        value: Value,
    ) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_set_field");
        req.set_param("key", Value::from(key));
        req.set_param("field", Value::from(field));
        req.set_param("value", value);
        result_ok!(self, req)
    }

    pub async fn key_delete_field(&mut self, key: &str, field: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_delete_field");
        req.set_param("key", Value::from(key));
        req.set_param("field", Value::from(field));
        result_ok!(self, req)
    }

    pub async fn key_increment(&mut self, key: &str) -> Result<i64, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_increment");
        req.set_param("key", Value::from(key));
        result_i64!(self, req)
    }

    pub async fn key_decrement(&mut self, key: &str) -> Result<i64, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_decrement");
        req.set_param("key", Value::from(key));
        result_i64!(self, req)
    }

    pub async fn key_delete(&mut self, key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_delete");
        req.set_param("key", Value::from(key));
        result_ok!(self, req)
    }

    pub async fn key_delete_recursive(&mut self, key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_delete_recursive");
        req.set_param("key", Value::from(key));
        result_ok!(self, req)
    }

    pub async fn server_set(&mut self, name: &str, value: Value) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "server_set");
        req.set_param("name", Value::from(name));
        req.set_param("value", value);
        result_ok!(self, req)
    }

    pub async fn info(&mut self) -> Result<DBInfo, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "info");
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn test(&mut self) -> Result<(), Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "test");
        result_ok!(self, req)
    }
    pub async fn check(&mut self) -> Result<Vec<String>, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "check");
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn repair(&mut self) -> Result<Vec<(String, bool)>, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "repair");
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn purge(&mut self) -> Result<Vec<String>, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "purge");
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn purge_cache(&mut self) -> Result<(), Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "purge_cache");
        result_ok!(self, req)
    }

    #[allow(dead_code)]
    pub async fn safe_purge(&mut self) -> Result<(), Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "safe_purge");
        result_ok!(self, req)
    }

    pub async fn key_dump(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_dump");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn key_load(&mut self, data: Vec<(String, Value)>) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_load");
        let data_to_load: Vec<Value> = data
            .into_iter()
            .map(|v| Value::from(vec![Value::from(v.0), v.1]))
            .collect();
        req.set_param("data", Value::from(data_to_load));
        result_ok!(self, req)
    }
}
