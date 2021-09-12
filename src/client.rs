use serde_json::Value;

use std::io::prelude::*;
use std::io::Read;
use std::net::TcpStream;
use std::os::unix::net::UnixStream;

use std::time::Duration;

use super::common::{DBInfo, Error, ErrorKind, JSONRpcRequest, JSONRpcResponse, KeyExplained};

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
        match $s.call(&$r)? {
            _ => Ok(()),
        }
    };
}

macro_rules! result_i64 {
    ( $s:expr, $r:expr ) => {
        match $s.call(&$r)? {
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
    fn set_timeout(&mut self, timeout: Duration) -> Result<(), std::io::Error> {
        match self {
            ClientStream::Tcp(v) => {
                v.set_read_timeout(Some(timeout))?;
                v.set_write_timeout(Some(timeout))?;
            }
            ClientStream::Unix(v) => {
                v.set_read_timeout(Some(timeout))?;
                v.set_write_timeout(Some(timeout))?;
            }
        }
        Ok(())
    }
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), std::io::Error> {
        match self {
            ClientStream::Tcp(v) => v.read_exact(buf),
            ClientStream::Unix(v) => v.read_exact(buf),
        }
    }
    fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        match self {
            ClientStream::Tcp(v) => v.write_all(buf),
            ClientStream::Unix(v) => v.write_all(buf),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct YedbClient {
    stream: Option<ClientStream>,
    request_id: u64,
    pub path: String,
    pub retries: u8,
    pub timeout: Duration,
}

impl YedbClient {
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

    fn get_stream(&mut self) -> Result<&mut ClientStream, std::io::Error> {
        if let Some(ref mut v) = self.stream {
            Ok(v)
        } else {
            let mut stream: ClientStream;
            if self.path.starts_with("tcp://") {
                stream = ClientStream::Tcp(TcpStream::connect(&self.path[6..])?);
            } else {
                stream = ClientStream::Unix(UnixStream::connect(&self.path)?);
            }
            stream.set_timeout(self.timeout)?;
            self.stream = Some(stream);
            Ok(self.stream.as_mut().unwrap())
        }
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn call(&mut self, req: &JSONRpcRequest) -> Result<Value, Error> {
        let mut attempt = 0;
        loop {
            match self._call(req) {
                Ok(v) => return Ok(v),
                Err(e) if e.kind() == ErrorKind::ProtocolError => {
                    attempt += 1;
                    if attempt > self.retries {
                        return Err(e);
                    }
                    self.stream = None;
                }
                Err(e) => return Err(e),
            }
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn _call(&mut self, req: &JSONRpcRequest) -> Result<Value, Error> {
        let mut frame = vec![1_u8, 2_u8];
        let buf = req.pack()?;
        frame.extend(&(buf.len() as u32).to_le_bytes());
        frame.extend(&buf);
        let stream = self.get_stream()?;
        stream.write_all(&frame)?;
        let mut buf = [0_u8; 6];
        stream.read_exact(&mut buf)?;
        if buf[0] != 1 || buf[1] != 2 {
            return Err(Error::new(
                ErrorKind::ProtocolError,
                "binary protocol error",
            ));
        }
        let frame_len = u32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]) as usize;
        let mut buf = vec![0_u8; frame_len];
        stream.read_exact(&mut buf)?;
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

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_list(&mut self, key: &str) -> Result<Vec<String>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_list");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_list_all(&mut self, key: &str) -> Result<Vec<String>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_list_all");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_get(&mut self, key: &str) -> Result<Value, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_get");
        req.set_param("key", Value::from(key));
        self.call(&req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_get_field(&mut self, key: &str, field: &str) -> Result<Value, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_get_field");
        req.set_param("key", Value::from(key));
        req.set_param("field", Value::from(field));
        self.call(&req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_get_recursive(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_get_recursive");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_copy(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_copy");
        req.set_param("key", Value::from(key));
        req.set_param("dst_key", Value::from(dst_key));
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_rename(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_rename");
        req.set_param("key", Value::from(key));
        req.set_param("dst_key", Value::from(dst_key));
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_explain(&mut self, key: &str) -> Result<KeyExplained, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_explain");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_set(&mut self, key: &str, value: Value) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_set");
        req.set_param("key", Value::from(key));
        req.set_param("value", value);
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_set_field(&mut self, key: &str, field: &str, value: Value) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_set_field");
        req.set_param("key", Value::from(key));
        req.set_param("field", Value::from(field));
        req.set_param("value", value);
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_delete_field(&mut self, key: &str, field: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_delete_field");
        req.set_param("key", Value::from(key));
        req.set_param("field", Value::from(field));
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_increment(&mut self, key: &str) -> Result<i64, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_increment");
        req.set_param("key", Value::from(key));
        result_i64!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_decrement(&mut self, key: &str) -> Result<i64, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_decrement");
        req.set_param("key", Value::from(key));
        result_i64!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_delete(&mut self, key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_delete");
        req.set_param("key", Value::from(key));
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_delete_recursive(&mut self, key: &str) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_delete_recursive");
        req.set_param("key", Value::from(key));
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn server_set(&mut self, name: &str, value: Value) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "server_set");
        req.set_param("name", Value::from(name));
        req.set_param("value", value);
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn info(&mut self) -> Result<DBInfo, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "info");
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn test(&mut self) -> Result<(), Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "test");
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn check(&mut self) -> Result<Vec<String>, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "check");
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn repair(&mut self) -> Result<Vec<(String, bool)>, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "repair");
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn purge(&mut self) -> Result<Vec<String>, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "purge");
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn purge_cache(&mut self) -> Result<(), Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "purge_cache");
        result_ok!(self, req)
    }

    #[allow(dead_code)]
    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn safe_purge(&mut self) -> Result<(), Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "safe_purge");
        result_ok!(self, req)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_dump(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_dump");
        req.set_param("key", Value::from(key));
        Ok(serde_json::from_value(self.call(&req)?)?)
    }

    /// # Errors
    ///
    /// Will return Err on failed calls
    pub fn key_load(&mut self, data: Vec<(String, Value)>) -> Result<(), Error> {
        let mut req = JSONRpcRequest::new(self.gen_id(), "key_load");
        let data_to_load: Vec<Value> = data
            .into_iter()
            .map(|v| Value::from(vec![Value::from(v.0), v.1]))
            .collect();
        req.set_param("data", Value::from(data_to_load));
        result_ok!(self, req)
    }
}
