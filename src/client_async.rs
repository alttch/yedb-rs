use serde_json::Value;

use std::io;
use tokio::io::Interest;
use tokio::net::TcpStream;
use tokio::net::UnixStream;

use std::time::Duration;

use super::common::{
    DBInfo,
    Error, ErrorKind, JSONRpcRequest, JSONRpcResponse,
//    KeyExplained
};

//const INVALID_SERVER_VALUE: &str = "Invalid value received from the server";

macro_rules! result_ok {
    ( $s:expr, $r:expr ) => {
        match $s.call(&$r).await? {
            _ => Ok(()),
        }
    };
}

enum ClientStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

macro_rules! read_stream {
    ($stream: expr, $buf: expr) => {{
        let _ready = $stream.ready(Interest::READABLE).await?;
        loop {
            match $stream.try_read($buf) {
                Ok(_) => {
                    return Ok(());
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }};
}

macro_rules! write_stream {
    ($stream: expr, $buf: expr) => {{
        let _ready = $stream.ready(Interest::WRITABLE).await?;
        loop {
            match $stream.try_write($buf) {
                Ok(_) => {
                    return Ok(());
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }};
}

impl ClientStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<(), std::io::Error> {
        match self {
            ClientStream::Tcp(v) => {
                read_stream!(v, buf)
            }
            ClientStream::Unix(v) => {
                read_stream!(v, buf)
            }
        }
    }
    async fn write(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        match self {
            ClientStream::Tcp(v) => {
                write_stream!(v, buf)
            }
            ClientStream::Unix(v) => {
                write_stream!(v, buf)
            }
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
                    stream = ClientStream::Tcp(
                        tokio::time::timeout(self.timeout, TcpStream::connect(&self.path[6..]))
                            .await??,
                    );
                } else {
                    stream = ClientStream::Unix(
                        tokio::time::timeout(self.timeout, UnixStream::connect(&self.path))
                            .await??,
                    );
                }
                self.stream = Some(stream);
                Ok(self.stream.as_mut().unwrap())
            }
        }
    }

    pub async fn call(&mut self, req: &JSONRpcRequest) -> Result<Value, Error> {
        let mut attempt = 0;
        loop {
            match self._call(req).await {
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
    pub async fn info(&mut self) -> Result<DBInfo, Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "info");
        Ok(serde_json::from_value(self.call(&req).await?)?)
    }

    pub async fn test(&mut self) -> Result<(), Error> {
        let req = JSONRpcRequest::new(self.gen_id(), "test");
        result_ok!(self, req)
    }
}
