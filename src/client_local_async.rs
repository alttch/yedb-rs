use crate::common::{DBInfo, Error, KeyExplained};
use crate::Database;
use crate::YedbClientAsyncExt;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[allow(clippy::module_name_repetitions)]
pub struct YedbClientLocalAsync {
    db: Arc<RwLock<Database>>,
}

impl YedbClientLocalAsync {
    pub fn open(path: &str, timeout: Duration) -> Result<Self, Error> {
        let mut db = Database::new();
        db.timeout = timeout;
        db.set_db_path(path)?;
        db.open()?;
        Ok(Self {
            db: Arc::new(RwLock::new(db)),
        })
    }
    #[inline]
    pub fn db(&self) -> &RwLock<Database> {
        &self.db
    }
}

#[async_trait::async_trait]
impl YedbClientAsyncExt for YedbClientLocalAsync {
    async fn key_list(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.db.write().await.key_list(key)
    }
    async fn key_list_all(&mut self, key: &str) -> Result<Vec<String>, Error> {
        self.db.write().await.key_list_all(key)
    }
    async fn key_get(&mut self, key: &str) -> Result<Value, Error> {
        self.db.write().await.key_get(key)
    }
    async fn key_get_field(&mut self, key: &str, field: &str) -> Result<Value, Error> {
        self.db.write().await.key_get_field(key, field)
    }
    async fn key_get_recursive(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        self.db.write().await.key_get_recursive(key)
    }
    async fn key_copy(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        self.db.write().await.key_copy(key, dst_key)
    }
    async fn key_rename(&mut self, key: &str, dst_key: &str) -> Result<(), Error> {
        self.db.write().await.key_rename(key, dst_key)
    }
    async fn key_explain(&mut self, key: &str) -> Result<KeyExplained, Error> {
        self.db.write().await.key_explain(key)
    }
    async fn key_set(&mut self, key: &str, value: Value) -> Result<(), Error> {
        self.db.write().await.key_set(key, value)
    }
    async fn key_set_field(&mut self, key: &str, field: &str, value: Value) -> Result<(), Error> {
        self.db.write().await.key_set_field(key, field, value)
    }
    async fn key_delete_field(&mut self, key: &str, field: &str) -> Result<(), Error> {
        self.db.write().await.key_delete_field(key, field)
    }
    async fn key_increment(&mut self, key: &str) -> Result<i64, Error> {
        self.db.write().await.key_increment(key)
    }
    async fn key_decrement(&mut self, key: &str) -> Result<i64, Error> {
        self.db.write().await.key_decrement(key)
    }
    async fn key_delete(&mut self, key: &str) -> Result<(), Error> {
        self.db.write().await.key_delete(key)
    }
    async fn key_delete_recursive(&mut self, key: &str) -> Result<(), Error> {
        self.db.write().await.key_delete_recursive(key)
    }
    async fn server_set(&mut self, name: &str, value: Value) -> Result<(), Error> {
        self.db.write().await.server_set(name, value)
    }
    async fn info(&mut self) -> Result<DBInfo, Error> {
        self.db.write().await.info()
    }
    async fn test(&mut self) -> Result<(), Error> {
        Ok(())
    }
    async fn check(&mut self) -> Result<Vec<String>, Error> {
        self.db.write().await.check()
    }
    async fn repair(&mut self) -> Result<Vec<(String, bool)>, Error> {
        self.db.write().await.repair()
    }
    async fn purge(&mut self) -> Result<Vec<String>, Error> {
        self.db.write().await.purge()
    }
    async fn purge_cache(&mut self) -> Result<(), Error> {
        self.db.write().await.purge_cache()
    }
    async fn safe_purge(&mut self) -> Result<(), Error> {
        self.db.write().await.safe_purge()
    }
    async fn key_dump(&mut self, key: &str) -> Result<Vec<(String, Value)>, Error> {
        self.db.write().await.key_dump(key)
    }
    async fn key_load(&mut self, data: Vec<(String, Value)>) -> Result<(), Error> {
        self.db.write().await.key_load(data)
    }
}
