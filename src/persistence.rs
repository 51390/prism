use crate::transaction::Transaction;
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use log::warn;
use serde::Serialize;
use std::result::Result;

pub trait Backend {
    fn persist(&self, transaction: &Transaction) -> Result<(), ()>;
}

#[derive(Serialize)]
struct Document {
    method: String,
    uri: String,
    body: String,
    raw_body: String,
    encoding: String,
    date: String,
}

static mut ELASTICSEARCH_INITIALIZED: bool = false;

/// Elasticsearch persistence backend.
pub struct Elasticsearch {
    /// Hostname of the ES instance.
    hostname: String,
    /// ES Port
    port: i64,
    /// Protocol to use to connect with ES
    protocol: String,
    /// Index name to use as storage
    index: String,
    /// Client used to communicate with ES.
    client: reqwest::blocking::Client,
    /// An integer to differentiate newly persisted items in this run
    /// from previously persisted ones, as the id counter for transactions
    /// is reset between executions.
    generation: u128,
}

impl Elasticsearch {
    pub fn new(hostname: String, port: i64, protocol: String, index: String) -> Self {
        let generation = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis();
        let client = reqwest::blocking::Client::new();
        let backend = Elasticsearch {
            hostname,
            port,
            protocol,
            index,
            client,
            generation,
        };

        if unsafe { ELASTICSEARCH_INITIALIZED } {
            backend
        } else {
            backend.initialize()
        }
    }

    fn check_initialized(&self, endpoint: &str) -> bool {
        match self.client.get(endpoint).send() {
            Ok(response) => response.status() == reqwest::StatusCode::OK,
            Err(_) => false,
        }
    }

    fn initialize(self) -> Self {
        let endpoint = format!(
            "{}://{}:{}/{}",
            self.protocol, self.hostname, self.port, self.index
        );

        if self.check_initialized(&endpoint) {
            unsafe { ELASTICSEARCH_INITIALIZED = true };
            return self;
        }

        let mapping = r#"
        {
            "mappings": {
                "properties": {
                    "method": {"type": "keyword"},
                    "uri": {"type": "text", "analyzer": "simple"},
                    "encoding": {"type": "keyword"},
                    "body": {"type": "text"},
                    "raw_body": { "type": "binary", "store": true },
                    "date": {"type": "date"}
                }
            }
        }
        "#;
        match self
            .client
            .put(&endpoint)
            .header("Content-Type", "application/json")
            .body(mapping)
            .send()
        {
            Ok(response) => {
                let status = response.status();
                if status != reqwest::StatusCode::OK {
                    warn!(
                        "Failed initializing elasticsearch backend, calls to persist transaction will fail (http {}) : {}",
                        status, response.text().unwrap()
                    );
                } else {
                    unsafe { ELASTICSEARCH_INITIALIZED = true };
                }
            }
            Err(err) => {
                warn!("Failed initializing elasticsearch backend, calls to persist transaction will fail: {}", err);
            }
        }

        self
    }

    fn date(&self) -> String {
        Utc::now().format("%Y-%m-%dY%H:%M:SZ").to_string()
    }

    fn raw_body(&self, body: &Vec<u8>) -> String {
        general_purpose::STANDARD.encode(body)
    }
}

impl Backend for Elasticsearch {
    fn persist(&self, transaction: &Transaction) -> Result<(), ()> {
        if !unsafe { ELASTICSEARCH_INITIALIZED } {
            return Err(());
        }

        let decoded_body = match String::from_utf8(transaction.body()) {
            Ok(body) => body,
            Err(_) => "".to_string(),
        };
        let document = Document {
            method: transaction.method.clone(),
            uri: transaction.uri.clone(),
            raw_body: self.raw_body(&transaction.body()),
            body: decoded_body,
            encoding: match &transaction.encoding {
                Some(encoding) => encoding.to_string(),
                None => "".to_string(),
            },
            date: self.date(),
        };
        let json = serde_json::to_string(&document).unwrap();
        let id = format!("{}-{}", self.generation, transaction.id);
        let endpoint = format!(
            "{}://{}:{}/{}/_doc/{}",
            self.protocol, self.hostname, self.port, self.index, id
        );
        match self
            .client
            .put(endpoint)
            .header("Content-Type", "application/json")
            .body(json)
            .send()
        {
            Ok(response) => {
                let status = response.status();
                let request_ok =
                    [reqwest::StatusCode::OK, reqwest::StatusCode::CREATED].contains(&status);
                if !request_ok {
                    warn!(
                        "Failed persisting transaction for transaction no. {} (http status {}): {}",
                        id,
                        status,
                        response.text().unwrap()
                    );
                    Err(())
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                warn!(
                    "Failed persisting transaction for transaction no. {} (error: {})",
                    id, err
                );
                Err(())
            }
        }
    }
}
