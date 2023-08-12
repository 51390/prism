use crate::transaction::Transaction;
use log::warn;
use serde::Serialize;
use std::result::Result;

pub trait Backend {
    fn persist(&self, data: &Transaction) -> Result<(), ()>;
}

#[derive(Serialize)]
struct Document {
    method: String,
    uri: String,
    body: String,
}

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
}

impl Elasticsearch {
    pub fn new(hostname: String, port: i64, protocol: String, index: String) -> Self {
        let client = reqwest::blocking::Client::new();
        Elasticsearch {
            hostname,
            port,
            protocol,
            index,
            client,
        }
    }
}

impl Backend for Elasticsearch {
    fn persist(&self, data: &Transaction) -> Result<(), ()> {
        let body = data.body();
        let document = Document {
            method: data.method.clone(),
            uri: data.uri.clone(),
            body: body,
        };
        let json = serde_json::to_string(&document).unwrap();
        let id = data.id;
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
                        "Failed persisting data for transaction no. {} (http status {})",
                        id, status
                    );
                }
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Failed persisting data for transaction no. {} (error: {})",
                    id, e
                );
                Err(())
            }
        }
    }
}
