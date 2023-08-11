use crate::buffer::Buffer;
use std::result::Result;
use serde::Serialize;

pub trait Backend {
    fn persist(&self, data: &Buffer) -> Result<(), ()>;
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
    client: reqwest::blocking::Client,
}

impl Elasticsearch {
    pub fn new(hostname: String, port: i64, protocol: String, index: String) -> Self {
        let client = reqwest::blocking::Client::new();
        Elasticsearch{ hostname, port, protocol, index, client }
    }
}

impl Backend for Elasticsearch {
    fn persist(&self, data: &Buffer) -> Result<(), ()> {
        let body = data.body();
        let document = Document {
            method: data.method.clone(),
            uri: data.uri.clone(),
            body: body,
        };
        let json = serde_json::to_string(&document).unwrap();
        let id = data.id;
        let target = format!(
            "{}://{}:{}/{}/_doc/{}",
            self.protocol, self.hostname, self.port, self.index, id
        );
        let response = self.client.put(target).
            header("Content-Type", "application/json").
            body(json);
        Ok(())
    }
}
