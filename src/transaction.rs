use log::{error, info};
use std::cell::RefCell;
use std::cmp::min;
use std::io::prelude::*;
use std::sync::mpsc::{channel, Receiver, SendError, Sender};
use std::vec::Vec;
use zstream::{Decoder, Encoder};

const INPUT_BUFFER_SIZE: usize = 32 * 1024;
const ENCODER_BUFFER_SIZE: usize = 1024 * 1024;

struct BufferReader {
    receiver: Receiver<Vec<u8>>,
    pending: Vec<u8>,
}

impl Read for BufferReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let _n_data = match self.receiver.try_recv() {
            Ok(data) => {
                let n_data = data.len();
                self.pending.extend(data);
                n_data
            }
            Err(_) => 0,
        };

        let to_transfer = min(buf.len(), self.pending.len());
        let drained: Vec<u8> = self.pending.drain(0..to_transfer).collect();
        buf[0..to_transfer].copy_from_slice(&drained[0..to_transfer]);

        Ok(to_transfer)
    }
}

pub struct RawDataReader {
    pub reader: RefCell<Decoder>,
    inner_buffer: RefCell<Vec<u8>>,
}

impl RawDataReader {
    pub fn new(reader: Decoder) -> Self {
        RawDataReader {
            reader: RefCell::new(reader),
            inner_buffer: RefCell::new(Vec::<u8>::new()),
        }
    }

    pub fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut temp_buf = vec![0; buf.len()];
        let result = self.reader.borrow_mut().read(temp_buf.as_mut_slice());
        match result {
            Ok(bytes) => {
                self.inner_buffer
                    .borrow_mut()
                    .extend(temp_buf[0..bytes].to_vec());
                buf.copy_from_slice(temp_buf.as_slice());
            }
            _ => (),
        };

        result
    }

    pub fn extract(&self) -> Vec<u8> {
        self.inner_buffer.borrow().to_vec()
    }
}

pub struct RawDataWrapper {
    reader: std::rc::Rc<RawDataReader>,
}

impl RawDataWrapper {
    pub fn new(reader: std::rc::Rc<RawDataReader>) -> Self {
        RawDataWrapper { reader: reader }
    }
}

impl Read for RawDataWrapper {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}

pub struct Transaction {
    pub id: i64,
    pub uri: String,
    pub method: String,
    pub is_done: bool,
    pub encoding: Option<String>,
    pub transfer_chunk: Vec<u8>,
    pub bytes_total: usize,
    pub bytes_sender: Sender<Vec<u8>>,
    pub bytes_receiver: Receiver<Vec<u8>>,
    pub encoder: Encoder,
    pub decoder_sender: Sender<Vec<u8>>,
    pub error: bool,
    pub data_reader: std::rc::Rc<RawDataReader>,
}

impl Transaction {
    pub fn new(id: i64, method: String, uri: String, encoding: Option<&String>) -> Self {
        let (bytes_sender, bytes_receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel();
        let (decoder_sender, decoder_receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel();

        let data_reader = std::rc::Rc::new(RawDataReader::new(Decoder::new_with_size(
            BufferReader {
                receiver: decoder_receiver,
                pending: Vec::<u8>::new(),
            },
            INPUT_BUFFER_SIZE,
        )));
        let wrapper = RawDataWrapper::new(data_reader.clone());

        Transaction {
            id: id,
            uri: uri,
            is_done: false,
            method: method,
            encoding: encoding.cloned(),
            transfer_chunk: Vec::<u8>::new(),
            bytes_total: 0,
            bytes_sender: bytes_sender,
            bytes_receiver: bytes_receiver,
            encoder: Encoder::new_with_size(wrapper, ENCODER_BUFFER_SIZE),
            decoder_sender: decoder_sender,
            error: false,
            data_reader: data_reader,
        }
    }

    pub fn done(&mut self) {
        self.is_done = true;
        info!(
            "Transaction {} is set as done for uri: {}",
            self.id, self.uri
        );
    }

    pub fn write_bytes(&mut self, data: &[u8]) {
        let sender = {
            match &self.encoding {
                Some(encoding) => {
                    if encoding == "gzip" {
                        &self.decoder_sender
                    } else {
                        &self.bytes_sender
                    }
                }
                None => &self.bytes_sender,
            }
        };

        match sender.send(data.to_vec()) {
            Ok(()) => {
                self.bytes_total += data.len();
            }
            Err(SendError(sent)) => {
                error!("Failed to send {} bytes", sent.len());
            }
        }
    }

    pub fn body(&self) -> Vec<u8> {
        self.data_reader.extract()
    }
}
