//! Starting commit 009d8d5's reused decoder, batching, mutex and flush behavior.

use arrow_array::RecordBatch;
use arrow_avro::reader::{Decoder as AvroDecoder, ReaderBuilder};
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
use arrow_schema::SchemaRef;
use parking_lot::Mutex;

pub(super) struct Decoder {
    store: SchemaStore,
    decoder: Mutex<Option<AvroDecoder>>,
}

impl Decoder {
    pub(super) fn new(native: &str) -> Self {
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
        store
            .set(Fingerprint::Id(7), AvroSchema::new(native.to_owned()))
            .unwrap();
        Self {
            store,
            decoder: Mutex::new(None),
        }
    }

    pub(super) fn decode(&self, records: &[&[u8]], schema: &SchemaRef) -> RecordBatch {
        if records.is_empty() {
            return RecordBatch::new_empty(schema.clone());
        }
        let mut guard = self.decoder.lock();
        let decoder = if let Some(decoder) = guard.as_mut() {
            decoder
        } else {
            let decoder = ReaderBuilder::new()
                .with_batch_size(8192)
                .with_writer_schema_store(self.store.clone())
                .build_decoder()
                .unwrap();
            guard.insert(decoder)
        };
        let mut partials = Vec::new();
        for record in records {
            let mut offset = 0;
            while offset < record.len() {
                let consumed = decoder.decode(&record[offset..]).unwrap();
                if consumed == 0 {
                    break;
                }
                offset += consumed;
                if decoder.batch_is_full() {
                    if let Some(batch) = decoder.flush().unwrap() {
                        partials.push(batch);
                    }
                }
            }
        }
        if let Some(batch) = decoder.flush().unwrap() {
            partials.push(batch);
        }
        match partials.len() {
            0 => panic!("baseline decoded no records"),
            1 => partials.pop().unwrap(),
            _ => arrow_select::concat::concat_batches(schema, &partials).unwrap(),
        }
    }
}
