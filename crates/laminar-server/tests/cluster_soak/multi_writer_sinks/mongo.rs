//! MongoDB insert mode: every node inserts its rows into one collection, so a replay adds
//! documents instead of overwriting them. The oracle counts the documents per id in snapshots of
//! the collection.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt as _;
use mongodb::bson::{doc, Bson, Document};
use mongodb::Client;

use super::{block_on, setting, Destination, Observed, Oracle};

/// Passed to the nodes as a reference, because a connection URI can carry credentials.
const URI: &str = "LAMINAR_SOAK_MONGODB_URI";
const DATABASE: &str = "laminar_soak";

pub(crate) struct MongoDb {
    uri: String,
}

impl MongoDb {
    pub(crate) fn from_environment() -> Self {
        Self {
            uri: setting(URI, None),
        }
    }

    /// The properties of a MongoDB sink that upserts into `collection` by `key`.
    #[cfg(all(feature = "postgres-cdc", feature = "postgres-sink"))]
    pub(crate) fn upsert_sink_properties(&self, collection: &str, key: &str) -> String {
        format!(
            r#""connection.uri" = "$${{{URI}}}"
database = "{DATABASE}"
collection = "{collection}"
"write.mode" = "upsert"
"write.mode.key_fields" = "{key}"
"#
        )
    }

    /// Create a unique index on `field` of `collection`, which an upsert sink shared by key needs.
    #[cfg(all(feature = "postgres-cdc", feature = "postgres-sink"))]
    pub(crate) fn create_unique_index(&self, collection: &str, field: &str) {
        let (uri, collection, field) = (self.uri.clone(), collection.to_owned(), field.to_owned());
        block_on(async move {
            Client::with_uri_str(&uri)
                .await
                .expect("connect to MongoDB")
                .database(DATABASE)
                .collection::<Document>(&collection)
                .create_index(
                    mongodb::IndexModel::builder()
                        .keys(doc! { field: 1 })
                        .options(
                            mongodb::options::IndexOptions::builder()
                                .unique(true)
                                .build(),
                        )
                        .build(),
                )
                .await
                .expect("create the unique key index");
        });
    }

    /// The documents of `collection`, in `field` order.
    #[cfg(all(feature = "postgres-cdc", feature = "postgres-sink"))]
    pub(crate) async fn documents(&self, collection: &str, field: &str) -> Vec<Document> {
        Client::with_uri_str(&self.uri)
            .await
            .expect("connect to MongoDB")
            .database(DATABASE)
            .collection::<Document>(collection)
            .find(doc! {})
            .sort(doc! { field: 1 })
            .await
            .expect("read the collection")
            .try_collect()
            .await
            .expect("read the documents")
    }
}

impl Destination for MongoDb {
    fn connector(&self) -> &'static str {
        "mongodb-sink"
    }

    fn written_metric(&self) -> &'static str {
        "laminardb_mongodb_sink_records_written_total"
    }

    fn format(&self) -> Option<&'static str> {
        None
    }

    fn name(&self, run_id: &str) -> String {
        format!("sink_{}", run_id.replace('-', "_"))
    }

    fn sink_properties(&self, collection: &str) -> String {
        format!(
            r#""connection.uri" = "$${{{URI}}}"
database = "{DATABASE}"
collection = "{collection}"
"write.mode" = "insert"
"#
        )
    }

    fn create(&self, collection: &str) {
        let uri = self.uri.clone();
        block_on(async move {
            Client::with_uri_str(&uri)
                .await
                .expect("connect to MongoDB")
                .database(DATABASE)
                .create_collection(collection)
                .await
                .expect("create the output collection");
        });
    }

    fn delete(&self, collection: &str) {
        let uri = self.uri.clone();
        block_on(async move {
            if let Ok(client) = Client::with_uri_str(&uri).await {
                let _ = client
                    .database(DATABASE)
                    .collection::<Document>(collection)
                    .drop()
                    .await;
            }
        });
    }

    fn spawn_oracle(&self, collection: &str) -> Oracle {
        let (uri, collection) = (self.uri.clone(), collection.to_owned());
        Oracle::spawn(move |observed, stop| {
            block_on(snapshot_loop(uri, collection, observed, stop));
        })
    }
}

async fn snapshot_loop(uri: String, collection: String, observed: Observed, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        let Ok(client) = Client::with_uri_str(&uri).await else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        let collection = client
            .database(DATABASE)
            .collection::<Document>(&collection);
        while !stop.load(Ordering::Acquire) {
            let pipeline = [doc! { "$group": { "_id": "$id", "n": { "$sum": 1 } } }];
            let Ok(cursor) = collection.aggregate(pipeline).await else {
                break;
            };
            let Ok(groups) = cursor.try_collect::<Vec<Document>>().await else {
                break;
            };
            let counts: BTreeMap<u64, u32> = groups
                .iter()
                .map(|group| {
                    (
                        unsigned(group.get("_id")),
                        u32::try_from(unsigned(group.get("n"))).expect("an id's count fits u32"),
                    )
                })
                .collect();
            observed.replace(counts);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

fn unsigned(value: Option<&Bson>) -> u64 {
    let signed = match value {
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Int32(value)) => i64::from(*value),
        other => panic!("expected an integer in the output collection, found {other:?}"),
    };
    u64::try_from(signed).expect("output integers are non-negative")
}
