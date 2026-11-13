//! PostgreSQL append mode: every node copies its rows into one table without a key, so a replay
//! adds rows instead of overwriting them. The oracle counts the rows per id in snapshots of the
//! table.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_postgres::{Client, NoTls};

use super::{block_on, setting, Destination, Observed, Oracle};

const PASSWORD: &str = "LAMINAR_SOAK_POSTGRES_PASSWORD";

pub(super) struct Postgres {
    host: String,
    port: String,
    database: String,
    username: String,
}

impl Postgres {
    pub(super) fn from_environment() -> Self {
        setting(PASSWORD, None);
        Self {
            host: setting("LAMINAR_SOAK_POSTGRES_HOST", Some("127.0.0.1")),
            port: setting("LAMINAR_SOAK_POSTGRES_PORT", Some("15434")),
            database: setting("LAMINAR_SOAK_POSTGRES_DATABASE", Some("laminar")),
            username: setting("LAMINAR_SOAK_POSTGRES_USERNAME", Some("laminar")),
        }
    }

    fn connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.host,
            self.port,
            self.database,
            self.username,
            setting(PASSWORD, None)
        )
    }
}

impl Destination for Postgres {
    fn connector(&self) -> &'static str {
        "postgres-sink"
    }

    fn written_metric(&self) -> &'static str {
        "laminardb_postgres_sink_records_written_total"
    }

    fn format(&self) -> Option<&'static str> {
        None
    }

    fn name(&self, run_id: &str) -> String {
        format!("soak_sink_{}", run_id.replace('-', "_"))
    }

    fn sink_properties(&self, table: &str) -> String {
        format!(
            r#"hostname = "{host}"
port = "{port}"
database = "{database}"
username = "{username}"
password = "$${{{PASSWORD}}}"
"ssl.mode" = "disable"
"table.name" = "{table}"
"write.mode" = "append"
"#,
            host = self.host,
            port = self.port,
            database = self.database,
            username = self.username,
        )
    }

    // Every node opens the sink concurrently, so the test creates the table rather than racing
    // three `auto.create.table` statements.
    fn create(&self, table: &str) {
        let connection = self.connection_string();
        block_on(async move {
            connect(&connection)
                .await
                .expect("connect to PostgreSQL")
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id BIGINT NOT NULL, payload TEXT NOT NULL)"
                ))
                .await
                .expect("create the output table");
        });
    }

    fn delete(&self, table: &str) {
        let connection = self.connection_string();
        block_on(async move {
            if let Ok(client) = connect(&connection).await {
                let _ = client
                    .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
                    .await;
            }
        });
    }

    fn spawn_oracle(&self, table: &str) -> Oracle {
        let (connection, table) = (self.connection_string(), table.to_owned());
        Oracle::spawn(move |observed, stop| {
            block_on(snapshot_loop(connection, table, observed, stop));
        })
    }
}

async fn connect(connection: &str) -> Result<Client, tokio_postgres::Error> {
    let (client, driver) = tokio_postgres::connect(connection, NoTls).await?;
    tokio::spawn(async move {
        let _ = driver.await;
    });
    Ok(client)
}

async fn snapshot_loop(
    connection: String,
    table: String,
    observed: Observed,
    stop: Arc<AtomicBool>,
) {
    let query = format!("SELECT id, count(*) FROM {table} GROUP BY id");
    while !stop.load(Ordering::Acquire) {
        let Ok(client) = connect(&connection).await else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        };
        while !stop.load(Ordering::Acquire) {
            let Ok(rows) = client.query(query.as_str(), &[]).await else {
                break;
            };
            let counts: BTreeMap<u64, u32> = rows
                .iter()
                .map(|row| {
                    let id: i64 = row.get(0);
                    let count: i64 = row.get(1);
                    (
                        u64::try_from(id).expect("output ids are non-negative"),
                        u32::try_from(count).expect("an id's row count fits u32"),
                    )
                })
                .collect();
            observed.replace(counts);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
