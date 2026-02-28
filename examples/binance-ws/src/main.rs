#![allow(clippy::disallowed_types)]

use laminar_db::{ExecuteResult, LaminarDB};
use laminar_derive::FromRow;

#[derive(Debug, FromRow)]
#[allow(dead_code)]
struct Alert {
    symbol: String,
    vwap: f64,
    avg_price: f64,
    total_qty: f64,
    trades: i64,
    signal: String,
}

#[derive(Debug, FromRow)]
#[allow(dead_code)]
struct Momentum {
    symbol: String,
    vwap_1m: f64,
    price_range: f64,
    volume_1m: f64,
    trades_1m: i64,
}

async fn sql(db: &LaminarDB, query: &str) -> Result<(), Box<dyn std::error::Error>> {
    match db.execute(query).await {
        Ok(result) => {
            if let ExecuteResult::Ddl(info) = &result {
                println!("[OK] {} {}", info.statement_type, info.object_name);
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("[SQL ERROR] {e}");
            eprintln!("  Query: {}", query.lines().next().unwrap_or(query));
            Err(e.into())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = LaminarDB::open()?;

    sql(
        &db,
        "CREATE SOURCE trades (
            s VARCHAR,
            p DOUBLE,
            q DOUBLE,
            \"T\" BIGINT,
            WATERMARK FOR \"T\" AS \"T\" - INTERVAL '0' SECOND
        ) FROM WEBSOCKET (
            url = 'wss://stream.binance.com:9443/ws/btcusdt@trade',
            format = 'json'
        )",
    )
    .await?;

    sql(
        &db,
        "CREATE STREAM vwap_10s AS
        SELECT s AS symbol,
               SUM(p * q) / SUM(q) AS vwap,
               AVG(p)              AS avg_price,
               MIN(p)              AS min_price,
               MAX(p)              AS max_price,
               SUM(q)              AS total_qty,
               COUNT(*)            AS trades
        FROM trades
        GROUP BY s, TUMBLE(\"T\", INTERVAL '10' SECOND)
        EMIT ON WINDOW CLOSE",
    )
    .await?;

    sql(
        &db,
        "CREATE STREAM momentum_1m AS
        SELECT s AS symbol,
               SUM(p * q) / SUM(q) AS vwap_1m,
               MAX(p) - MIN(p)     AS price_range,
               SUM(q)              AS volume_1m,
               COUNT(*)            AS trades_1m
        FROM trades
        GROUP BY s, TUMBLE(\"T\", INTERVAL '1' MINUTE)
        EMIT ON WINDOW CLOSE",
    )
    .await?;

    sql(
        &db,
        "CREATE STREAM alerts AS
        SELECT symbol, vwap, avg_price, total_qty, trades,
               CASE WHEN avg_price > vwap * 1.002 THEN 'SELL'
                    WHEN avg_price < vwap * 0.998 THEN 'BUY'
                    ELSE 'HOLD' END AS signal
        FROM vwap_10s",
    )
    .await?;

    db.start().await?;

    let alerts = db.subscribe::<Alert>("alerts")?;
    let momentum = db.subscribe::<Momentum>("momentum_1m")?;

    println!("Listening for BTC/USDT trades...\n");

    let t1 = std::thread::spawn(move || {
        while let Ok(rows) = alerts.recv() {
            for r in &rows {
                println!(
                    "{:4} {} | vwap={:.2} avg={:.2} qty={:.4} trades={}",
                    r.signal, r.symbol, r.vwap, r.avg_price, r.total_qty, r.trades
                );
            }
        }
    });

    let t2 = std::thread::spawn(move || {
        while let Ok(rows) = momentum.recv() {
            for r in &rows {
                println!(
                    "[1m]  {} | vwap={:.2} range={:.2} vol={:.4} trades={}",
                    r.symbol, r.vwap_1m, r.price_range, r.volume_1m, r.trades_1m
                );
            }
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();

    Ok(())
}
