#!/usr/bin/env python3
"""
Reads payment_summary from Lakekeeper via PyIceberg.

    pip install "pyiceberg[s3fs,pyarrow]"
    python query.py

Run after ~90 seconds of `gen.py` so at least one tumbling minute has
closed and the sink has committed.

Prerequisite: `rustfs` must resolve to `127.0.0.1` from the host so
PyIceberg can fetch the manifest paths Lakekeeper bakes into table
metadata. See README.
"""

from datetime import datetime, timezone

from pyiceberg.catalog.rest import RestCatalog


catalog = RestCatalog(
    name="demo",
    **{
        "uri":       "http://localhost:8181/catalog",
        "warehouse": "demo",
        # Lakekeeper runs unauthenticated in this demo profile.
        "auth":      {"type": "noop"},
        # Storage credentials for the data files. Lakekeeper points at
        # http://rustfs:9000 (in-cluster); on the host `rustfs` resolves
        # to 127.0.0.1 via the README hosts entry.
        "s3.endpoint":          "http://rustfs:9000",
        "s3.access-key-id":     "rustfsadmin",
        "s3.secret-access-key": "rustfsadmin",
        "s3.region":            "us-east-1",
    },
)

table = catalog.load_table("finance.payment_summary")

# scan().to_pandas() applies snapshot resolution and schema evolution.
df = table.scan().to_pandas()

# window_start / window_end are Int64 epoch ms; render as timestamps.
for col in ("window_start", "window_end"):
    df[col] = df[col].apply(
        lambda ms: datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")
    )
df["total_usd"] = df["total_usd"].round(2)
df["avg_usd"]   = df["avg_usd"].round(2)

df = df.sort_values(["window_start", "total_usd"], ascending=[False, False]).head(40)

print("\n--- payment summary by region and method ---------------------")
print(df.to_string(index=False))
print(f"\n{len(df)} rows")
