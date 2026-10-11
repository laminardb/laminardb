"""Spark/Delta oracle for the LaminarDB Delta interop test.

Runs inside the delta-spark compose service. Tables live under /lake, which the host sees as
target/delta-spark-interop. Every command prints one JSON document on its last stdout line.
"""

import json
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def spark():
    builder = (
        SparkSession.builder.appName("laminar-delta-oracle")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def rows(session, sql):
    return sorted(
        ([row[0], row[1], int(row[2])] for row in session.sql(sql).collect()),
        key=lambda row: row[0],
    )


def write_changes(session, path):
    """A CDF table with deletion vectors, changed by every Spark DML kind and compacted."""
    table = f"delta.`{path}`"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT NOT NULL, label STRING NOT NULL, qty BIGINT NOT NULL) "
        "USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true, "
        "delta.enableDeletionVectors = true)"
    )
    session.sql(
        f"INSERT INTO {table} SELECT id, IF(id % 3 = 0, 'OPEN', 'CLOSED'), id "
        "FROM range(1, 201)"
    )
    session.sql(f"INSERT INTO {table} SELECT id, 'LATE', id FROM range(201, 251)")
    session.sql(f"UPDATE {table} SET qty = qty * 10 WHERE id % 7 = 0")
    session.sql(f"DELETE FROM {table} WHERE id BETWEEN 10 AND 40")
    session.sql(
        f"MERGE INTO {table} t USING (SELECT id, 'MERGED' AS label, id + 1000 AS qty "
        "FROM range(240, 271)) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET label = s.label, qty = s.qty "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    session.sql(f"UPDATE {table} SET id = id + 5000 WHERE id = 3")
    session.sql(f"OPTIMIZE {table}")
    version = session.sql(f"DESCRIBE HISTORY {table}").agg({"version": "max"}).collect()[0][0]
    protocol = session.sql(f"DESCRIBE DETAIL {table}").select(
        "minReaderVersion", "minWriterVersion", "tableFeatures"
    ).collect()[0]
    return {
        "version": int(version),
        "rows": rows(session, f"SELECT id, label, qty FROM {table}"),
        "protocol": [protocol[0], protocol[1], sorted(protocol[2] or [])],
    }


def write_column_mapped(session, path):
    """A name-mapped table whose physical column names differ from its logical ones."""
    table = f"delta.`{path}`"
    session.sql(
        f"CREATE TABLE {table} (id BIGINT NOT NULL, name STRING NOT NULL, qty BIGINT NOT NULL) "
        "USING DELTA TBLPROPERTIES (delta.columnMapping.mode = 'name', "
        "delta.enableChangeDataFeed = true)"
    )
    session.sql(f"INSERT INTO {table} SELECT id, CONCAT('n', id), id FROM range(1, 11)")
    session.sql(f"ALTER TABLE {table} RENAME COLUMN name TO label")
    session.sql(f"DELETE FROM {table} WHERE id > 8")
    return read(session, path)


def read(session, path):
    return {"rows": rows(session, f"SELECT id, label, qty FROM delta.`{path}`")}


def main():
    command, path = sys.argv[1], sys.argv[2]
    session = spark()
    commands = {
        "write_changes": write_changes,
        "write_column_mapped": write_column_mapped,
        "read": read,
    }
    result = commands[command](session, path)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
