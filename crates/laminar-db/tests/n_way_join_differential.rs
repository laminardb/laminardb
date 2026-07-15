use laminar_db::LaminarDB;

#[tokio::test]
async fn bounded_multi_way_join_requires_named_two_way_stages() {
    let db = LaminarDB::open().expect("open database");
    db.execute("CREATE SOURCE a (id BIGINT, ts BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE b (id BIGINT, ts BIGINT)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE c (id BIGINT, ts BIGINT)")
        .await
        .unwrap();

    let error = db
        .execute(
            "CREATE STREAM joined AS SELECT a.id FROM a \
             JOIN b ON a.id = b.id \
               AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '5' SECOND \
             JOIN c ON b.id = c.id \
               AND c.ts BETWEEN b.ts AND b.ts + INTERVAL '5' SECOND",
        )
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("explicitly named two-way stages"),
        "unexpected error: {error}"
    );
}
