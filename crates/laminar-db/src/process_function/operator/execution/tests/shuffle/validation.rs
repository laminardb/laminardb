use super::*;

#[tokio::test]
async fn a_pre_admission_send_failure_remains_runnable_and_retries_the_same_cut() {
    let pair = Pair::new().await;
    let closed = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let unavailable = closed.local_addr().unwrap();
    drop(closed);
    pair.nodes[0].scope.sender.register_peer(8, unavailable);
    let mut operator = pair.operator(0);
    let local_key = key_for(0);
    let remote_key = key_for(1);
    operator
        .process_with_frontiers(
            &[vec![input_batch(&[
                (&local_key, 7, 100_000),
                (&remote_key, 2, 100_000),
            ])]],
            &frontier(100),
        )
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, async {
        while !operator.deferred_work_is_runnable() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(operator
        .process_with_frontiers(&[], &frontier(100))
        .await
        .unwrap()
        .is_empty());
    assert!(operator.deferred_work_is_runnable());
    assert!(!operator.wants_input());
    assert!(operator.checkpoint().is_err());
    assert_eq!(operator.next_activation_id, 0);
    pair.nodes[0]
        .scope
        .sender
        .register_peer(8, pair.nodes[1].scope.receiver.local_addr());
    let output = drain_local(&mut operator, 100).await;
    assert_eq!(
        activity_rows(&output),
        [(local_key, "running".into(), 7, false, 100_000)]
    );
    assert_eq!(operator.next_activation_id, 1);
    let received = tokio::time::timeout(DEADLINE, pair.nodes[1].scope.receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ShuffleMessage::Data { batch, .. } = received.message() else {
        panic!("retained process data must precede its frontier");
    };
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(received.checkpoint_sequence(), 0);
    let received = tokio::time::timeout(DEADLINE, pair.nodes[1].scope.receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.checkpoint_sequence(), 1);
    assert_eq!(received.message(), &peer_frontier(Some(100), false));
    assert!(pair.nodes[1]
        .scope
        .receiver
        .drain_checkpointed_staged()
        .is_empty());
}

#[tokio::test]
async fn invalid_peer_scope_and_canonical_routes_never_apply_state() {
    let pair = Pair::new().await;
    let key = key_for(0);
    let good = input_batch(&[(&key, 3, 100_000)]);
    for (stage, peer, version, recovery, routes) in [
        ("other", 8, 7, 3, vec![0]),
        ("activity", 7, 7, 3, vec![0]),
        ("activity", 9, 7, 3, vec![0]),
        ("activity", 8, 6, 3, vec![0]),
        ("activity", 8, 7, 2, vec![0]),
        ("activity", 8, 7, 3, vec![]),
        ("activity", 8, 7, 3, vec![0, 0]),
        ("activity", 8, 7, 3, vec![1]),
        ("activity", 8, 7, 3, vec![2]),
        ("activity", 8, 7, 3, vec![0, 2]),
        ("activity", 8, 7, 3, vec![4]),
    ] {
        let mut operator = pair.operator(0);
        let before = state_image(&operator);
        let metadata = operator.checkpoint().unwrap().unwrap().data;
        let batch = RetainedBatch::restored_channel(
            good.clone(),
            peer,
            version,
            recovery,
            Arc::from(routes),
        );
        assert!(
            operator
                .stage_checkpointed_shuffle(stage, batch, 100)
                .is_err(),
            "{stage} {peer} {version} {recovery}"
        );
        assert_eq!(state_image(&operator), before);
        assert_eq!(operator.checkpoint().unwrap().unwrap().data, metadata);
    }
    let mut operator = pair.operator(0);
    assert!(operator
        .stage_checkpointed_shuffle("activity", RetainedBatch::local(good.clone()), 100)
        .is_err());
    assert!(operator
        .stage_checkpointed_shuffle(
            "activity",
            RetainedBatch::restored_channel(good.slice(0, 0), 8, 7, 3, Arc::from([0])),
            100
        )
        .is_err());
    let invalid_schema = good.project(&[0, 2]).unwrap();
    assert!(operator
        .stage_checkpointed_shuffle(
            "activity",
            RetainedBatch::restored_channel(invalid_schema.clone(), 8, 7, 3, Arc::from([0])),
            100,
        )
        .unwrap_err()
        .requires_pipeline_halt());
    assert!(operator
        .process_with_frontiers(&[vec![invalid_schema]], &frontier(100))
        .await
        .unwrap_err()
        .requires_pipeline_halt());
    assert_eq!(operator.next_activation_id, 0);
}

#[tokio::test]
async fn peer_frontier_validation_preserves_accepted_order() {
    let pair = Pair::new().await;
    for (stage, peer, version, recovery, watermark) in [
        ("other", 8, 7, 3, Some(100)),
        ("activity", 7, 7, 3, Some(100)),
        ("activity", 9, 7, 3, Some(100)),
        ("activity", 8, 6, 3, Some(100)),
        ("activity", 8, 7, 2, Some(100)),
        ("activity", 8, 7, 3, Some(i64::MIN)),
    ] {
        let mut operator = pair.operator(0);
        let before = operator.checkpoint().unwrap().unwrap().data;
        assert!(operator
            .stage_checkpointed_shuffle_frontier(
                stage,
                peer,
                InputFrontier {
                    watermark,
                    idle: false
                },
                version,
                recovery
            )
            .is_err());
        assert_eq!(operator.checkpoint().unwrap().unwrap().data, before);
    }
    let mut operator = pair.operator(0);
    operator
        .stage_checkpointed_shuffle_frontier("activity", 8, frontier(200)[0], 7, 3)
        .unwrap();
    for watermark in [None, Some(199), Some(i64::MIN)] {
        assert!(operator
            .stage_checkpointed_shuffle_frontier(
                "activity",
                8,
                InputFrontier {
                    watermark,
                    idle: false
                },
                7,
                3
            )
            .is_err());
    }
    let key = key_for(0);
    assert!(operator
        .stage_checkpointed_shuffle(
            "activity",
            RetainedBatch::restored_channel(
                input_batch(&[(&key, 3, 199_999)]),
                8,
                7,
                3,
                Arc::from([0])
            ),
            100
        )
        .is_err());
    assert_eq!(operator.next_activation_id, 0);
    assert_eq!(operator.watermark_us, i64::MIN);
}

#[tokio::test]
async fn shuffle_queues_and_routing_obey_input_and_state_caps() {
    let pair = Pair::new().await;
    let key = key_for(0);
    let batch = input_batch(&[(&key, 1, 100_000)]);
    let retained = || RetainedBatch::restored_channel(batch.clone(), 8, 7, 3, Arc::from([0]));
    let mut binding = descriptor();
    binding.limits.max_input_rows = 1;
    let mut operator = ProcessFunctionOperator::new(binding, Arc::new(AccountActivity), 4).unwrap();
    pair.nodes[0].bind_operator(&mut operator);
    operator
        .stage_checkpointed_shuffle("activity", retained(), 100)
        .unwrap();
    let before = state_image(&operator);
    assert!(operator
        .stage_checkpointed_shuffle("activity", retained(), 100)
        .is_err());
    assert_eq!(state_image(&operator), before);
    operator
        .stage_checkpointed_shuffle_frontier("activity", 8, frontier(100)[0], 7, 3)
        .unwrap();
    assert_eq!(
        activity_rows(&drain_local(&mut operator, 100).await)[0].2,
        1
    );

    let mut operator = pair.operator(0);
    let limit = operator.managed_state_accounting().unwrap().live;
    operator.set_managed_state_budget(limit);
    assert!(operator
        .stage_checkpointed_shuffle("activity", retained(), 100)
        .is_err());
    assert_eq!(operator.next_activation_id, 0);
    let error = operator
        .process_with_frontiers(&[vec![batch]], &frontier(100))
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::BackpressureFail(_)), "{error}");
    assert_eq!(operator.next_activation_id, 0);
    assert_eq!(operator.key_count, 0);

    let mut operator = pair.operator(0);
    for _ in 0..(descriptor().limits.max_input_rows * 2 + 2) {
        operator
            .stage_checkpointed_shuffle_frontier("activity", 8, InputFrontier::default(), 7, 3)
            .unwrap();
    }
    assert!(operator
        .stage_checkpointed_shuffle_frontier("activity", 8, InputFrontier::default(), 7, 3)
        .is_err());
    assert_eq!(operator.next_activation_id, 0);
}

#[tokio::test]
async fn retained_input_is_fenced_before_application_and_during_an_outbound_cut() {
    let pair = Pair::new().await;
    let mut operator = pair.operator(0);
    let key = key_for(0);
    operator
        .stage_checkpointed_shuffle(
            "activity",
            RetainedBatch::restored_channel(
                input_batch(&[(&key, 7, 100_000)]),
                8,
                7,
                3,
                Arc::from([0]),
            ),
            100,
        )
        .unwrap();
    let before = state_image(&operator);
    pair.nodes[0]
        .controller
        .process_lease_deadline()
        .unwrap()
        .fence();
    assert!(operator
        .process_with_frontiers(&[], &frontier(100))
        .await
        .unwrap_err()
        .requires_pipeline_recovery());
    assert_eq!(state_image(&operator), before);

    let pair = Pair::new().await;
    let mut operator = pair.operator(0);
    let remote_key = key_for(1);
    operator
        .process_with_frontiers(
            &[vec![input_batch(&[
                (&key, 7, 100_000),
                (&remote_key, 2, 100_000),
            ])]],
            &frontier(100),
        )
        .await
        .unwrap();
    assert_eq!(
        operator.key_count, 0,
        "local state waits for the outbound admission outcome"
    );
    assert!(operator.checkpoint().is_err());
    pair.nodes[0].scope.sender.set_recovery_gen(4);
    pair.nodes[0].scope.receiver.set_recovery_gen(4);
    assert!(operator
        .process_with_frontiers(&[], &frontier(100))
        .await
        .unwrap_err()
        .requires_pipeline_recovery());
    assert_eq!(operator.key_count, 0);
}
