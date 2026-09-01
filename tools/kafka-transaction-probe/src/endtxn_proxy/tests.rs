use super::*;

const CLIENT: &str = "ldb-kafka-ambiguity-marker-applied-a";
const TX_ID: &str = "ldb.tx.v1.c49ace6d02eb21ec7a2dc4424d8c3b9680fc3cd828cd754fec079b800a37411a";

fn request(correlation: i32, version: i16, committed: u8) -> Vec<u8> {
    request_for(CLIENT, TX_ID, correlation, version, committed)
}

fn request_for(
    client_id: &str,
    transactional_id: &str,
    correlation: i32,
    version: i16,
    committed: u8,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&END_TXN_API_KEY.to_be_bytes());
    body.extend_from_slice(&version.to_be_bytes());
    body.extend_from_slice(&correlation.to_be_bytes());
    body.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    body.extend_from_slice(&(transactional_id.len() as i16).to_be_bytes());
    body.extend_from_slice(transactional_id.as_bytes());
    body.extend_from_slice(&42_i64.to_be_bytes());
    body.extend_from_slice(&7_i16.to_be_bytes());
    body.push(committed);
    let mut frame = Vec::new();
    frame.extend_from_slice(&(body.len() as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn response(correlation: i32, error: i16) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&10_i32.to_be_bytes());
    frame.extend_from_slice(&correlation.to_be_bytes());
    frame.extend_from_slice(&0_i32.to_be_bytes());
    frame.extend_from_slice(&error.to_be_bytes());
    frame
}

fn target(outcome: FaultOutcome) -> Arc<Shared> {
    Arc::new(Shared::new(TargetSpec {
        client_id: CLIENT.to_owned(),
        transactional_id: TX_ID.to_owned(),
        outcome,
    }))
}

#[test]
fn exact_endtxn_v1_request_and_response_parse() {
    let frame = request(123, 1, 1);
    let header = parse_request_header(&frame).unwrap();
    let parsed = parse_endtxn_v1(9, &frame, &header).unwrap();
    assert_eq!(parsed.connection_id, 9);
    assert_eq!(parsed.api_version, 1);
    assert_eq!(parsed.correlation_id, 123);
    assert_eq!(parsed.client_id, CLIENT);
    assert_eq!(parsed.transactional_id, TX_ID);
    assert_eq!(parsed.producer_id, 42);
    assert_eq!(parsed.producer_epoch, 7);
    assert!(parsed.committed);

    let parsed_response = parse_endtxn_v1_response(9, &response(123, 0)).unwrap();
    assert_eq!(parsed_response.connection_id, 9);
    assert_eq!(parsed_response.correlation_id, 123);
    assert_eq!(parsed_response.throttle_ms, 0);
    assert_eq!(parsed_response.error_code, 0);
}

#[test]
fn parsers_reject_version_commit_length_and_response_drift() {
    let version = request(1, 2, 1);
    let header = parse_request_header(&version).unwrap();
    assert!(parse_endtxn_v1(1, &version, &header).is_err());

    let commit = request(1, 1, 2);
    let header = parse_request_header(&commit).unwrap();
    assert!(parse_endtxn_v1(1, &commit, &header).is_err());

    let mut trailing = request(1, 1, 1);
    trailing.push(0);
    let length = i32::try_from(trailing.len() - 4).unwrap();
    trailing[..4].copy_from_slice(&length.to_be_bytes());
    let header = parse_request_header(&trailing).unwrap();
    assert!(parse_endtxn_v1(1, &trailing, &header).is_err());

    let mut bad_response = response(1, 0);
    bad_response[0..4].copy_from_slice(&9_i32.to_be_bytes());
    assert!(parse_endtxn_v1_response(1, &bad_response).is_err());
}

#[test]
fn applied_and_unapplied_state_require_exact_byte_dispositions() {
    let applied = target(FaultOutcome::AppliedResponseLost);
    applied.arm().unwrap();
    let frame = request(11, 1, 1);
    assert!(matches!(
        applied.classify_request(3, &frame).unwrap(),
        RequestAction::ForwardTarget { correlation_id: 11 }
    ));
    applied.target_request_written(frame.len()).unwrap();
    applied.capture_target_response(3, response(11, 0)).unwrap();
    assert!(applied
        .state
        .lock()
        .unwrap()
        .actuated(FaultOutcome::AppliedResponseLost));

    let unapplied = target(FaultOutcome::UnappliedRequestHeld);
    unapplied.arm().unwrap();
    assert!(matches!(
        unapplied.classify_request(4, &request(12, 1, 1)).unwrap(),
        RequestAction::SuppressTarget
    ));
    let state = unapplied.state.lock().unwrap();
    assert!(state.actuated(FaultOutcome::UnappliedRequestHeld));
    assert_eq!(state.request_upstream_bytes, 0);
    assert!(state.response.is_none());
}

#[test]
fn retry_and_wrong_target_fail_closed() {
    let shared = target(FaultOutcome::UnappliedRequestHeld);
    shared.arm().unwrap();
    let frame = request(13, 1, 1);
    shared.classify_request(1, &frame).unwrap();
    assert!(shared.classify_request(1, &frame).is_err());

    let wrong = target(FaultOutcome::UnappliedRequestHeld);
    wrong.arm().unwrap();
    let abort = request(14, 1, 0);
    assert!(wrong.classify_request(1, &abort).is_err());
}

#[test]
fn target_identity_version_response_and_correlation_drift_fail_closed() {
    let wrong_client = target(FaultOutcome::UnappliedRequestHeld);
    wrong_client.arm().unwrap();
    assert!(matches!(
        wrong_client
            .classify_request(1, &request_for("other-client", TX_ID, 16, 1, 1))
            .unwrap(),
        RequestAction::ForwardNormal { correlation_id: 16 }
    ));
    assert!(wrong_client.state.lock().unwrap().request.is_none());

    let wrong_tx = target(FaultOutcome::UnappliedRequestHeld);
    wrong_tx.arm().unwrap();
    assert!(wrong_tx
        .classify_request(1, &request_for(CLIENT, &"f".repeat(74), 17, 1, 1))
        .is_err());

    let wrong_version = target(FaultOutcome::UnappliedRequestHeld);
    wrong_version.arm().unwrap();
    assert!(wrong_version
        .classify_request(1, &request(18, 3, 1))
        .is_err());

    let mismatch = target(FaultOutcome::AppliedResponseLost);
    mismatch.arm().unwrap();
    let frame = request(19, 1, 1);
    mismatch.classify_request(1, &frame).unwrap();
    mismatch.target_request_written(frame.len()).unwrap();
    assert!(mismatch
        .capture_target_response(1, response(20, 0))
        .is_err());

    let rejected = target(FaultOutcome::AppliedResponseLost);
    rejected.arm().unwrap();
    let frame = request(20, 1, 1);
    rejected.classify_request(1, &frame).unwrap();
    rejected.target_request_written(frame.len()).unwrap();
    assert!(rejected
        .capture_target_response(1, response(20, 47))
        .is_err());

    let partial_write = target(FaultOutcome::AppliedResponseLost);
    partial_write.arm().unwrap();
    let frame = request(21, 1, 1);
    partial_write.classify_request(1, &frame).unwrap();
    assert!(partial_write
        .target_request_written(frame.len() - 1)
        .is_err());

    let pending = ProbeMutex::new(BTreeMap::new());
    insert_pending(&pending, 22, PendingKind::Normal).unwrap();
    assert!(insert_pending(&pending, 22, PendingKind::Target).is_err());
}

#[test]
fn frame_bounds_reject_negative_zero_partial_and_trailing_lengths() {
    for frame in [
        (-1_i32).to_be_bytes().to_vec(),
        0_i32.to_be_bytes().to_vec(),
        vec![0, 0, 0, 10, 0],
        vec![0, 0, 0, 1, 0, 0],
    ] {
        assert!(require_wire_length(&frame).is_err());
    }
}

#[test]
fn target_client_traffic_after_finish_is_fatal() {
    let shared = target(FaultOutcome::UnappliedRequestHeld);
    shared.arm().unwrap();
    shared.classify_request(1, &request(21, 1, 1)).unwrap();
    shared.close_connection(1);
    shared.finish_target().unwrap();

    let mut late = request(22, 1, 1);
    late[4..6].copy_from_slice(&3_i16.to_be_bytes());
    assert!(shared.classify_request(2, &late).is_err());
    assert!(shared.state.lock().unwrap().fatal.is_some());
}

#[test]
fn framed_reader_handles_byte_fragmentation_and_rejects_oversize() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let frame = request(15, 1, 1);
    let writer_frame = frame.clone();
    let writer = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        for byte in writer_frame {
            stream.write_all(&[byte]).unwrap();
        }
    });
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
    let global = AtomicBool::new(false);
    let local = AtomicBool::new(false);
    assert_eq!(
        read_frame(&mut stream, &global, &local).unwrap(),
        Some(frame)
    );
    writer.join().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let writer = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .write_all(&((MAX_FRAME_PAYLOAD_BYTES as i32) + 1).to_be_bytes())
            .unwrap();
    });
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
    assert!(read_frame(&mut stream, &global, &local).is_err());
    writer.join().unwrap();
}

#[test]
fn framed_reader_distinguishes_teardown_cancellation_from_partial_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(&10_i32.to_be_bytes()).unwrap();
        stream.write_all(&[0]).unwrap();
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
    ready_rx.recv().unwrap();
    let global = Arc::new(AtomicBool::new(false));
    let local = Arc::new(AtomicBool::new(false));
    let stop = Arc::clone(&local);
    let stopper = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        stop.store(true, Ordering::SeqCst);
    });
    let result = read_frame(&mut stream, &global, &local);
    stopper.join().unwrap();
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(result.unwrap(), None);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let writer = thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(&10_i32.to_be_bytes()).unwrap();
        stream.write_all(&[0]).unwrap();
    });
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(SOCKET_POLL)).unwrap();
    let global = AtomicBool::new(false);
    let local = AtomicBool::new(false);
    assert!(read_frame(&mut stream, &global, &local).is_err());
    writer.join().unwrap();
}
