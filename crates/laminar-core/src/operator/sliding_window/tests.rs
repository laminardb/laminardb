use super::*;

#[test]
fn test_sliding_assigner_basic() {
    let assigner = SlidingWindowAssigner::from_millis(10_000, 5_000);
    let windows = assigner.assign_windows(7_000);
    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0].start, 0);
    assert_eq!(windows[0].end, 10_000);
    assert_eq!(windows[1].start, 5_000);
    assert_eq!(windows[1].end, 15_000);
}

#[test]
fn test_sliding_assigner_windows_per_event() {
    let assigner = SlidingWindowAssigner::from_millis(10_000, 5_000);
    assert_eq!(assigner.windows_per_event(), 2);

    let assigner = SlidingWindowAssigner::from_millis(15_000, 5_000);
    assert_eq!(assigner.windows_per_event(), 3);
}

#[test]
fn test_sliding_iterator_boundaries() {
    let assigner = SlidingWindowAssigner::from_millis(10, 6).with_offset_ms(3);
    let cases = [
        (-3, vec![WindowId::new(-9, 1), WindowId::new(-3, 7)]),
        (6, vec![WindowId::new(-3, 7), WindowId::new(3, 13)]),
        (7, vec![WindowId::new(3, 13)]),
        (9, vec![WindowId::new(3, 13), WindowId::new(9, 19)]),
    ];

    for (timestamp, expected) in cases {
        let lazy = assigner
            .try_iter_windows(timestamp)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(lazy, expected);
        assert_eq!(
            lazy.as_slice(),
            assigner.assign_windows(timestamp).as_slice()
        );
    }
}

#[test]
fn test_sliding_extreme_boundaries_are_checked() {
    let wide = SlidingWindowAssigner::from_millis(1, 1).with_offset_ms(i64::MAX);
    assert_eq!(
        wide.try_iter_windows(i64::MIN).unwrap().collect::<Vec<_>>(),
        [WindowId::new(i64::MIN, i64::MIN + 1)]
    );

    let maximum = SlidingWindowAssigner::from_millis(i64::MAX, i64::MAX);
    assert_eq!(maximum.windows_per_event(), 1);
    assert_eq!(
        maximum.try_iter_windows(0).unwrap().collect::<Vec<_>>(),
        [WindowId::new(0, i64::MAX)]
    );

    let assigner = SlidingWindowAssigner::from_millis(2, 1);
    assert_eq!(
        assigner
            .try_iter_windows(i64::MIN + 1)
            .unwrap()
            .collect::<Vec<_>>(),
        [
            WindowId::new(i64::MIN, i64::MIN + 2),
            WindowId::new(i64::MIN + 1, i64::MIN + 3),
        ]
    );
    assert!(assigner.try_iter_windows(i64::MIN).is_err());
    assert!(assigner.try_iter_windows(i64::MAX - 1).is_err());
    assert!(SlidingWindowAssigner::from_millis(10, 5)
        .with_offset_ms(i64::MAX)
        .try_iter_windows(i64::MIN)
        .is_err());
}
