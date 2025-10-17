use super::GasStatisticsInner;
use std::collections::VecDeque;

/// Check that we compute the median correctly
#[test]
fn median() {
    // sorted: 4 4 6 7 8
    assert_eq!(GasStatisticsInner::new(5, 5, [6, 4, 7, 8, 4]).median(), 6);
    // sorted: 4 4 8 10
    assert_eq!(GasStatisticsInner::new(4, 4, [8, 4, 4, 10]).median(), 8);
}

/// Check that we properly manage the block base fee queue
#[test]
fn samples_queue() {
    let mut stats = GasStatisticsInner::new(5, 5, [6, 4, 7, 8, 4, 5]);

    assert_eq!(stats.samples, VecDeque::from([4, 7, 8, 4, 5]));

    stats.add_samples([18, 18, 18]);

    assert_eq!(stats.samples, VecDeque::from([4, 5, 18, 18, 18]));
}
