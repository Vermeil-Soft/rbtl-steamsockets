
pub type SeqId = u32;

/// Is the `test` value a previous/past value compared to `curr`, e.g. if test is lower than curr
///
/// Better than just a comparison because if takes into account wrapping
pub (crate) fn is_seq_id_past(curr: SeqId, test: SeqId) -> bool {
    // we can't use simple comparisons here just in case we wrap around SeqId::max
    // a simple alternative is just taking the diff between ok_seq_id and seq_id, 2 cases:
    // * ok_seq_id = 4_000_000_000; seq_id = 1 => diff = 4billion
    // * ok_seq_id = 0; seq_id = 1 => diff = -1, but wrapped ~4billion
    // in both cases 4billion would be above u32::MAX / 2, so the check would not pass
    let diff = curr.wrapping_sub(test);
    diff < (SeqId::MAX / 2)
}

#[test]
#[cfg(test)]
fn is_seq_id_past_test() {
    assert!(is_seq_id_past(5, u32::MAX));
    assert!(!is_seq_id_past(u32::MAX, 5));
    assert!(is_seq_id_past(10, 5));
    assert!(!is_seq_id_past(5, 10));
}