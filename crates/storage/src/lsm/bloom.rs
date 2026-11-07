//! A small Bloom filter (double hashing over one 64-bit hash).
//!
//! Serialized form: `[k: u8][bit array...]`. An empty/short slice means "no
//! filter": `may_contain` then answers `true`, never a false negative.

/// 64-bit FNV-1a followed by a splitmix64 finalizer (good avalanche, no deps).
pub(crate) fn hash64(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// Build a filter for the given key hashes with `bits_per_key` bits each.
pub(crate) fn build(hashes: &[u64], bits_per_key: usize) -> Vec<u8> {
    if bits_per_key == 0 || hashes.is_empty() {
        return Vec::new();
    }
    // k = ln2 * bits_per_key, clamped.
    let k = ((bits_per_key as f64) * 0.69).round().clamp(1.0, 30.0) as u8;
    let nbits = (hashes.len() * bits_per_key).max(64);
    let nbytes = nbits.div_ceil(8);
    let nbits = nbytes * 8;
    let mut out = vec![0u8; 1 + nbytes];
    out[0] = k;
    for &h in hashes {
        let delta = h.rotate_right(17) | 1;
        let mut x = h;
        for _ in 0..k {
            let bit = (x % nbits as u64) as usize;
            out[1 + bit / 8] |= 1 << (bit % 8);
            x = x.wrapping_add(delta);
        }
    }
    out
}

pub(crate) fn may_contain(filter: &[u8], key: &[u8]) -> bool {
    if filter.len() < 2 {
        return true;
    }
    let k = filter[0];
    let bits = &filter[1..];
    let nbits = (bits.len() * 8) as u64;
    let h = hash64(key);
    let delta = h.rotate_right(17) | 1;
    let mut x = h;
    for _ in 0..k {
        let bit = (x % nbits) as usize;
        if bits[bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
        x = x.wrapping_add(delta);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_false_positive_rate() {
        let keys: Vec<Vec<u8>> = (0..5000u32)
            .map(|i| format!("key{i}").into_bytes())
            .collect();
        let hashes: Vec<u64> = keys.iter().map(|k| hash64(k)).collect();
        let f = build(&hashes, 10);
        for k in &keys {
            assert!(may_contain(&f, k));
        }
        let fp = (0..20000u32)
            .filter(|i| may_contain(&f, format!("absent{i}").as_bytes()))
            .count();
        // 10 bits/key => ~1% theoretical; allow generous slack.
        assert!(fp < 600, "false positives: {fp}/20000");
    }

    #[test]
    fn empty_filter_never_rejects() {
        assert!(may_contain(&[], b"x"));
        assert!(build(&[], 10).is_empty());
    }
}
