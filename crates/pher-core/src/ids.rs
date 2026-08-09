use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Honeybee-style short id, e.g. `PH.4k2x`. Time-seeded, per-process counter
/// mixed in; collision-resistant enough for a single node's lifetime.
pub fn short_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    // Mix with a splitmix64-style finalizer so consecutive ids don't share prefixes.
    let mut x = nanos ^ count.wrapping_mul(0x9E3779B97F4A7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    format!("{prefix}.{}", base36(x % 36u64.pow(4), 4))
}

fn base36(mut n: u64, width: usize) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = vec![b'0'; width];
    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[(n % 36) as usize];
        n /= 36;
    }
    String::from_utf8(out).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_have_shape_and_vary() {
        let a = short_id("PH");
        let b = short_id("PH");
        assert!(a.starts_with("PH."));
        assert_eq!(a.len(), 7);
        assert_ne!(a, b);
    }
}
