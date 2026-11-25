use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Envelope;

static COUNTER: AtomicU64 = AtomicU64::new(0);
static EVENT_COUNTER: AtomicU64 = AtomicU64::new(0);

const EVENT_TIME_BITS: u32 = 48;
const EVENT_TAIL_BITS: u32 = 80;
const EVENT_TAIL_MASK: u128 = (1u128 << EVENT_TAIL_BITS) - 1;
const EVENT_ID_CHARS: usize = 26;
const CROCKFORD: &[u8] = b"0123456789abcdefghjkmnpqrstvwxyz";

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

pub fn event_id(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        & ((1u128 << EVENT_TIME_BITS) - 1);
    let count = EVENT_COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let tail = process_nonce().wrapping_add(count) & EVENT_TAIL_MASK;
    let value = (millis << EVENT_TAIL_BITS) | tail;
    format!("{prefix}.{}", crockford32(value))
}

pub fn is_event_id(id: &str) -> bool {
    id.rsplit_once('.').is_some_and(|(_, body)| {
        body.len() == EVENT_ID_CHARS && body.bytes().all(|b| CROCKFORD.contains(&b))
    })
}

pub fn admission_key(event: &Envelope) -> String {
    if is_event_id(&event.id) {
        event.id.clone()
    } else {
        format!("{}|{}|{}", event.node, event.ts, event.id)
    }
}

fn process_nonce() -> u128 {
    static NONCE: OnceLock<u128> = OnceLock::new();
    *NONCE.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let word = |salt: u64| {
            let mut h = RandomState::new().build_hasher();
            h.write_u64(salt);
            h.write_u128(nanos);
            h.write_u32(std::process::id());
            h.finish() as u128
        };
        ((word(1) << 64) | word(2)) & EVENT_TAIL_MASK
    })
}

fn crockford32(mut n: u128) -> String {
    let mut out = vec![b'0'; EVENT_ID_CHARS];
    for slot in out.iter_mut().rev() {
        *slot = CROCKFORD[(n & 31) as usize];
        n >>= 5;
    }
    String::from_utf8(out).unwrap()
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

    #[test]
    fn event_ids_are_unique_at_volume() {
        let ids: Vec<String> = (0..100_000).map(|_| event_id("PH")).collect();
        let distinct: std::collections::HashSet<&String> = ids.iter().collect();
        assert_eq!(distinct.len(), ids.len());
        assert!(ids.iter().all(|id| is_event_id(id)));
        assert!(!is_event_id("PH.4k2x"));
    }
}
