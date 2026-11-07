//! A byte-bounded LRU block cache, shared by every SSTable of one engine.
//!
//! Slab-backed doubly linked list + hash map: O(1) get/insert/evict.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) type BlockKey = (u64, u64); // (sst id, block offset)

struct Node {
    key: BlockKey,
    val: Arc<Vec<u8>>,
    prev: usize,
    next: usize,
}

const NIL: usize = usize::MAX;

struct Inner {
    map: HashMap<BlockKey, usize>,
    slab: Vec<Node>,
    free: Vec<usize>,
    head: usize, // most recently used
    tail: usize, // least recently used
    bytes: usize,
    capacity: usize,
}

impl Inner {
    fn unlink(&mut self, i: usize) {
        let (p, n) = (self.slab[i].prev, self.slab[i].next);
        if p != NIL {
            self.slab[p].next = n;
        } else {
            self.head = n;
        }
        if n != NIL {
            self.slab[n].prev = p;
        } else {
            self.tail = p;
        }
    }
    fn push_front(&mut self, i: usize) {
        self.slab[i].prev = NIL;
        self.slab[i].next = self.head;
        if self.head != NIL {
            let h = self.head;
            self.slab[h].prev = i;
        }
        self.head = i;
        if self.tail == NIL {
            self.tail = i;
        }
    }
}

pub struct BlockCache {
    inner: Mutex<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                slab: Vec::new(),
                free: Vec::new(),
                head: NIL,
                tail: NIL,
                bytes: 0,
                capacity: capacity_bytes,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    pub(crate) fn get(&self, key: BlockKey) -> Option<Arc<Vec<u8>>> {
        let mut g = self.inner.lock().unwrap();
        match g.map.get(&key).copied() {
            Some(i) => {
                g.unlink(i);
                g.push_front(i);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(g.slab[i].val.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub(crate) fn insert(&self, key: BlockKey, val: Arc<Vec<u8>>) {
        let mut g = self.inner.lock().unwrap();
        if val.len() > g.capacity {
            return;
        }
        if let Some(&i) = g.map.get(&key) {
            // Replace in place (same key => same bytes in practice).
            g.bytes = g.bytes - g.slab[i].val.len() + val.len();
            g.slab[i].val = val;
            g.unlink(i);
            g.push_front(i);
        } else {
            g.bytes += val.len();
            let node = Node {
                key,
                val,
                prev: NIL,
                next: NIL,
            };
            let i = match g.free.pop() {
                Some(i) => {
                    g.slab[i] = node;
                    i
                }
                None => {
                    g.slab.push(node);
                    g.slab.len() - 1
                }
            };
            g.map.insert(key, i);
            g.push_front(i);
        }
        while g.bytes > g.capacity && g.tail != NIL {
            let t = g.tail;
            g.unlink(t);
            let k = g.slab[t].key;
            g.bytes -= g.slab[t].val.len();
            g.slab[t].val = Arc::new(Vec::new());
            g.map.remove(&k);
            g.free.push(t);
        }
    }

    /// Drop every cached block of one SSTable (it has been compacted away).
    pub(crate) fn evict_file(&self, sst_id: u64) {
        let mut g = self.inner.lock().unwrap();
        let victims: Vec<BlockKey> = g.map.keys().copied().filter(|k| k.0 == sst_id).collect();
        for k in victims {
            let i = g.map.remove(&k).unwrap();
            g.unlink(i);
            g.bytes -= g.slab[i].val.len();
            g.slab[i].val = Arc::new(Vec::new());
            g.free.push(i);
        }
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
    pub fn used_bytes(&self) -> usize {
        self.inner.lock().unwrap().bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used_within_capacity() {
        let c = BlockCache::new(100);
        c.insert((1, 0), Arc::new(vec![0; 40]));
        c.insert((1, 1), Arc::new(vec![0; 40]));
        assert!(c.get((1, 0)).is_some()); // touch 0 => 1 is now LRU
        c.insert((1, 2), Arc::new(vec![0; 40]));
        assert!(c.get((1, 1)).is_none(), "LRU entry evicted");
        assert!(c.get((1, 0)).is_some());
        assert!(c.get((1, 2)).is_some());
        assert!(c.used_bytes() <= 100);
    }

    #[test]
    fn evict_file_removes_only_that_file() {
        let c = BlockCache::new(1000);
        c.insert((1, 0), Arc::new(vec![0; 10]));
        c.insert((2, 0), Arc::new(vec![0; 10]));
        c.evict_file(1);
        assert!(c.get((1, 0)).is_none());
        assert!(c.get((2, 0)).is_some());
        assert_eq!(c.used_bytes(), 10);
    }
}
