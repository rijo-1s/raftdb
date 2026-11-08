//! Linearizability checker for a key-value register history.
//!
//! This is the Wing & Gong (1993) search with Lowe's (2017) memoisation of
//! visited (linearised-set, state) configurations -- the algorithm used by
//! Knossos/Porcupine -- applied per key, which is sound because linearizability
//! is compositional across independent objects (Herlihy & Wing 1990; the
//! "P-compositionality" decomposition of Horn & Kroening 2015). Each key is a
//! register: `Set(v)`, `Delete`, `Get -> Option<v>`.
//!
//! Operations whose outcome the client never learned (timeout, crash of the
//! node it talked to) have `ret == None`: they may have taken effect at any
//! point after their call, or never. A `Get` with no response has no effect
//! and must be omitted by the caller.

use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Set(String),
    Delete,
    Get,
}

#[derive(Clone, Debug)]
pub struct Rec {
    pub client: usize,
    pub key: String,
    pub op: Op,
    /// Logical invocation time.
    pub call: u64,
    /// Logical response time; `None` = outcome unknown (may or may not have applied).
    pub ret: Option<u64>,
    /// For a completed `Get`: what it returned.
    pub got: Option<Option<String>>,
}

#[derive(Debug)]
pub enum Verdict {
    Linearizable,
    Violation { key: String, ops: Vec<Rec> },
    Inconclusive { key: String },
}

const NIL: usize = usize::MAX;
const STEP_BUDGET: u64 = 20_000_000;

pub fn check(history: &[Rec]) -> Verdict {
    let mut by_key: HashMap<&str, Vec<&Rec>> = HashMap::new();
    for r in history {
        by_key.entry(r.key.as_str()).or_default().push(r);
    }
    let mut keys: Vec<_> = by_key.keys().copied().collect();
    keys.sort_unstable();
    for k in keys {
        let ops = &by_key[k];
        match check_register(ops) {
            Some(true) => {}
            Some(false) => {
                return Verdict::Violation {
                    key: k.to_string(),
                    ops: ops.iter().map(|r| (*r).clone()).collect(),
                }
            }
            None => return Verdict::Inconclusive { key: k.to_string() },
        }
    }
    Verdict::Linearizable
}

fn step(state: &Option<String>, op: &Rec) -> Option<Option<String>> {
    match &op.op {
        Op::Set(v) => Some(Some(v.clone())),
        Op::Delete => Some(None),
        Op::Get => match &op.got {
            Some(g) if g == state => Some(state.clone()),
            _ => None,
        },
    }
}

/// `Some(true)` linearizable, `Some(false)` not, `None` budget exhausted.
fn check_register(ops: &[&Rec]) -> Option<bool> {
    let n = ops.len();
    if n == 0 {
        return Some(true);
    }
    // Event list: (time, op index, is_call). Unknown-outcome ops get their
    // (fake) return after everything else.
    let mut events: Vec<(u64, usize, bool)> = Vec::with_capacity(2 * n);
    for (i, o) in ops.iter().enumerate() {
        events.push((o.call, i, true));
        events.push((o.ret.unwrap_or(u64::MAX / 2 + i as u64), i, false));
    }
    events.sort_by_key(|e| (e.0, !e.2)); // calls before returns on ties
    let m = events.len();
    // node 0 = head sentinel; events are nodes 1..=m
    let mut op_of = vec![0usize; m + 1];
    let mut is_call = vec![false; m + 1];
    let mut matched = vec![NIL; m + 1];
    let mut call_node = vec![0usize; n];
    let mut ret_node = vec![0usize; n];
    for (pos, &(_, i, call)) in events.iter().enumerate() {
        let node = pos + 1;
        op_of[node] = i;
        is_call[node] = call;
        if call {
            call_node[i] = node;
        } else {
            ret_node[i] = node;
        }
    }
    for i in 0..n {
        matched[call_node[i]] = ret_node[i];
    }
    let mut next: Vec<usize> = (0..=m).map(|x| if x < m { x + 1 } else { NIL }).collect();
    let mut prev: Vec<usize> = (0..=m).map(|x| if x == 0 { NIL } else { x - 1 }).collect();

    let mut required_left = ops.iter().filter(|o| o.ret.is_some()).count();
    let words = n.div_ceil(64);
    let mut lin = vec![0u64; words];
    let mut state: Option<String> = None;
    let mut cache: HashSet<(Vec<u64>, Option<String>)> = HashSet::new();
    let mut stack: Vec<(usize, Option<String>)> = Vec::new(); // (call node, state before)
    let mut steps = 0u64;
    let mut entry = next[0];

    while required_left > 0 {
        steps += 1;
        if steps > STEP_BUDGET {
            return None;
        }
        if entry != NIL && is_call[entry] {
            let i = op_of[entry];
            if let Some(new_state) = step(&state, ops[i]) {
                let mut nl = lin.clone();
                nl[i / 64] |= 1 << (i % 64);
                if cache.insert((nl.clone(), new_state.clone())) {
                    stack.push((entry, state.clone()));
                    state = new_state;
                    lin = nl;
                    if ops[i].ret.is_some() {
                        required_left -= 1;
                    }
                    lift(entry, &matched, &mut next, &mut prev);
                    entry = next[0];
                    continue;
                }
            }
            entry = next[entry];
        } else {
            // A return that cannot be passed (or the end of the list):
            // backtrack the most recent linearisation.
            let Some((call, old)) = stack.pop() else {
                return Some(false);
            };
            let i = op_of[call];
            lin[i / 64] &= !(1 << (i % 64));
            if ops[i].ret.is_some() {
                required_left += 1;
            }
            state = old;
            unlift(call, &matched, &mut next, &mut prev);
            entry = next[call];
        }
    }
    Some(true)
}

fn lift(c: usize, matched: &[usize], next: &mut [usize], prev: &mut [usize]) {
    let r = matched[c];
    for x in [c, r] {
        let (p, nx) = (prev[x], next[x]);
        next[p] = nx;
        if nx != NIL {
            prev[nx] = p;
        }
    }
}

fn unlift(c: usize, matched: &[usize], next: &mut [usize], prev: &mut [usize]) {
    let r = matched[c];
    for x in [r, c] {
        let (p, nx) = (prev[x], next[x]);
        next[p] = x;
        if nx != NIL {
            prev[nx] = x;
        }
    }
}
