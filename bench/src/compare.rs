//! Regression gate. A change is a regression only if it is worse than the
//! threshold, and the threshold is deliberately ABOVE measured noise:
//!
//!   threshold_pct = max(floor_pct, 1.5 * max(spread_baseline, spread_current))
//!
//! `spread` is (max-min)/median over the measured runs of that very metric
//! (recorded in each result file), so a noisy metric gets a wider gate
//! automatically. `floor_pct` defaults to 30: above the 5-25% run-to-run
//! variation measured on the test VM. Correctness failures (`ok:false`) are
//! always failures regardless of threshold.

use std::path::Path;

use crate::json::J;

pub const DEFAULT_FLOOR_PCT: f64 = 30.0;
pub const NOISE_MULTIPLIER: f64 = 1.5;

pub fn threshold(floor: f64, spread_a: f64, spread_b: f64) -> f64 {
    floor.max(NOISE_MULTIPLIER * spread_a.max(spread_b))
}

/// Returns (regressed?, signed % change where positive = worse, threshold).
pub fn judge(
    higher_is_better: bool,
    base: f64,
    cur: f64,
    spread_a: f64,
    spread_b: f64,
    floor: f64,
) -> (bool, f64, f64) {
    let th = threshold(floor, spread_a, spread_b);
    if base == 0.0 {
        return (false, 0.0, th);
    }
    let worse_pct = if higher_is_better {
        (base - cur) / base * 100.0
    } else {
        (cur - base) / base * 100.0
    };
    (worse_pct > th, worse_pct, th)
}

fn benches(doc: &J) -> Vec<&J> {
    doc.get("benchmarks")
        .and_then(|b| b.arr())
        .map(|a| a.iter().collect())
        .unwrap_or_default()
}

fn med(b: &J, path: &str) -> Option<(f64, f64)> {
    let m = b.path(&format!("{path}.median"))?.num()?;
    let sp = b.path(&format!("{path}.spread_pct"))?.num()?;
    Some((m, sp))
}

/// Compares every result file present in both directories. Returns true if
/// no regression and no correctness failure.
pub fn compare_dirs(base: &Path, cur: &Path, floor: f64) -> Result<bool, String> {
    let mut good = true;
    let mut files: Vec<_> = std::fs::read_dir(cur)
        .map_err(|e| format!("{}: {e}", cur.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    files.sort();
    for f in files {
        let name = f.file_name().unwrap().to_owned();
        let bp = base.join(&name);
        let cdoc = J::parse(&std::fs::read_to_string(&f).map_err(|e| e.to_string())?)
            .map_err(|e| format!("{}: {e}", f.display()))?;
        let bdoc = match std::fs::read_to_string(&bp) {
            Ok(t) => Some(J::parse(&t).map_err(|e| format!("{}: {e}", bp.display()))?),
            Err(_) => None,
        };
        println!("== {} ==", name.to_string_lossy());
        for cb in benches(&cdoc) {
            let nm = cb.get("name").and_then(|x| x.str()).unwrap_or("?");
            if !matches!(cb.get("ok"), Some(J::Bool(true))) {
                println!("FAIL  {nm}: correctness/self-check failed in current results");
                good = false;
                continue;
            }
            let Some(bd) = &bdoc else {
                println!("skip  {nm}: no baseline file");
                continue;
            };
            let Some(bb) = benches(bd)
                .into_iter()
                .find(|b| b.get("name").and_then(|x| x.str()) == Some(nm))
            else {
                println!("skip  {nm}: not in baseline");
                continue;
            };
            let hib = matches!(cb.get("higher_is_better"), Some(J::Bool(true)));
            let mut line_bad = false;
            let mut parts = Vec::new();
            if let (Some((a, sa)), Some((c, sc))) = (med(bb, "primary"), med(cb, "primary")) {
                let (bad, w, th) = judge(hib, a, c, sa, sc, floor);
                parts.push(format!(
                    "primary {a:.1}->{c:.1} ({:+.1}% worse, gate {th:.0}%, noise {:.0}%/{:.0}%)",
                    w, sa, sc
                ));
                line_bad |= bad;
            }
            if let (Some((a, sa)), Some((c, sc))) =
                (med(bb, "latency_p99_ms"), med(cb, "latency_p99_ms"))
            {
                let (bad, w, th) = judge(false, a, c, sa, sc, floor);
                parts.push(format!(
                    "p99 {a:.3}->{c:.3}ms ({:+.1}% worse, gate {th:.0}%)",
                    w
                ));
                line_bad |= bad;
            }
            println!(
                "{}  {nm}: {}",
                if line_bad { "REGR" } else { "ok  " },
                parts.join(" | ")
            );
            good &= !line_bad;
        }
    }
    Ok(good)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_is_above_noise() {
        // 25% spread seen on the VM -> gate must exceed it.
        assert!(threshold(30.0, 25.0, 10.0) > 25.0);
        // a very noisy metric widens the gate beyond the floor
        assert_eq!(threshold(30.0, 40.0, 5.0), 60.0);
    }

    #[test]
    fn noise_does_not_trip_but_real_drop_does() {
        // throughput 1000 -> 800 (-20%), noise 25%: not a regression
        assert!(!judge(true, 1000.0, 800.0, 25.0, 20.0, 30.0).0);
        // 1000 -> 500 (-50%): regression
        assert!(judge(true, 1000.0, 500.0, 25.0, 20.0, 30.0).0);
        // lower-is-better: 100ms -> 180ms is +80%: regression
        assert!(judge(false, 100.0, 180.0, 5.0, 5.0, 30.0).0);
        // improvement never regresses
        assert!(!judge(true, 1000.0, 5000.0, 5.0, 5.0, 30.0).0);
    }
}
