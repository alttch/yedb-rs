use std::time::{Duration, Instant};
use yedb::Context;

fn main() {
    let n = 100_000;
    let rep = 5u64;
    let mut ctx = Context::create("/tmp/ctx1", n, None);
    ctx.set_cache_size(n as usize);
    let t_start = Instant::now();
    for _ in 0..rep {
        for reg in 0..n {
            ctx.set(reg, -777);
        }
    }
    let duration = Instant::now() - t_start;
    println!(
        "{}: {} ops/sec",
        "set",
        ((((n * rep) as f64) / (duration.as_micros() as f64 / 1_000_000f64)) as u64).to_string()
    );
    ctx.clear_cache();
    ctx.set_cache_size(0);
    let t_start = Instant::now();
    for _ in 0..rep {
        for reg in 0..n {
            ctx.get(reg);
        }
    }
    let duration = Instant::now() - t_start;
    println!(
        "{}: {} ops/sec",
        "get",
        ((((n * rep) as f64) / (duration.as_micros() as f64 / 1_000_000f64)) as u64).to_string()
    );
    ctx.set_cache_size(n as usize);
    for reg in 0..n {
        ctx.get(reg);
    }
    let t_start = Instant::now();
    for _ in 0..rep {
        for reg in 0..n {
            ctx.get(reg);
        }
    }
    let duration = Instant::now() - t_start;
    println!(
        "{}: {} ops/sec",
        "get(cached)",
        ((((n * rep) as f64) / (duration.as_micros() as f64 / 1_000_000f64)) as u64).to_string()
    );
    let t_start = Instant::now();
    for _ in 0..rep {
        for reg in 0..n {
            ctx.increment(reg);
        }
    }
    let duration = Instant::now() - t_start;
    println!(
        "{}: {} ops/sec",
        "increment",
        ((((n * rep) as f64) / (duration.as_micros() as f64 / 1_000_000f64)) as u64).to_string()
    );
}
