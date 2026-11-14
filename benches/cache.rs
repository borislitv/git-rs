#![forbid(unsafe_code)]
use git_rs::cache::Cache;
use std::time::Instant;
fn main() {
    let cache = Cache::new(16 * 1024 * 1024, 1024 * 1024);
    let buffer = cache
        .get_or_load("synthetic:pack:digest", 1024 * 1024, |output| {
            output.fill(42);
            Ok(())
        })
        .unwrap()
        .unwrap();
    let start = Instant::now();
    let operations = 100_000;
    for _ in 0..operations {
        let hit = cache
            .get_or_load("synthetic:pack:digest", 1024 * 1024, |_| {
                panic!("unexpected miss")
            })
            .unwrap()
            .unwrap();
        std::hint::black_box(hit);
    }
    let elapsed = start.elapsed();
    println!(
        "cache hits={operations} elapsed_us={} allocated_bytes={} pinned_sample={}",
        elapsed.as_micros(),
        cache.allocated(),
        buffer[0]
    );
}
