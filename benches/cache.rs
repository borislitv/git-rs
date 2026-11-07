#![forbid(unsafe_code)]
use git_rs::cache::Cache;
use std::io::{Read, Write};
use std::time::Instant;
fn main() {
    let cache = Cache::new(16 * 1024 * 1024, 1024 * 1024);
    let buffer = cache
        .get_or_load("synthetic:pack:digest", 1024 * 1024, |mut file| {
            file.write_all(&vec![42; 1024 * 1024])?;
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
    let mut sample = [0];
    std::fs::File::open(buffer.path())
        .unwrap()
        .read_exact(&mut sample)
        .unwrap();
    println!(
        "cache hits={operations} elapsed_us={} allocated_bytes={} pinned_sample={}",
        elapsed.as_micros(),
        cache.allocated(),
        sample[0]
    );
}
