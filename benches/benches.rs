use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use memmap2::{Mmap, MmapMut};
use quackmap::Quack;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::OpenOptions;
use std::hint::black_box;
use std::iter;
use std::path::Path;
use std::time::Duration;

const VAL_SIZE: usize = 32;

fn haystack() -> impl Iterator<Item = (u64, [u8; VAL_SIZE])> + Clone {
    let mut rng = StdRng::seed_from_u64(42);
    iter::from_fn(move || {
        let key = rng.random();
        let value = rng.random();
        Some((key, value))
    })
}

fn needles() -> impl Iterator<Item = u64> {
    let mut rng = StdRng::seed_from_u64(123);
    iter::from_fn(move || Some(rng.random()))
}

fn size_needed(num_slots: usize, max_vals: usize) -> usize {
    let header = 16;
    let per_slot = 8;
    let per_value = 16 + VAL_SIZE;
    header + per_slot * num_slots + per_value * max_vals
}

unsafe fn load_quack(path: impl AsRef<Path>) -> Quack<Mmap> {
    let file = OpenOptions::new().read(true).open(path).unwrap();
    let buffer = unsafe { Mmap::map(&file).unwrap() };
    buffer.advise(memmap2::Advice::Random).unwrap();
    Quack::new(buffer)
}

unsafe fn load_quack_mut(path: impl AsRef<Path>) -> Quack<MmapMut> {
    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(path)
        .unwrap();
    let buffer = unsafe { MmapMut::map_mut(&file).unwrap() };
    buffer.advise(memmap2::Advice::Random).unwrap();
    Quack::new(buffer)
}

fn construct_quack(
    path: impl AsRef<Path>,
    num_slots: usize,
    max_vals: usize,
    haystack: impl IntoIterator<Item = (u64, [u8; VAL_SIZE])>,
) {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .read(true)
        .open(path)
        .unwrap();
    file.set_len(size_needed(num_slots, max_vals) as u64)
        .unwrap();

    let mut buffer = unsafe { MmapMut::map_mut(&file).unwrap() };
    buffer.advise(memmap2::Advice::Random).unwrap();
    let mut quack = Quack::initialize_assume_zeroed(&mut buffer, num_slots as u64).unwrap();
    for (key, value) in haystack {
        quack.write(key, &value).unwrap();
    }
    buffer.flush().unwrap();
}

fn run_large_dataset_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_dataset");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("random_write", |b| {
        // using tmpdir tmpfile so sigterm during these benches could leak into /tmp
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("quack.mmap");
        let empty: [(u64, [u8; VAL_SIZE]); 0] = [];

        construct_quack(&path, 900_000_000, 900_000_000, empty);
        let mut quack = unsafe { load_quack_mut(&path) };

        let mut rando_kvs = haystack();

        b.iter(|| {
            let (k, v) = rando_kvs.next().unwrap();
            quack.write(k, &v).unwrap();
        });

        drop(tmpdir);
    });

    // would be nice to test larger sizes like
    // 1_000_000, 10_000_000, 100_000_000, and 900_000_000 to compare against rocksdb
    // https://github.com/facebook/rocksdb/wiki/performance-benchmarks
    // but writes are not fast enough yet, setup for
    // building a 900_000_000 takes too long.
    // linux kernel seems to have trouble managing
    // so many dirty pages, tends to fill up fs cache
    // bringing host to a crawl
    //
    // possible improvements:
    //   1. Use DMA with io_uring so writes can be smaller than
    //      4KiB (maybe kernel does this already). Would need to manage
    //      our own cache while async operations happen. complitated
    //   2. Try syscalls instead of mmap. Use pread and pwrite.
    //      But this might not actually solve the 4KiB page size issue.
    //   3. Make quack more efficient, add a batch write that doesn't
    //      update bump_allocator pointer in storeage on every write.
    //      - Maybe don't even store bump allocator length. Treat
    //        writes as a one-time batch operation.
    for &size in &[1_000, 10_000, 100_000] {
        group.bench_function(BenchmarkId::new("lookup", size), |b| {
            let num_slots = size;
            let num_vals = size;

            let tmpdir = tempfile::tempdir().unwrap();
            let path = tmpdir.path().join("quack.mmap");

            construct_quack(&path, num_slots, num_vals, haystack().take(num_vals));
            let quack = unsafe { load_quack(&path) };

            let mut rando_vs = needles();

            b.iter(|| {
                let next_key = rando_vs.next().unwrap();
                for val in quack.read(next_key).unwrap() {
                    black_box(val);
                }
            });

            drop(tmpdir);
        });
    }

    group.finish();
}

criterion_group!(benches, run_large_dataset_benchmark);
criterion_main!(benches);
