use std::fs::File;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use memmap2::Mmap;
use quackmap::Quack;
use rand::RngCore;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

/// Reads random entries from a quack, measuring time per read.
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to the file containing the quack
    quack: PathBuf,

    /// Number of random read attempts to make
    #[arg(long, default_value_t = 1_000_000)]
    reads: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let file = File::open(&args.quack)?;

    // Safety: YOLO
    let mmap = unsafe { Mmap::map(&file)? };

    drop(file);

    let quack = Quack::new(mmap);

    let mut rng = Xoshiro256PlusPlus::from_seed(rand::random());

    let (res, elapsed) = time(|| {
        for _ in 0..args.reads {
            let key = rng.next_u64();
            quack.read(key)?;
            for _ in quack.read(key)? {}
        }
        anyhow::Ok(())
    });
    res?;

    eprintln!("Read {} entries in {:?}", args.reads, elapsed);
    if let Some(time_per_read) = per(elapsed, args.reads) {
        eprintln!("Time per read: {:?}", time_per_read);
    }

    Ok(())
}

fn time<F: FnOnce() -> R, R>(f: F) -> (R, Duration) {
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed();
    (result, elapsed)
}

fn per(d: Duration, n: usize) -> Option<Duration> {
    let nanos: u64 = d
        .as_nanos()
        .checked_div(n.try_into().ok()?)?
        .try_into()
        .ok()?;
    Some(Duration::from_nanos(nanos))
}
