use std::{
    io::Write,
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
use quackmap::Quack;
use rand::{Rng, RngCore, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;

#[derive(Parser, Debug)]
#[command(author, version, about)]
/// Generates a quack with random contents, then writes it to stdout.
struct Args {
    /// Number of key-value pairs to generate.
    #[arg(long, default_value_t = 1_000_000)]
    entries: usize,

    /// Size of the lookup table, larger numbers reduce collisions, improving lookup speed, smaller numbers use less space.
    ///
    /// If it's not specified, it defaults to the same value as `entries`.
    ///
    /// Defaults to the value of `entries`.
    #[arg(long)]
    slots: Option<usize>,

    /// Size in bytes of randomly generated values.
    #[arg(long, default_value_t = 32)]
    value_size: usize,
}

impl Args {
    fn slots(&self) -> usize {
        self.slots.unwrap_or(self.entries)
    }
}

fn size_needed(entries: usize, slots: usize, value_size: usize) -> Result<usize> {
    fn imple(entries: usize, slots: usize, value_size: usize) -> Option<usize> {
        let header = 16;
        let per_slot = 8;
        let value_header = 16;
        let per_value = value_size.checked_add(value_header)?;

        let slotspace = slots.checked_mul(per_slot)?;
        let valuespace = entries.checked_mul(per_value)?;

        slotspace.checked_add(valuespace)?.checked_add(header)
    }

    imple(entries, slots, value_size).ok_or_else(|| {
        anyhow::anyhow!(
            "would be too large for this platform to address, are you perhaps using a 32 bit machine?"
        )
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    let size_needed = size_needed(args.entries, args.slots(), args.value_size)?;

    let mut mmap = memmap2::MmapMut::map_anon(size_needed)?;
    let mut quack = Quack::initialize_assume_zeroed(&mut mmap, args.slots().try_into()?)?;

    let mut rng = Xoshiro256PlusPlus::from_seed(rand::random());
    let mut value = vec![0u8; args.value_size];

    let (res, elapsed) = time(|| {
        for _ in 0..args.entries {
            let k: u64 = rng.random();
            rng.fill_bytes(&mut value);
            quack.write(k, &value)?;
        }
        anyhow::Ok(())
    });
    res?;
    eprintln!(
        "Constructed a quack with {} entries in {:?}",
        args.entries, elapsed
    );
    if let Some(time_per_read) = per(elapsed, args.entries) {
        eprintln!("Time per write: {:?}", time_per_read);
    }

    let (res, elapsed) = time(|| std::io::stdout().write_all(&mmap));
    res?;
    if let Some(time_per_write) = per(elapsed, 1) {
        eprintln!("Time to write to output: {:?}", time_per_write);
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
