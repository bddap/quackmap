use std::{
    io::Write,
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
use memmap2::MmapMut;
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

    /// After initial build, run a second pass to optimize reads. The resulting quack will have values with the same key
    /// stored adjacent to eachother in memory.
    #[arg(long)]
    optimize: bool,
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

fn create_mmaped_mut_quack(slots: usize, size_bytes: usize) -> Result<Quack<MmapMut>> {
    let mmap = MmapMut::map_anon(size_bytes)?;
    let quack = Quack::initialize_assume_zeroed(mmap, slots.try_into()?)?;
    Ok(quack)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let size_needed = size_needed(args.entries, args.slots(), args.value_size)?;

    // let mmap = MmapMut::map_anon(size_needed)?;
    // let mut quack = Quack::initialize_assume_zeroed(mmap, args.slots().try_into()?)?;
    let mut quack = create_mmaped_mut_quack(args.slots(), size_needed)?;

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

    if args.optimize {
        let (res, elapsed) = time(|| optimize(&quack));
        quack = res?;
        eprintln!("Optimized the quack in {:?}", elapsed);
    }

    let (res, elapsed) = time(|| std::io::stdout().write_all(&quack.into_inner()));
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

fn optimize(quack: &Quack<MmapMut>) -> Result<Quack<MmapMut>> {
    let size = quack.ref_inner().len();
    let num_slots = quack.slots()?;
    let mut optimized_quack = create_mmaped_mut_quack(num_slots.try_into()?, size)?;
    for slot in 0..num_slots {
        for entry in quack.read(slot)? {
            optimized_quack.write(slot, entry)?;
        }
    }

    #[cfg(debug_assertions)]
    {
        for slot in 0..num_slots {
            let inps = Vec::<&[u8]>::from_iter(quack.read(slot)?);
            let mut outps = Vec::<&[u8]>::from_iter(optimized_quack.read(slot)?);
            outps.reverse();
            assert_eq!(inps, outps, "slot {} does not match", slot);
        }
    }

    Ok(optimized_quack)
}
