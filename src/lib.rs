use std::borrow::{Borrow, BorrowMut};

#[derive(Debug)]
pub struct InvalidStore;

pub struct Quack<S, D> {
    slots: S,
    store: D,

    // must not be zero, this would cause entries to be written at the beginning of the store, but zero is used to mark empty slots
    store_len: u64,
}

impl<S: Borrow<[u8]>, D: Borrow<[u8]>> Quack<S, D> {
    pub fn try_read(&self, k: u64) -> Result<Sequence<'_>, InvalidStore> {
        let slots = self.slots.borrow();
        let num_slots = slots.len() as u64 / 8;

        let Some(k) = k.checked_rem(num_slots) else {
            // map has no space
            return Ok(Sequence::empty());
        };
        let Some(k) = k.checked_mul(8) else {
            return Ok(Sequence::empty());
        };
        let start = read_u64(slots, k)?;
        Ok(Sequence {
            store: self.store.borrow(),
            start,
        })
    }
}

pub struct Sequence<'a> {
    store: &'a [u8],
    start: u64,
}

impl<'a> Iterator for Sequence<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok().unwrap_or(None)
    }
}

impl<'a> Sequence<'a> {
    fn empty() -> Self {
        Sequence {
            store: &[],
            start: 0,
        }
    }

    /// Get the next value in the sequence but return Error if the data is invalid.
    pub fn try_next(&mut self) -> Result<Option<&'a [u8]>, InvalidStore> {
        if self.start == 0 {
            return Ok(None);
        }
        let next_start = read_u64(self.store, self.start)?;
        let payload_len = read_u64(self.store, self.start.checked_add(8).ok_or(InvalidStore)?)?; // todo: variable length encode
        let ret = get_range_dynamic(
            self.store,
            self.start.checked_add(16).ok_or(InvalidStore)?,
            payload_len,
        )?;
        self.start = next_start;
        Ok(Some(ret))
    }
}

fn get_range<const N: usize>(store: &[u8], start: u64) -> Result<&[u8; N], InvalidStore> {
    let start: usize = start.try_into().map_err(|_| InvalidStore)?;
    let end = start.checked_add(N).ok_or(InvalidStore)?;
    let ret = store
        .get(start..end)
        .ok_or(InvalidStore)?
        .try_into()
        .unwrap(); // we know the slice is exactly N bytes
    Ok(ret)
}

fn get_range_dynamic(store: &[u8], start: u64, len: u64) -> Result<&[u8], InvalidStore> {
    let start: usize = start.try_into().map_err(|_| InvalidStore)?;
    let end = start
        .checked_add(len.try_into().map_err(|_| InvalidStore)?)
        .ok_or(InvalidStore)?;
    store.get(start..end).ok_or(InvalidStore)
}

fn read_u64(store: &[u8], start: u64) -> Result<u64, InvalidStore> {
    Ok(u64::from_be_bytes(*get_range(store, start)?))
}

fn write_u64(store: &mut [u8], start: u64, value: u64) -> Result<(), WriteError> {
    let start: usize = start.try_into().map_err(|_| WriteError)?;
    let bytes = value.to_be_bytes();
    let end = start.checked_add(8).ok_or(WriteError)?;
    store
        .get_mut(start..end)
        .ok_or(WriteError)?
        .copy_from_slice(&bytes);
    Ok(())
}

fn write_range(store: &mut [u8], start: u64, data: &[u8]) -> Result<(), WriteError> {
    let start: usize = start.try_into().map_err(|_| WriteError)?;
    let end = start.checked_add(data.len()).ok_or(WriteError)?;
    store
        .get_mut(start..end)
        .ok_or(WriteError)?
        .copy_from_slice(data);
    Ok(())
}

#[derive(Debug)]
pub struct WriteError;

impl From<InvalidStore> for WriteError {
    fn from(_: InvalidStore) -> Self {
        WriteError
    }
}

impl<S: BorrowMut<[u8]>, D: BorrowMut<[u8]>> Quack<S, D> {
    pub fn write(&mut self, k: u64, v: &[u8]) -> Result<(), WriteError> {
        let slots = self.slots.borrow_mut();
        let store = self.store.borrow_mut();

        let num_slots = slots.len() as u64 / 8;

        let Some(k) = k.checked_rem(num_slots) else {
            // map has no space
            return Err(WriteError);
        };
        let Some(k) = k.checked_mul(8) else {
            return Err(WriteError);
        };

        // we are going to at least 16 + v.len() bytes in store
        let new_len = self
            .store_len
            .checked_add(v.len() as u64)
            .ok_or(WriteError)?
            .checked_add(16)
            .ok_or(WriteError)?;
        if new_len > store.len() as u64 {
            return Err(WriteError);
        }

        // insert at the head of the linked list
        let target = read_u64(slots, k)?;
        write_u64(slots, k, self.store_len)?;
        write_payload(store, target, self.store_len, v)?;

        self.store_len = new_len;

        Ok(())
    }
}

/// save a payload to the store at the given location
/// payload will be preceded by
/// - a u64 for the next pointer
/// - a u64 for the length of the payload
fn write_payload(
    store: &mut [u8],
    next: u64,
    start: u64,
    payload: &[u8],
) -> Result<(), WriteError> {
    write_u64(store, start, next)?; // next pointer
    write_u64(
        store,
        start.checked_add(8).ok_or(WriteError)?,
        payload.len() as u64,
    )?;
    let payload_start = start.checked_add(16).ok_or(WriteError)?;
    write_range(store, payload_start, payload)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_key() {
        let slots = [0u8; 32];
        let store = [0u8; 64];
        let mut quack = Quack {
            slots,
            store,
            store_len: 1,
        };

        println!("Initial state: {:?}", slots);
        println!("Initial store: {:?}", store);

        quack.write(0, b"hello").unwrap();

        println!("After first write: {:?}", quack.slots);
        println!("After first write: {:?}", quack.store);

        quack.write(0, b"world").unwrap();

        println!("After writes: {:?}", quack.slots);
        println!("After writes: {:?}", quack.store);

        assert_eq!(
            quack.try_read(0).unwrap().collect::<Vec<_>>(),
            [b"world", b"hello"]
        );
    }

    #[test]
    fn multiple_keys() {
        let slots = [0u8; 32];
        let store = [0u8; 128];
        let mut quack = Quack {
            slots,
            store,
            store_len: 1,
        };

        quack.write(0, b"hello").unwrap();
        quack.write(1, b"world").unwrap();
        quack.write(2, b"quack").unwrap();

        assert_eq!(quack.try_read(0).unwrap().collect::<Vec<_>>(), [b"hello"]);
        assert_eq!(quack.try_read(1).unwrap().collect::<Vec<_>>(), [b"world"]);
        assert_eq!(quack.try_read(2).unwrap().collect::<Vec<_>>(), [b"quack"]);
    }

    #[test]
    fn miss() {
        let slots = [0u8; 32];
        let store = [0u8; 64];
        let mut quack = Quack {
            slots,
            store,
            store_len: 1,
        };

        quack.write(0, b"hello").unwrap();

        assert!(quack.try_read(1).unwrap().next().is_none());
    }
}
