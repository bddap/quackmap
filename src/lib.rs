use core::mem::size_of;

/// We store everything in one buffer. The layout is:
/// [0..8):             u64 num_slots
/// [8..16):            u64 store_len, serves as bump allocator state
/// [16..num_slots+16): slots array
/// [num_slots+16..):   store
///
/// The store is bump-allocated storage for linked lists elements. Each element
/// has this layout:
/// [0..8):                    u64 next pointer
/// [8..16):                   u64 payload length
/// [16..payload_length + 16): payload data
///
/// u64s are stored big-endian
mod stor {
    use super::*;

    pub const NUM_SLOTS_OFFSET: u64 = 0;
    pub const STORE_LEN_OFFSET: u64 = size_of::<u64>() as u64;
    pub const SLOTS_START: u64 = STORE_LEN_OFFSET + size_of::<u64>() as u64;

    pub fn read_num_slots(data: &[u8]) -> Result<u64, OutaBounds> {
        super::read_u64(data, NUM_SLOTS_OFFSET)
    }

    pub fn read_store_len(data: &[u8]) -> Result<u64, OutaBounds> {
        super::read_u64(data, STORE_LEN_OFFSET)
    }

    pub fn write_store_len(data: &mut [u8], store_len: u64) -> Result<(), OutaBounds> {
        super::write_u64(data, STORE_LEN_OFFSET, store_len)
    }

    pub fn write_num_slots(data: &mut [u8], num_slots: u64) -> Result<(), OutaBounds> {
        super::write_u64(data, NUM_SLOTS_OFFSET, num_slots)
    }

    pub fn read_slot(data: &[u8], slot_index: u64) -> Result<u64, OutaBounds> {
        let slot_offset = slot_index
            .checked_mul(size_of::<u64>() as u64)
            .ok_or(OutaBounds)?
            .checked_add(SLOTS_START)
            .ok_or(OutaBounds)?;
        super::read_u64(data, slot_offset)
    }

    pub fn write_slot(data: &mut [u8], slot_index: u64, value: u64) -> Result<(), OutaBounds> {
        let slot_offset = slot_index
            .checked_mul(size_of::<u64>() as u64)
            .ok_or(OutaBounds)?
            .checked_add(SLOTS_START)
            .ok_or(OutaBounds)?;
        super::write_u64(data, slot_offset, value)
    }

    pub fn store_start(num_slots: u64) -> Result<u64, OutaBounds> {
        num_slots
            .checked_mul(size_of::<u64>() as u64)
            .and_then(|slots_byte_size| SLOTS_START.checked_add(slots_byte_size))
            .ok_or(OutaBounds)
    }
}

/// Values stored in the store. Each is a linked list. Layout:
/// [0..8):                    u64 next pointer
/// [8..16):                   u64 payload length
/// [16..payload_length + 16): payload data
mod val {
    use super::*;

    pub const NEXT_POINTER_OFFSET: u64 = 0;
    pub const PAYLOAD_LEN_OFFSET: u64 = size_of::<u64>() as u64;
    pub const PAYLOAD_START: u64 = PAYLOAD_LEN_OFFSET + size_of::<u64>() as u64;

    pub fn write(data: &mut [u8], start: u64, next: u64, payload: &[u8]) -> Result<(), OutaBounds> {
        write_u64(
            data,
            val::NEXT_POINTER_OFFSET
                .checked_add(start)
                .ok_or(OutaBounds)?,
            next,
        )?;
        write_u64(
            data,
            val::PAYLOAD_LEN_OFFSET
                .checked_add(start)
                .ok_or(OutaBounds)?,
            payload.len() as u64,
        )?;
        write_range(
            data,
            val::PAYLOAD_START.checked_add(start).ok_or(OutaBounds)?,
            payload,
        )?;
        Ok(())
    }
}

/// Calculate the required buffer size for the backing store
/// given a number of slots and the sizes of the values to be written.
/// Assumes each value will be written with 16 bytes of overhead.
pub fn calculate_store_size<T>(slot_count: u64, value_sizes: T) -> Result<u64, OutaBounds>
where
    T: IntoIterator<Item = u64>,
{
    value_sizes
        .into_iter()
        .try_fold(stor::store_start(slot_count)?, |acc, size| {
            // Each value has 16 bytes overhead (next pointer + len)
            acc.checked_add(val::PAYLOAD_START).ok_or(OutaBounds)?
              .checked_add(size).ok_or(OutaBounds)
        })
}

#[derive(Debug)]
pub struct OutaBounds;

pub struct Quack<B> {
    /// Single buffer holding num_slots, store_len, the slots array, and store data.
    data: B,
}

impl<B> Quack<B> {
    pub fn new(data: B) -> Self {
        Quack { data }
    }
}

impl<B: AsRef<[u8]>> Quack<B> {
    pub fn read(&self, k: u64) -> Result<Sequence<'_>, OutaBounds> {
        let data = self.data.as_ref();

        let num_slots = stor::read_num_slots(data)?;

        let Some(slot_index) = k.checked_rem(num_slots) else {
            return Ok(Sequence::empty());
        };

        let head = stor::read_slot(data, slot_index)?;

        Ok(Sequence { data, next: head })
    }
}

impl<B: AsMut<[u8]>> Quack<B> {
    /// Initializes the Quack with a given number of slots
    /// the data store provided must be all zeroes.
    pub fn initialize_assume_zeroed(
        mut data: B,
        num_slots: u64,
    ) -> Result<Self, OutaBounds> {
        let dat = data.as_mut();
        if dat.len() < stor::store_start(num_slots)? as usize {
            return Err(OutaBounds);
        }
        stor::write_store_len(dat, 0)?;
        stor::write_num_slots(dat, num_slots)?;
        Ok(Quack { data })
    }

    /// Writes an item for a given key by prepending it to the linked list in that slot.
    pub fn write(&mut self, k: u64, v: &[u8]) -> Result<(), OutaBounds> {
        let data = self.data.as_mut();

        let num_slots = stor::read_num_slots(data)?;
        let store_len = stor::read_store_len(data)?;

        let slot_index = k.checked_rem(num_slots).ok_or(OutaBounds)?;

        let new_len = (size_of::<u64>() as u64 * 2)
            .checked_add(v.len() as u64)
            .ok_or(OutaBounds)?
            .checked_add(store_len)
            .ok_or(OutaBounds)?;

        let store_start = stor::store_start(num_slots)?;

        let required_data_size = store_start.checked_add(new_len).ok_or(OutaBounds)?;

        if required_data_size > data.len() as u64 {
            return Err(OutaBounds);
        }

        let old_head = stor::read_slot(data, slot_index)?;
        let new_head = store_len.checked_add(store_start).ok_or(OutaBounds)?;
        val::write(data, new_head, old_head, v)?;
        stor::write_slot(data, slot_index, new_head)?;
        stor::write_store_len(data, new_len)?;

        Ok(())
    }
}

/// An iterator over values stored in the Quack.
/// Essentially a view of a linked list.
pub struct Sequence<'a> {
    data: &'a [u8],
    next: u64,
}

impl<'a> Iterator for Sequence<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().ok().unwrap_or(None)
    }
}

impl<'a> Sequence<'a> {
    fn empty() -> Self {
        Sequence { data: &[], next: 0 }
    }

    /// Return the next element in this linked list (if any),
    /// or an error if the data is out of bounds or corrupt.
    pub fn try_next(&mut self) -> Result<Option<&'a [u8]>, OutaBounds> {
        if self.next == 0 {
            return Ok(None);
        }
        let next_start = read_u64(
            self.data,
            val::NEXT_POINTER_OFFSET
                .checked_add(self.next)
                .ok_or(OutaBounds)?,
        )?;
        let payload_len = read_u64(
            self.data,
            val::PAYLOAD_LEN_OFFSET
                .checked_add(self.next)
                .ok_or(OutaBounds)?,
        )?;
        let ret = get_range_dynamic(
            self.data,
            val::PAYLOAD_START
                .checked_add(self.next)
                .ok_or(OutaBounds)?,
            payload_len,
        )?;
        self.next = next_start;
        Ok(Some(ret))
    }
}

fn get_range<const N: usize>(data: &[u8], start: u64) -> Result<&[u8; N], OutaBounds> {
    let start = start as usize;
    let end = start.checked_add(N).ok_or(OutaBounds)?;
    let slice = data.get(start..end).ok_or(OutaBounds)?;
    Ok(slice.try_into().expect("slice has length N"))
}

fn get_range_dynamic(data: &[u8], start: u64, len: u64) -> Result<&[u8], OutaBounds> {
    let start = start as usize;
    let len = len as usize;
    let end = start.checked_add(len).ok_or(OutaBounds)?;
    data.get(start..end).ok_or(OutaBounds)
}

fn read_u64(data: &[u8], start: u64) -> Result<u64, OutaBounds> {
    let raw = get_range::<8>(data, start)?;
    Ok(u64::from_be_bytes(*raw))
}

fn write_u64(data: &mut [u8], start: u64, value: u64) -> Result<(), OutaBounds> {
    let start = start as usize;
    let end = start.checked_add(8).ok_or(OutaBounds)?;
    let bytes = value.to_be_bytes();
    let slice = data.get_mut(start..end).ok_or(OutaBounds)?;
    slice.copy_from_slice(&bytes);
    Ok(())
}

fn write_range(data: &mut [u8], start: u64, buf: &[u8]) -> Result<(), OutaBounds> {
    let start = start as usize;
    let end = start.checked_add(buf.len()).ok_or(OutaBounds)?;
    let slice = data.get_mut(start..end).ok_or(OutaBounds)?;
    slice.copy_from_slice(buf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use stor::write_store_len;
    use stor::write_num_slots;

    use super::*;

    #[test]
    fn single_key() {
        let mut buf = [0u8; 112];
        write_num_slots(&mut buf, 4).unwrap();
        write_store_len(&mut buf, 0).unwrap();

        let mut quack = Quack::new(buf);

        quack.write(0, b"hello").unwrap();
        quack.write(0, b"world").unwrap();

        let items = quack.read(0).unwrap().collect::<Vec<_>>();
        assert_eq!(&items[..], &[b"world", b"hello"]);
    }

    #[test]
    fn multiple_keys() {
        let mut buf = [0u8; 128];
        write_num_slots(&mut buf, 4).unwrap();
        write_store_len(&mut buf, 0).unwrap();

        let mut quack = Quack::new(buf);
        quack.write(0, b"hello").unwrap();
        quack.write(1, b"world").unwrap();
        quack.write(2, b"quack").unwrap();

        assert_eq!(&quack.read(0).unwrap().collect::<Vec<_>>(), &[b"hello"]);
        assert_eq!(&quack.read(1).unwrap().collect::<Vec<_>>(), &[b"world"]);
        assert_eq!(&quack.read(2).unwrap().collect::<Vec<_>>(), &[b"quack"]);
    }

    #[test]
    fn miss() {
        let mut buf = [0u8; 69];
        write_num_slots(&mut buf, 4).unwrap();
        write_store_len(&mut buf, 0).unwrap();

        let mut quack = Quack::new(&mut buf);

        println!("quack: {:?}", quack.data);
        quack.write(0, b"hello").unwrap();

        assert!(quack.read(1).unwrap().next().is_none());
    }
}
