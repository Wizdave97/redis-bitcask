pub mod server_helpers;

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::{BufReader, BufWriter};
use std::io::{ErrorKind, Error};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::io::Result;
use std::collections::HashMap;
use std::usize;
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crc::crc32;
use serde_derive::{Deserialize, Serialize};



type ByteString = Vec<u8>;


#[derive(Serialize, Deserialize, Debug)]
pub struct KeyValuePair {
    key: ByteString,
    value: ByteString
}
pub struct AKVMEM {
    f: File,
    pub index: HashMap<ByteString, u64>
}

pub fn open(path: &Path) -> Result<AKVMEM> {
    let f = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .append(true)
                                .open(path)?;
    Ok(AKVMEM {
        f,
        index: HashMap::new()
    })
}

pub fn process_record<R: Read>(f: &mut R) -> Result<KeyValuePair>{
    let saved_checksum = f.read_u32::<LittleEndian>()?;
    let key_len = f.read_u32::<LittleEndian>()?;
    let val_len = f.read_u32::<LittleEndian>()?;

    let data_len = key_len + val_len;

    let mut data = ByteString::with_capacity(data_len as usize);

    {
        f.by_ref()
        .take(data_len as u64)
        .read_to_end(&mut data)?;
    }

    debug_assert_eq!(data.len(), data_len as usize);

    let checksum = crc32::checksum_ieee(&data);
    if checksum != saved_checksum {
        panic!("Data corruption detected, saved_checksum -> {:08x} != calculated_checksum -> {:08x}", saved_checksum, checksum)
    }
    
    let value = data.split_off(key_len as usize);
    data.resize(key_len as usize, 0);

    Ok(KeyValuePair{key: data, value})
}

impl AKVMEM {
    pub fn load(&mut self) -> Result<()>{
        let mut buf = BufReader::new(&self.f);

        loop {
            let current_position = buf.seek(SeekFrom::Current(0))?;

            let maybe_kv = process_record(&mut buf);

            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        break;
                    }
                    else  {
                        return Err(err)
                    }
                }
            };

            self.index.insert(kv.key, current_position);
        }
        Ok(())
    }
    pub fn seek_to_end(&mut self) -> u64 {
        self.f.seek(SeekFrom::End(0)).unwrap()
    }

    pub fn insert_ignoring_index(&mut self, key: &ByteString, value: &ByteString) -> Result<u64> {
        let mut f = BufWriter::new(&mut self.f);
        let key = key.to_vec();
        let value = value.to_vec();

        let mut tmp = Vec::<u8>::with_capacity(key.len() + value.len());
        tmp.extend(key.iter());
        tmp.extend(value.iter());
    
        let checksum = crc32::checksum_ieee(&tmp);

        let next_byte = SeekFrom::End(0);

        let current_position = f.seek(SeekFrom::Current(0))?;

        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key.len()  as u32)?;
        f.write_u32::<LittleEndian>(value.len() as u32)?;
        f.write_all(&tmp)?;
        f.flush()?;
        Ok(current_position)
    }

    pub fn get(&self, key: &ByteString) -> Result<Option<ByteString>> {
        let kv = {
            if let Some(position) = self.index.get(key) {
             Some(self.get_at(*position)?.value)
            }
            else  { None }
        };
        Ok(kv)
    }
    
    pub fn get_at(&self, position: u64) -> Result<KeyValuePair> {
        let mut buf = BufReader::new(&self.f);
        buf.seek(SeekFrom::Start(position))?;
        let kv = process_record(&mut buf)?;
        Ok(kv)
    }

    pub fn insert(&mut self, key: &ByteString, value: &ByteString) -> Result<()>{
        let current_position = self.insert_ignoring_index(key, value)?;
        self.index.insert(key.clone(), current_position);
        Ok(())
    }

    #[inline]
    pub fn update(&mut self, key: &ByteString, value: &ByteString) -> Result<()>{
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteString) -> Result<()>{
        self.index.remove(key);
        self.insert(key, b"".to_vec().as_ref())
    }

    pub fn find(&mut self, target: &ByteString) -> Result<Option<(u64, ByteString)>> {
        let mut r = BufReader::new(&self.f);
        r.seek(SeekFrom::Start(0))?;
        loop {
            let current_position = r.seek(SeekFrom::Current(0))?;

            let maybe_kv = process_record(&mut r);

            match maybe_kv {
                Ok(kv) => {
                    if kv.value == *target {
                        if self.index.values().collect::<Vec<&u64>>().iter().any(|pos| **pos == current_position) {
                            return Ok(Some((current_position, kv.value)))
                        }
                    }
                }
                Err(err) => {
                    match err.kind() {
                        ErrorKind::UnexpectedEof => break,
                        _ => return Err(Error::new(ErrorKind::NotFound, "Unexpected error while searching database"))
                    }
                    
                }
            }

        }
        Ok(None)

    }
}