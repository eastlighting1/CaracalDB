use crc32fast::Hasher;

pub const MAGIC: &[u8; 8] = b"CRCL\0\0\0\x01";
pub const FORMAT_VERSION: u32 = 1;
pub const DEFAULT_PAGE_SIZE: u32 = 16 * 1024;
pub const HEADER_SIZE: usize = 24;

pub fn header_crc32(magic: &[u8; 8], version: u32, page_size: u32, flags: u32) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(magic);
    hasher.update(&version.to_le_bytes());
    hasher.update(&page_size.to_le_bytes());
    hasher.update(&flags.to_le_bytes());
    hasher.finalize()
}

pub fn pack_header() -> [u8; HEADER_SIZE] {
    let flags = 0u32;
    let crc = header_crc32(MAGIC, FORMAT_VERSION, DEFAULT_PAGE_SIZE, flags);
    let mut out = [0u8; HEADER_SIZE];
    out[0..8].copy_from_slice(MAGIC);
    out[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    out[12..16].copy_from_slice(&DEFAULT_PAGE_SIZE.to_le_bytes());
    out[16..20].copy_from_slice(&flags.to_le_bytes());
    out[20..24].copy_from_slice(&crc.to_le_bytes());
    out
}
