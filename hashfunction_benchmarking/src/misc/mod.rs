use hash_lib::{self, Hasher};

pub fn get_hash_functions() -> Vec<Box<dyn hash_lib::Hasher>> {
    let funcs: Vec<Box<dyn Hasher>> = vec![
        Box::new(hash_lib::crc16::CRC16::new()),
        Box::new(hash_lib::crc32::CRC32::new()),
        Box::new(hash_lib::crc64::CRC64::new()),
        Box::new(hash_lib::blake2b::Blake2B::new()),
        Box::new(hash_lib::md5::MD5::new()),
        Box::new(hash_lib::murmur_hash3::MurMurHash3::new()),
        Box::new(hash_lib::ripemd160::Ripemd160::new()),
        Box::new(hash_lib::sha3::SHA3::new()),
        Box::new(hash_lib::sha512::SHA512::new()),
        Box::new(hash_lib::tigerhash::TigerHash::new()),
        Box::new(hash_lib::xx_hash64::XXHash64::new())
    ];
    
    return funcs;
}