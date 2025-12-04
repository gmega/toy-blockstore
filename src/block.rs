use cid::Cid;
use multihash::{Error, Multihash};
use sha2::{Digest, Sha256};

const SHA2_256: u64 = 0x12;

#[derive(Debug)]
pub struct Block {
    pub cid: Cid,
    pub data: Vec<u8>,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Result<Block, Error> {
        let digest = Sha256::digest(&data);
        let multihash = Multihash::wrap(SHA2_256, digest.as_slice())?;
        Ok(Block { cid: Cid::new_v1(SHA2_256, multihash), data })
    }
}

impl PartialEq<Self> for Block {
    fn eq(&self, other: &Self) -> bool {
        self.cid == other.cid
    }
}

#[cfg(test)]
pub mod tests {
    use rand::RngCore;
    use super::*;

    pub fn make_random_block(size: usize) -> Block {
        let mut data = vec![0u8; size];
        rand::rng().fill_bytes(&mut data);
        Block::new(data).unwrap()
    }

    #[test]
    pub fn should_evaluate_equal_blocks_as_equal() {
        let block1 = make_random_block(10);
        let block2 = Block::new(block1.data.clone()).unwrap();

        assert_eq!(block1, block2);
    }

    #[test]
    pub fn should_not_evaluate_different_blocks_as_not_equal() {
        let block1 = make_random_block(10);
        let block2 = make_random_block(10);

        assert_ne!(block1, block2);
    }
}