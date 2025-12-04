use std::fs::{create_dir_all, File};
use std::{fs, io};
use std::io::Write;
use std::path::PathBuf;

use crate::block::Block;
use cid::Cid;

pub trait Blockstore {
    fn put_block(&self, block: &Block) -> impl Future<Output = Result<(), io::Error>> + Send;
    fn has_block(&self, cid: &Cid) -> impl Future<Output = bool> + Send;
    fn get_block(&self, cid: &Cid) -> impl Future<Output = Result<Option<Block>, io::Error>> + Send;
    fn del_block(&self, cid: &Cid) -> impl Future<Output = Result<(), io::Error>> + Send;
}

pub struct FSStore {
    root: PathBuf,
    chars_per_level: usize,
}

const DEFAULT_CHARS_PER_LEVEL: usize = 15;

impl FSStore {
    pub async fn create(root: PathBuf) -> Result<Self, io::Error> {
        let root_ref = &root;
        if !root_ref.exists() {
            create_dir_all(root_ref)?
        }

        Ok(FSStore {
            root,
            chars_per_level: DEFAULT_CHARS_PER_LEVEL,
        })
    }

    pub fn block_path_raw(chars_per_level: usize, cid: &Cid) -> PathBuf {
        // This is a bit ugly but chunks only works on slices and I was feeling lazy. :-)
        let parts: Vec<String> = format!("{}", cid)
            .as_bytes()
            .chunks(chars_per_level)
            .map(|chunk| str::from_utf8(&chunk).unwrap().to_string())
            .collect();

        parts.iter().collect()
    }

    pub fn block_path(&self, cid: &Cid) -> PathBuf {
        let rawpath = Self::block_path_raw(self.chars_per_level, &cid);
        self.root.join(rawpath)
    }
}

impl Drop for FSStore {
    fn drop(&mut self) {}
}

impl Blockstore for FSStore {
    async fn put_block(&self, block: &Block) -> Result<(), io::Error> {
        let block_path = self.block_path(&block.cid);
        let block_dir = block_path.parent().unwrap(); // should always have a parent

        // This is thread-safe, as per
        // https://doc.rust-lang.org/stable/std/fs/fn.create_dir_all.html
        create_dir_all(&block_dir)?;

        // This is not thread-safe, and might cause a block to be corrupted.
        let mut file = File::create(&block_path)?;
        file.write_all(&block.data)?;

        Ok(())
    }

    async fn has_block(&self, cid: &Cid) -> bool {
        self.block_path(cid).exists()
    }

    async fn get_block(&self, cid: &Cid) -> Result<Option<Block>, io::Error> {
        let block_path = self.block_path(&cid);
        let contents = fs::read(block_path)?;

        match Block::new(contents) {
            Ok(block) => Ok(Some(block)),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    async fn del_block(&self, cid: &Cid) -> Result<(), io::Error> {
        let block_path = self.block_path(&cid);
         fs::remove_file(&block_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::min;
    use std::fs;
    use tempfile::{tempdir, TempDir};
    use crate::block::make_random_block;

    pub async fn make_fs_store() -> (FSStore, TempDir) {
        let tempdir = tempdir().unwrap();
        (
            FSStore::create(PathBuf::from(tempdir.path()))
                .await
                .unwrap(),
            tempdir,
        )
    }

    #[test]
    fn should_compute_correct_block_path() {
        let block = make_random_block(1_000);
        let cid_str = format!("{}", &block.cid);
        let cpl = DEFAULT_CHARS_PER_LEVEL;
        let block_path = FSStore::block_path_raw(cpl, &block.cid);

        for (i, component) in block_path.components().enumerate() {
            assert_eq!(
                &cid_str[i * cpl..min(cid_str.len(), (i + 1) * cpl)],
                component.as_os_str().to_str().unwrap()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn should_put_block() {
        let (store, _) = make_fs_store().await;
        let block = make_random_block(1_000);

        store.put_block(&block).await.unwrap();

        let path = store.block_path(&block.cid);
        assert_eq!(fs::read(path).unwrap(), block.data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn should_get_block() {
        let (store, _) = make_fs_store().await;
        let stored = make_random_block(1_000);

        store.put_block(&stored).await.unwrap();
        let retrieved = store.get_block(&stored.cid).await.unwrap().unwrap();
        assert_eq!(stored, retrieved);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn should_contain_stored_blocks() {
        let (store, _) = make_fs_store().await;
        let block = make_random_block(1_000);

        assert!(!store.has_block(&block.cid).await);
        store.put_block(&block).await.unwrap();
        assert!(store.has_block(&block.cid).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn should_delete_block() {
        let (store, _) = make_fs_store().await;
        let block = make_random_block(1_000);

        store.put_block(&block).await.unwrap();
        assert!(store.has_block(&block.cid).await);
        store.del_block(&block.cid).await.unwrap();
        assert!(!store.has_block(&block.cid).await);

        let path = store.block_path(&block.cid);
        assert!(!path.exists());
    }
}
