use std::collections::HashSet;
use std::path::PathBuf;
use cid::Cid;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{rng, Rng};
use tempfile::tempdir;
use blockstore::block::make_random_block;
use blockstore::blockstore::{FSStore, Blockstore};

const BLOCK_SIZE: usize = 65536;
const N_OPS: usize = 10000;
const RW_RATIO: f64 = 0.5;
const N_THREADS: usize = 80;

async fn random_rw_bench() {
    let threshold = i32::MAX / ((1.0 / RW_RATIO) as i32);
    let mut existing: HashSet<Cid> = HashSet::new();
    let root = tempdir().unwrap();
    let store = FSStore::create(PathBuf::from(root.path())).await.unwrap();

    for val in rng().random_iter::<i32>().take(N_OPS) {
        if val < threshold && existing.len() > 0 {
            let anyblock = existing.iter().next().unwrap().clone();
            store.del_block(&anyblock).await.unwrap();
            existing.remove(&anyblock);
        } else {
            let block = make_random_block(BLOCK_SIZE);
            store.put_block(&block).await.unwrap();
            existing.insert(block.cid);
        }
    }
}

fn random_rw_bench_wrapper(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(N_THREADS)
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("Random RW Bench", |b| {
       b.to_async(&rt).iter(|| random_rw_bench())
    });
}

criterion_group!(benches, random_rw_bench_wrapper);
criterion_main!(benches);