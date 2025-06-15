use std::collections::{HashMap, HashSet, LinkedList};
use std::error::Error;
use std::fs::{create_dir_all, OpenOptions};
use std::io::{self, Write};
use std::iter::FromIterator;
use std::path::Path;

use byteorder::{ByteOrder, BE};
use bytes::BytesMut;
use log::{error, info, warn};
use prost::Message;
use rand::Rng;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, FlushOptions, IteratorMode, Options,
    ReadOptions, WriteBatch, WriteOptions,
};
use types::H256;

use chain::{BlockHeader, IndexedBlock, IndexedBlockHeader, IndexedTransaction, Transaction};
use proto::chain::ContractType;

pub type BoxError = Box<dyn Error>;

#[derive(Debug)]
pub enum CheckResult {
    Ok,
    ForkAt(u64),
    BreakAt(u64),
}

pub struct ChainDB {
    db: DB,
}

impl Drop for ChainDB {
    fn drop(&mut self) {
        info!("chain-db closed successfully");
    }
}

impl ChainDB {
    pub fn new<P: AsRef<Path>>(db_path: P) -> ChainDB {
        create_dir_all(&db_path).expect("create db directory");

        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.increase_parallelism(num_cpus::get() as i32);
        db_options.set_max_open_files(1024);

        let column_families = vec![
            ColumnFamilyDescriptor::new(
                "default",
                {
                    let mut opts = Options::default();
                    opts.set_num_levels(2);
                    opts.set_compression_type(rocksdb::DBCompressionType::None);
                    opts
                }
            ),
            // block_hash => BlockHeader
            ColumnFamilyDescriptor::new(
                "block-header",
                {
                    let mut opts = Options::default();
                    opts.set_max_write_buffer_number(6);
                    opts
                }
            ),
            // [block_hash, transaction_index: u64, transaction_hash] => Transaction
            ColumnFamilyDescriptor::new(
                "transaction",
                {
                    let mut opts = Options::default();
                    opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(32));
                    opts.set_max_write_buffer_number(6);
                    opts
                }
            ),
            // transaction_hash => [block_hash, transaction_index: u64]
            // Key and value lengths are fixed
            ColumnFamilyDescriptor::new(
                "transaction-block",
                {
                    let mut opts = Options::default();
                    opts.set_max_write_buffer_number(6);
                    opts
                }
            ),
        ];

        let db = DB::open_cf_descriptors(&db_options, db_path, column_families).unwrap();

        ChainDB { db }
    }

    fn default_cf(&self) -> &ColumnFamily {
        self.db.cf_handle("default").expect("default column family should exist")
    }

    fn block_header_cf(&self) -> &ColumnFamily {
        self.db.cf_handle("block-header").expect("block-header column family should exist")
    }

    fn transaction_cf(&self) -> &ColumnFamily {
        self.db.cf_handle("transaction").expect("transaction column family should exist")
    }

    fn transaction_block_cf(&self) -> &ColumnFamily {
        self.db.cf_handle("transaction-block").expect("transaction-block column family should exist")
    }

    pub fn reset_node_id(&self) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let mut node_id = vec![b'A'; 64];
        rng.fill(&mut node_id[32..]);
        self.db
            .put_cf(self.default_cf(), b"NODE_ID", &node_id)
            .unwrap();
        node_id
    }

    pub fn get_node_id(&self) -> Vec<u8> {
        if let Ok(Some(node_id)) = self.db.get_cf(self.default_cf(), b"NODE_ID") {
            node_id
        } else {
            self.reset_node_id()
        }
    }

    pub fn get_block_height(&self) -> i64 {
        self.db
            .get_cf(self.default_cf(), b"BLOCK_HEIGHT")
            .ok()
            .flatten()
            .map(|val| BE::read_u64(&val) as i64)
            .unwrap_or(0)
    }

    pub fn update_block_height(&self, height: i64) {
        assert!(height >= 0);
        if height > self.get_block_height() {
            let mut val = [0u8; 8];
            BE::write_u64(&mut val, height as u64);
            self.db
                .put_cf(self.default_cf(), b"BLOCK_HEIGHT", &val)
                .unwrap();
        }
    }

    pub fn force_update_block_height(&self, height: i64) -> Result<(), BoxError> {
        let mut val = [0u8; 8];
        BE::write_u64(&mut val, height as u64);
        self.db
            .put_cf(self.default_cf(), b"BLOCK_HEIGHT", &val)
            .map_err(From::from)
    }

    /// Highest block id, counted from 0
    pub fn highest_block(&self) -> Result<IndexedBlock, BoxError> {
        self.get_block_by_number(self.get_block_height() as u64)
    }

    pub fn insert_block(&self, block: &IndexedBlock) -> Result<(), Box<dyn Error>> {
        let mut batch = WriteBatch::default();

        let mut buf = BytesMut::with_capacity(block.header.raw.encoded_len());
        block.header.raw.encode(&mut buf)?;
        batch.put_cf(self.block_header_cf(), block.header.hash.as_bytes(), &buf);

        for (index, txn) in block.transactions.iter().enumerate() {
            buf.clear();
            txn.raw.encode(&mut buf)?;

            // [block_hash, transaction_index: u64, transaction_hash] => Transaction
            let mut idx_key = [0u8; 8];
            BE::write_u64(&mut idx_key[..], index as u64);

            let mut composite_key = Vec::with_capacity(32 + 8 + 32);
            composite_key.extend_from_slice(block.hash().as_bytes());
            composite_key.extend_from_slice(&idx_key);
            composite_key.extend_from_slice(txn.hash.as_bytes());

            batch.put_cf(self.transaction_cf(), &composite_key, &buf);

            // reverse index
            // transaction_hash => [block_hash, transaction_index: u64]
            let mut reverse_value = Vec::with_capacity(32 + 8);
            reverse_value.extend_from_slice(block.hash().as_bytes());
            reverse_value.extend_from_slice(&idx_key);

            batch.put_cf(self.transaction_block_cf(), txn.hash.as_bytes(), &reverse_value);
        }

        self.db.write(batch)?;
        Ok(())
    }

    pub fn has_block_id(&self, id: &H256) -> bool {
        self.db
            .get_cf(self.block_header_cf(), id.as_bytes())
            .map(|opt| opt.is_some())
            .unwrap_or(false)
    }

    pub fn has_block(&self, block: &IndexedBlock) -> bool {
        self.has_block_id(&block.header.hash)
    }

    pub fn has_block_number(&self, num: u64) -> bool {
        let mut lower_bound = [0u8; 32];
        BE::write_u64(&mut lower_bound[..8], num);

        let iter = self.db.iterator_cf(
            self.block_header_cf(),
            IteratorMode::From(&lower_bound, rocksdb::Direction::Forward)
        );

        iter.filter_map(|result| result.ok())
            .take_while(|(key, _)| key.starts_with(&lower_bound[..8]))
            .count() > 0
    }

    pub fn get_block_from_header(&self, header: IndexedBlockHeader) -> Result<IndexedBlock, BoxError> {
        let iter = self.db.iterator_cf(
            self.transaction_cf(),
            IteratorMode::From(header.hash.as_bytes(), rocksdb::Direction::Forward)
        );

        let transactions = iter
            .filter_map(|result| result.ok())
            .take_while(|(key, _)| key.starts_with(header.hash.as_bytes()))
            .map(|(key, val)| {
                let txn = Transaction::decode(&*val)?;
                Ok(IndexedTransaction::new(H256::from_slice(&key[32 + 8..]), txn))
            })
            .collect::<Result<Vec<_>, BoxError>>();

        transactions.map(|txns| IndexedBlock::new(header, txns))
    }

    pub fn get_block_transactions(&self, hash: &H256) -> Result<Vec<IndexedTransaction>, BoxError> {
        let iter = self.db.iterator_cf(
            self.transaction_cf(),
            IteratorMode::From(hash.as_bytes(), rocksdb::Direction::Forward)
        );

        let txns = iter
            .filter_map(|result| result.ok())
            .take_while(|(key, _)| key.starts_with(hash.as_bytes()))
            .map(|(key, val)| {
                let txn = Transaction::decode(&*val)?;
                Ok(IndexedTransaction::new(H256::from_slice(&key[32 + 8..]), txn))
            })
            .collect::<Result<Vec<_>, BoxError>>();
        txns
    }

    pub fn get_transaction_hashes_by_block_number(&self, num: i64) -> Result<Vec<H256>, BoxError> {
        let mut lower_bound = [0u8; 8];
        BE::write_u64(&mut lower_bound[..], num as u64);
        let mut upper_bound = [0u8; 8];
        BE::write_u64(&mut upper_bound[..], num as u64 + 1);

        let iter = self.db.iterator_cf(
            self.transaction_cf(),
            IteratorMode::From(&lower_bound, rocksdb::Direction::Forward)
        );

        let txn_hashes = iter
            .filter_map(|result| result.ok())
            .take_while(|(key, _)| &key[..8] < &upper_bound[..])
            .map(|(key, _)| Ok(H256::from_slice(&key[32 + 8..])))
            .collect::<Result<Vec<_>, BoxError>>();
        txn_hashes
    }

    pub fn get_transaction_hashes_by_block_hash(&self, hash: &H256) -> Result<Vec<H256>, BoxError> {
        let iter = self.db.iterator_cf(
            self.transaction_cf(),
            IteratorMode::From(hash.as_bytes(), rocksdb::Direction::Forward)
        );

        let txn_hashes = iter
            .filter_map(|result| result.ok())
            .take_while(|(key, _)| key.starts_with(hash.as_bytes()))
            .map(|(key, _)| Ok(H256::from_slice(&key[32 + 8..])))
            .collect::<Result<Vec<_>, BoxError>>();
        txn_hashes
    }

    pub fn get_block_header_by_number(&self, num: i64) -> Result<IndexedBlockHeader, BoxError> {
        let mut headers = self.get_block_headers_by_number(num as u64);
        if headers.is_empty() {
            return Err(Box::new(io::Error::new(io::ErrorKind::Other, "not found")));
        }
        if headers.len() != 1 {
            return Err(Box::new(io::Error::new(io::ErrorKind::Other, "chain fork")));
        }
        let header = headers.pop().unwrap();
        Ok(header)
    }

    pub fn get_block_header(&self, hash: &H256) -> Result<IndexedBlockHeader, BoxError> {
        self.db
            .get_cf(self.block_header_cf(), hash.as_bytes())
            .map_err(From::from)
            .and_then(|opt| opt.ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "block header not found")) as BoxError))
            .and_then(|raw_header| BlockHeader::decode(&*raw_header).map_err(From::from))
            .map(|header| IndexedBlockHeader::new(hash.clone(), header))
    }

    /// handles fork
    pub fn get_block_headers_by_number(&self, num: u64) -> Vec<IndexedBlockHeader> {
        let mut lower_bound = [0u8; 32];
        BE::write_u64(&mut lower_bound[..8], num);
        let mut upper_bound = [0xff_u8; 32];
        BE::write_u64(&mut upper_bound[..8], num);

        let iter = self.db.iterator_cf(
            self.block_header_cf(),
            IteratorMode::From(&lower_bound, rocksdb::Direction::Forward)
        );

        iter
            .filter_map(|result| result.ok())
            .take_while(|(key, _)| &key[..8] == &lower_bound[..8])
            .map(|(key, val)| IndexedBlockHeader::new(H256::from_slice(&key), BlockHeader::decode(&*val).unwrap()))
            .collect()
    }

    pub fn get_block_by_number(&self, num: u64) -> Result<IndexedBlock, BoxError> {
        let mut lower_bound = [0u8; 32];
        BE::write_u64(&mut lower_bound[..8], num);

        let iter = self.db.iterator_cf(
            self.block_header_cf(),
            IteratorMode::From(&lower_bound, rocksdb::Direction::Forward)
        );

        let found: Vec<(Vec<u8>, Vec<u8>)> = iter
            .filter_map(|result| result.ok())
            .take_while(|(key, _)| &key[..8] == &lower_bound[..8])
            .map(|(key, val)| (key.to_vec(), val.to_vec()))
            .collect();

        if found.is_empty() {
            return Err(Box::new(io::Error::new(io::ErrorKind::NotFound, "block not found")));
        }
        if found.len() > 1 {
            eprintln!("multiple blocks found for same number: {}", num);
            for item in &found {
                eprintln!("  => {}", hex::encode(&item.0));
                eprintln!("  => {}", hex::encode(&item.1));
            }
            return Err(Box::new(io::Error::new(io::ErrorKind::Other, "fork found")));
        }

        let header = IndexedBlockHeader::new(H256::from_slice(&found[0].0), BlockHeader::decode(&*found[0].1)?);
        self.get_block_from_header(header)
    }

    pub fn get_block_by_hash(&self, hash: &H256) -> Result<IndexedBlock, BoxError> {
        self.get_block_by_id(hash)
    }

    pub fn get_block_by_id(&self, id: &H256) -> Result<IndexedBlock, BoxError> {
        self.db
            .get_cf(self.block_header_cf(), id.as_bytes())
            .map_err(From::from)
            .and_then(|opt| opt.ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "block header not found")) as BoxError))
            .and_then(|raw_header| BlockHeader::decode(&*raw_header).map_err(From::from))
            .map(|header| IndexedBlockHeader::new(id.clone(), header))
            .and_then(|header| self.get_block_from_header(header))
    }

    pub fn get_genesis_block(&self) -> Result<IndexedBlock, BoxError> {
        self.get_block_by_number(0)
    }

    pub fn get_transaction_by_id(&self, id: &H256) -> Result<IndexedTransaction, BoxError> {
        let mut key = self.db
            .get_cf(self.transaction_block_cf(), id.as_bytes())?
            .ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "transaction block mapping not found")) as BoxError)?;
        key.extend_from_slice(id.as_bytes());

        let raw = self.db
            .get_cf(self.transaction_cf(), &key)?
            .ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "transaction not found")) as BoxError)?;

        let txn = Transaction::decode(&*raw)
            .map(|txn| IndexedTransaction::new(id.clone(), txn))?;
        Ok(txn)
    }

    pub fn get_transaction_index(&self, id: &H256) -> Result<i32, BoxError> {
        let key = self.db
            .get_cf(self.transaction_block_cf(), id.as_bytes())?
            .ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "transaction block mapping not found")) as BoxError)?;
        Ok(BE::read_u64(&key[32..]) as i32)
    }

    pub fn get_transaction_block_hash(&self, id: &H256) -> Result<H256, BoxError> {
        let key = self.db
            .get_cf(self.transaction_block_cf(), id.as_bytes())?
            .ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "transaction block mapping not found")) as BoxError)?;
        Ok(H256::from_slice(&key[..32]))
    }

    pub fn get_block_header_by_transaction_hash(&self, txn_hash: &H256) -> Result<IndexedBlockHeader, BoxError> {
        let block_key = self.db
            .get_cf(self.transaction_block_cf(), txn_hash.as_bytes())?
            .ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "transaction block mapping not found")) as BoxError)?;

        let raw = self.db
            .get_cf(self.block_header_cf(), &block_key[..32])?
            .ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "block header not found")) as BoxError)?;

        let header = BlockHeader::decode(&*raw)?;
        Ok(IndexedBlockHeader::new(H256::from_slice(&block_key[..32]), header))
    }

    pub fn delete_transaction(&self, txn: &IndexedTransaction, wb: &mut WriteBatch) -> Result<(), BoxError> {
        let block_key = self.db
            .get_cf(self.transaction_block_cf(), txn.hash.as_bytes())?
            .ok_or_else(|| Box::new(io::Error::new(io::ErrorKind::NotFound, "transaction block mapping not found")) as BoxError)?;

        if self.db.get_cf(self.block_header_cf(), &block_key[..32])?.is_none() {
            let mut composite_key = Vec::with_capacity(block_key.len() + 32);
            composite_key.extend_from_slice(&block_key);
            composite_key.extend_from_slice(txn.hash.as_bytes());

            wb.delete_cf(self.transaction_cf(), &composite_key);
            wb.delete_cf(self.transaction_block_cf(), txn.hash.as_bytes());
            return Ok(());
        }

        Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transaction is linked to a block, please delete the block first",
        )))
    }

    // TODO: Implement delete_block_by_number after completing basic migration
    pub fn delete_block_by_number(&self, _num: u64) -> Result<(), BoxError> {
        unimplemented!("delete_block_by_number - to be implemented after basic migration")
    }

    // TODO: Implement delete_block after completing basic migration
    pub fn delete_block(&self, _block: &IndexedBlock) -> bool {
        unimplemented!("delete_block - to be implemented after basic migration")
    }

    // TODO: Implement these helper methods after completing basic migration
    fn delete_block_without_reverse_index(&self, _block: &IndexedBlock, _wb: &mut WriteBatch) {
        unimplemented!("delete_block_without_reverse_index - to be implemented after basic migration")
    }

    fn relink_transactions_to_block(&self, _block: &IndexedBlock, _wb: &mut WriteBatch) {
        unimplemented!("relink_transactions_to_block - to be implemented after basic migration")
    }

    // TODO: Implement block_hashes_from after completing basic migration
    pub fn block_hashes_from(&self, _start_block_hash: &[u8], _count: usize) -> Vec<Vec<u8>> {
        unimplemented!("block_hashes_from - to be implemented after basic migration")
    }

    // TODO: Implement handle_chain_fork_at after completing basic migration
    pub fn handle_chain_fork_at(&self, _num: u64, _dry_run: bool) -> Result<(), BoxError> {
        unimplemented!("handle_chain_fork_at - to be implemented after basic migration")
    }

    // TODO: Implement these iterator methods after completing basic migration
    pub fn visit(&self) -> Result<(), Box<dyn Error>> {
        unimplemented!("visit - to be implemented after basic migration")
    }

    pub fn block_headers<'a>(&'a self) -> impl Iterator<Item = IndexedBlockHeader> + 'a {
        std::iter::empty() // Temporary stub
    }

    pub fn blocks<'a>(&'a self) -> impl Iterator<Item = IndexedBlock> + 'a {
        std::iter::empty() // Temporary stub
    }

    // TODO: Implement these methods after completing basic migration
    pub fn ref_block_hashes_of_block_num(&self, _num: i64) -> Vec<H256> {
        unimplemented!("ref_block_hashes_of_block_num - to be implemented after basic migration")
    }

    pub fn get_parent_hash_verified_block_number(&self) -> u64 {
        self.db
            .get_cf(self.default_cf(), b"PARENT_HASH_VERIFIED")
            .ok()
            .flatten()
            .map(|raw| BE::read_u64(&raw))
            .unwrap_or(0)
    }

    pub fn update_parent_hash_verified_block_number(&self, num: u64) -> Result<(), BoxError> {
        let mut raw = [0u8; 8];
        BE::write_u64(&mut raw[..], num);
        self.db
            .put_cf(self.default_cf(), b"PARENT_HASH_VERIFIED", &raw)
            .map_err(From::from)
    }

    // TODO: Implement these verification methods after completing basic migration
    pub fn verify_parent_hashes(&self) -> Result<CheckResult, BoxError> {
        unimplemented!("verify_parent_hashes - to be implemented after basic migration")
    }

    pub fn get_merkle_tree_verified_block_number(&self) -> u64 {
        self.db
            .get_cf(self.default_cf(), b"MERKLE_TREE_VERIFIED")
            .ok()
            .flatten()
            .map(|raw| BE::read_u64(&raw))
            .unwrap_or(0)
    }

    pub fn update_merkle_tree_verified_block_number(&self, num: u64) -> Result<(), BoxError> {
        let mut raw = [0u8; 8];
        BE::write_u64(&mut raw[..], num);
        self.db
            .put_cf(self.default_cf(), b"MERKLE_TREE_VERIFIED", &raw)
            .map_err(From::from)
    }

    // TODO: Implement these advanced methods after completing basic migration
    pub fn verify_merkle_tree(&self, _patch: &HashMap<H256, H256>) -> Result<bool, Box<dyn Error>> {
        unimplemented!("verify_merkle_tree - to be implemented after basic migration")
    }

    pub fn get_db_property(&self, _key: &str) -> u64 {
        unimplemented!("get_db_property - to be implemented after basic migration")
    }

    pub fn get_accumulated_db_property(&self, _key: &str) -> u64 {
        unimplemented!("get_accumulated_db_property - to be implemented after basic migration")
    }

    pub fn report_status(&self) {
        unimplemented!("report_status - to be implemented after basic migration")
    }

    pub fn await_background_jobs(&self) {
        unimplemented!("await_background_jobs - to be implemented after basic migration")
    }

    pub fn compact_db(&self) -> Result<(), BoxError> {
        unimplemented!("compact_db - to be implemented after basic migration")
    }

    pub unsafe fn prepare_close(&self) {
        info!("flush db ... {:?}", self.db.flush());
        info!("cancel background work ...");
        self.db.cancel_all_background_work(true);
        // Note: sync_wal method not available in rust-rocksdb 0.23
        // info!("syncing WAL ... {:?}", self.db.sync_wal());
    }
}
