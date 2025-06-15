//! The state-db implementation.

use std::collections::VecDeque;
use std::io;
use std::iter;
use std::path::Path;

use ::keys::Address;
use config::genesis::GenesisConfig;
use config::ChainConfig;
use log::info;
use proto::common::AccountType;
use proto::state as state_pb;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, IteratorMode, Options,
    WriteBatch,
};

use super::keys;
use super::parameter::default_parameters_from_config;
use super::DynamicProperty;

pub type BoxError = Box<dyn ::std::error::Error>;

pub struct OverlayWriteBatch {
    wb: WriteBatch,
    // TODO: Re-implement caching after basic migration is complete
    // For now, we'll disable caching to get the basic functionality working
}

// SAFETY: WriteBatch is safe to send between threads in our use case because:
// 1. It's only used within controlled database operations
// 2. The raw pointers are managed by rocksdb library
// 3. We don't share mutable references across thread boundaries
unsafe impl Send for OverlayWriteBatch {}
unsafe impl Sync for OverlayWriteBatch {}

impl std::ops::Deref for OverlayWriteBatch {
    type Target = WriteBatch;
    fn deref(&self) -> &Self::Target {
        &self.wb
    }
}

impl OverlayWriteBatch {
    pub fn new() -> Self {
        OverlayWriteBatch {
            wb: WriteBatch::new(),
        }
    }

    pub fn with_capacity(_cap: usize) -> Self {
        OverlayWriteBatch {
            wb: WriteBatch::new(), // Simplified for now
        }
    }

    pub fn put(&mut self, col: &ColumnFamily, key: &[u8], value: &[u8]) {
        self.wb.put_cf(col, key, value);
        // TODO: Add caching back after basic migration
    }

    pub fn delete(&mut self, col: &ColumnFamily, key: &[u8]) {
        self.wb.delete_cf(col, key);
        // TODO: Add caching back after basic migration
    }

    // Simplified methods - return NotFound for now since we disabled caching
    pub fn get(&self, _key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::new(io::ErrorKind::NotFound, "cache disabled"))
    }

    pub fn get_by_prefix(&self, _prefix: &[u8]) -> Option<Box<[u8]>> {
        None // Cache disabled
    }

    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        Box::new(iter::empty()) // Cache disabled
    }

    pub fn iter_with_prefix<'a>(
        &'a self,
        _prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a> {
        Box::new(iter::empty()) // Cache disabled
    }
}

pub struct OverlayDB {
    inner: DB,
    // Use push_back to add to the queue, and pop_front to remove from the queue.
    // push_back to add a new layer, pop_front to sync a layer to db, clear to discard all layers.
    layers: VecDeque<OverlayWriteBatch>,
}

impl OverlayDB {
    pub fn new(inner: DB) -> Self {
        OverlayDB {
            inner,
            // ceiling(27 - 27 * 70%) = 9
            layers: VecDeque::with_capacity(9),
        }
    }

    /// Fake `write` an OverlayWriteBath.
    pub fn write(&mut self, wb: OverlayWriteBatch) -> io::Result<()> {
        self.layers.push_back(wb);
        Ok(())
    }

    pub fn push_layer(&mut self, wb: OverlayWriteBatch) {
        self.layers.push_back(wb);
    }

    pub fn finalize_layers(&mut self) -> Result<(), BoxError> {
        for layer in self.layers.drain(..) {
            self.inner.write(layer.wb)?;
        }
        Ok(())
    }

    pub fn discard_layers(&mut self) -> io::Result<()> {
        self.layers.clear();
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&self, col: &ColumnFamily, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // TODO: Check layers when caching is re-enabled
        match self.inner.get_cf(col, key) {
            Ok(Some(val)) => Ok(Some(val)),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
        }
    }

    /// Get a value by key, skip top n layers.
    pub fn get_skipped(&self, _n: usize, col: &ColumnFamily, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // Simplified - just get from database for now
        self.get(col, key)
    }

    // TODO: Implement these methods properly after basic migration
    pub fn get_by_prefix(&self, _col: &ColumnFamily, _prefix: &[u8]) -> Option<Box<[u8]>> {
        None // Simplified for now
    }

    pub fn for_each<F>(&self, col: &ColumnFamily, mut func: F)
    where
        F: FnMut(&[u8], &[u8]) -> (),
    {
        // Simplified - just iterate over database
        let iter = self.inner.iterator_cf(col, IteratorMode::Start);
        for result in iter {
            if let Ok((key, value)) = result {
                func(&key, &value);
            }
        }
    }

    pub fn for_each_by_prefix<F>(&self, col: &ColumnFamily, prefix: &[u8], mut func: F)
    where
        F: FnMut(&[u8], &[u8]) -> (),
    {
        // Simplified - just iterate over database
        let iter = self.inner.iterator_cf(col, IteratorMode::From(prefix, rocksdb::Direction::Forward));
        for result in iter {
            if let Ok((key, value)) = result {
                if !key.starts_with(prefix) {
                    return;
                }
                func(&key, &value);
            }
        }
    }

    pub fn delete(&mut self, col: &ColumnFamily, key: &[u8]) -> io::Result<()> {
        let wb = self
            .layers
            .back_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no db layers found"))?;
        wb.delete(col, key);
        Ok(())
    }

    pub fn delete_by_prefix(&mut self, col: &ColumnFamily, prefix: &[u8]) -> io::Result<()> {
        // Simplified - collect keys and delete them
        let mut keys_to_delete = Vec::new();
        let iter = self.inner.iterator_cf(col, IteratorMode::From(prefix, rocksdb::Direction::Forward));
        for result in iter {
            if let Ok((key, _)) = result {
                if !key.starts_with(prefix) {
                    break;
                }
                keys_to_delete.push(key.to_vec());
            }
        }
        for key in &keys_to_delete {
            self.delete(col, key)?;
        }
        Ok(())
    }
}

// * Column family indices.
pub const COL_DEFAULT: usize = 0;
/// Account, with account resource.
pub const COL_ACCOUNT: usize = 1;
pub const COL_RESOURCE_DELEGATION: usize = 2;
pub const COL_RESOURCE_DELEGATION_INDEX: usize = 3;
pub const COL_VOTES: usize = 4;
pub const COL_CONTRACT: usize = 5;
pub const COL_CONTRACT_CODE: usize = 6;
pub const COL_CONTRACT_STORAGE: usize = 7;
pub const COL_WITNESS: usize = 8;
pub const COL_PROPOSAL: usize = 9;
pub const COL_ASSET: usize = 10;
pub const COL_TRANSACTION_RECEIPT: usize = 11;
pub const COL_INTERNAL_TRANSACTION: usize = 12;
pub const COL_TRANSACTION_LOG: usize = 13;
pub const COL_ACCOUNT_INDEX: usize = 14;
pub const COL_VOTER_REWARD: usize = 15;
pub const COL_EXCHANGE: usize = 16;

/// The State DB derived from Chain DB.
pub struct StateDB {
    db: OverlayDB,
    // Store column family names instead of handles for now
    col_names: Vec<String>,
}

impl Drop for StateDB {
    fn drop(&mut self) {
        info!("state-db closed successfully, all cached layers will be droped");
    }
}

fn col_descs_for_state_db() -> Vec<ColumnFamilyDescriptor> {
    vec![
        ColumnFamilyDescriptor::new(
            "default",
            {
                let mut opts = Options::default();
                opts.set_num_levels(2);
                opts.set_compression_type(rocksdb::DBCompressionType::None);
                opts
            }
        ),
        // address => Account
        ColumnFamilyDescriptor::new("account", Options::default()),
        // <<from_address, to_address>> => AccountResourceDelegation
        ColumnFamilyDescriptor::new("resource-delegation", Options::default()),
        // to_address => [from_address]
        ColumnFamilyDescriptor::new("resource-delegation-index", Options::default()),
        // address => Votes
        ColumnFamilyDescriptor::new("account-votes", Options::default()),
        // address => Contract
        ColumnFamilyDescriptor::new("contract", Options::default()),
        // address => Code
        ColumnFamilyDescriptor::new("contract-code", Options::default()),
        // <<contract_address: Address, storage_key: H256>> => H256
        ColumnFamilyDescriptor::new(
            "contract-storage",
            {
                let mut opts = Options::default();
                opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(32));
                opts
            }
        ),
        // <<Address>> => Witness
        ColumnFamilyDescriptor::new(
            "witness",
            {
                let mut opts = Options::default();
                opts.set_num_levels(2);
                opts.set_compression_type(rocksdb::DBCompressionType::None);
                opts
            }
        ),
        // <<id: u64>> => Proposal
        ColumnFamilyDescriptor::new(
            "proposal",
            {
                let mut opts = Options::default();
                opts.set_num_levels(2);
                opts.set_compression_type(rocksdb::DBCompressionType::None);
                opts
            }
        ),
        // <<id: u64>> => Asset
        ColumnFamilyDescriptor::new("asset", Options::default()),
        // <<txid: H256>> -> TransactionReceipt
        ColumnFamilyDescriptor::new("transaction-receipt", Options::default()),
        // <<txid: H256>> -> InternalTransaction
        ColumnFamilyDescriptor::new("internal-transaction", Options::default()),
        // <<Address, Topic: H256, [IndexedParam]>> => Transaction
        ColumnFamilyDescriptor::new(
            "transaction-log",
            {
                let mut opts = Options::default();
                opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(32));
                opts
            }
        ),
        // <<account_name: str>> => Address
        ColumnFamilyDescriptor::new(
            "account-index",
            {
                let mut opts = Options::default();
                opts.set_compression_type(rocksdb::DBCompressionType::None);
                opts
            }
        ),
        ColumnFamilyDescriptor::new("voter-reward", Options::default()),
        ColumnFamilyDescriptor::new("exchange", Options::default()),
    ]
}

impl StateDB {
    pub fn new<P: AsRef<Path>>(db_path: P) -> StateDB {
        std::fs::create_dir_all(&db_path).expect("create db directory");

        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.increase_parallelism(num_cpus::get() as i32);
        db_options.set_max_open_files(1024);

        let column_families = col_descs_for_state_db();

        let db = DB::open_cf_descriptors(&db_options, db_path, column_families).unwrap();

        // Store column family names
        let col_names = vec![
            "default".to_string(),
            "account".to_string(),
            "resource-delegation".to_string(),
            "resource-delegation-index".to_string(),
            "account-votes".to_string(),
            "contract".to_string(),
            "contract-code".to_string(),
            "contract-storage".to_string(),
            "witness".to_string(),
            "proposal".to_string(),
            "asset".to_string(),
            "transaction-receipt".to_string(),
            "internal-transaction".to_string(),
            "transaction-log".to_string(),
            "account-index".to_string(),
            "voter-reward".to_string(),
            "exchange".to_string(),
        ];

        StateDB {
            db: OverlayDB::new(db),
            col_names,
        }
    }
}

impl StateDB {
    fn get_cf(&self, index: usize) -> &ColumnFamily {
        self.db.inner.cf_handle(&self.col_names[index]).expect("column family should exist")
    }
    pub fn new_layer(&mut self) -> &mut OverlayWriteBatch {
        self.db.push_layer(OverlayWriteBatch::with_capacity(4 * 1024));
        self.db.layers.back_mut().unwrap()
    }

    pub fn finalize_layer(&mut self) {
        self.db
            .layers
            .pop_front()
            .map(|wb| self.db.inner.write(wb.wb));
    }

    pub fn discard_last_layer(&mut self) -> io::Result<()> {
        self.db
            .layers
            .pop_back()
            .ok_or(io::Error::new(io::ErrorKind::NotFound, "no layers"))?;
        Ok(())
    }

    pub fn put_key<T, K: keys::Key<T>>(&mut self, key: K, value: T) -> Result<(), BoxError> {
        // Get column family name first to avoid borrowing conflicts
        let col_name = self.col_names[K::COL].clone();

        // Get the column family handle first
        let col = self.db.inner.cf_handle(&col_name).expect("column family should exist");

        // Now get the write batch and perform the operation
        let wb = self
            .db
            .layers
            .back_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no db layers found"))?;
        wb.put(col, key.key().as_ref(), &*K::value(&value));
        Ok(())
    }

    pub fn delete_key<T, K: keys::Key<T>>(&mut self, key: &K) -> Result<(), BoxError> {
        let col_name = self.col_names[K::COL].clone();

        // For delete operations, we need to work around the borrowing issue
        // by using the write batch approach instead of direct delete
        let wb = self
            .db
            .layers
            .back_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no db layers found"))?;
        let col = self.db.inner.cf_handle(&col_name).expect("column family should exist");
        wb.delete(col, key.key().as_ref());
        Ok(())
    }

    pub fn delete_by_prefix(&mut self, col_idx: usize, prefix: &[u8]) -> Result<(), BoxError> {
        // For now, let's implement this by collecting keys first, then deleting them
        let col_name = self.col_names[col_idx].clone();
        let col = self.db.inner.cf_handle(&col_name).expect("column family should exist");

        // Collect keys to delete
        let mut keys_to_delete = Vec::new();
        let iter = self.db.inner.iterator_cf(col, IteratorMode::From(prefix, rocksdb::Direction::Forward));
        for result in iter {
            if let Ok((key, _)) = result {
                if !key.starts_with(prefix) {
                    break;
                }
                keys_to_delete.push(key.to_vec());
            }
        }

        // Now delete them using the write batch
        let wb = self
            .db
            .layers
            .back_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no db layers found"))?;
        let col = self.db.inner.cf_handle(&col_name).expect("column family should exist");
        for key in &keys_to_delete {
            wb.delete(col, key);
        }
        Ok(())
    }

    pub fn get<T, K: keys::Key<T>>(&self, key: &K) -> Result<Option<T>, BoxError> {
        self.db
            .get(self.get_cf(K::COL), key.key().as_ref())
            .map(|maybe_raw| maybe_raw.map(|raw| K::parse_value(&raw)))
            .map_err(|e| e.into())
    }

    pub fn get_skipped<T, K: keys::Key<T>>(&self, n: usize, key: &K) -> Result<Option<T>, BoxError> {
        self.db
            .get_skipped(n, self.get_cf(K::COL), key.key().as_ref())
            .map(|maybe_raw| maybe_raw.map(|raw| K::parse_value(&raw)))
            .map_err(|e| e.into())
    }

    pub fn must_get_skipped<T, K: keys::Key<T>>(&self, n: usize, key: &K) -> T {
        self.db
            .get_skipped(n, self.get_cf(K::COL), key.key().as_ref())
            .map(|maybe_raw| maybe_raw.map(|raw| K::parse_value(&raw)))
            .expect("corrupted db")
            .expect("key must exist")
    }

    pub fn must_get<T, K: keys::Key<T>>(&self, key: &K) -> T {
        self.db
            .get(self.get_cf(K::COL), key.key().as_ref())
            .map(|maybe_raw| maybe_raw.map(|raw| K::parse_value(&raw)))
            .expect("corrupted db")
            .expect("key must exist")
    }

    /// Increase a i64 key and the return updated value.
    pub fn incr_key<K: keys::Key<i64>>(&mut self, key: K) -> Result<i64, BoxError> {
        let old_val = self.get(&key)?.expect("key must be found");
        self.put_key(key, old_val + 1)?;
        Ok(old_val + 1)
    }

    pub fn for_each<T, K: keys::Key<T>, F>(&self, mut func: F)
    where
        F: FnMut(&K, &T) -> (),
    {
        self.db.for_each(self.get_cf(K::COL), move |key, value| {
            if let Some(key) = K::parse_key(key) {
                func(&key, &K::parse_value(value));
            }
        });
    }

    pub fn for_each_by_prefix<T, K: keys::Key<T>, F>(&self, prefix: &[u8], mut func: F)
    where
        F: FnMut(&K, &T) -> (),
    {
        self.db
            .for_each_by_prefix(self.get_cf(K::COL), prefix, move |key, value| {
                if let Some(key) = K::parse_key(key) {
                    func(&key, &K::parse_value(value));
                }
            });
    }

    pub fn init_genesis(&mut self, genesis: &GenesisConfig, chain: &ChainConfig) -> Result<(), BoxError> {
        if let Some(db_ver) = self.get(&keys::DynamicProperty::DbVersion)? {
            // TODO: check migration here
            let latest_block_hash = self.must_get(&keys::LatestBlockHash);
            let latest_block_numer = self.must_get(&DynamicProperty::LatestBlockNumber);
            info!(
                "state-db is already inited, db version: {}, block number: {}, block hash: {:?}",
                db_ver, latest_block_numer, latest_block_hash
            );

            return Ok(());
        }

        self.new_layer();

        for (k, v) in default_parameters_from_config(&chain.parameter) {
            self.put_key(k, v)?;
        }
        for (k, v) in DynamicProperty::default_properties() {
            self.put_key(k, v)?;
        }

        self.apply_genesis_config(genesis)?;

        // WitnessSchedule is inited in first maintenance cycle.

        self.db.finalize_layers()?;
        info!("state-db is inited from genesis");
        Ok(())
    }

    fn apply_genesis_config(&mut self, genesis: &GenesisConfig) -> Result<(), BoxError> {
        let mut witnesses: Vec<(Address, i64)> = vec![];
        for witness in &genesis.witnesses {
            let addr = witness.address.parse::<Address>()?;
            let wit = state_pb::Witness {
                address: addr.as_bytes().to_vec(),
                url: witness.url.clone(),
                vote_count: witness.votes,
                brokerage: constants::DEFAULT_BROKERAGE_RATE,
                // assume all witness in genesis are active witnesses.
                is_active: true,
                ..Default::default()
            };
            let key = keys::Witness(addr);

            self.put_key(key, wit)?;

            let key = keys::Account(addr);
            let acct = state_pb::Account {
                creation_time: genesis.timestamp,
                r#type: AccountType::Normal as i32,
                resource: Some(Default::default()),
                ..Default::default()
            };
            self.put_key(key, acct)?;

            witnesses.push((addr, witness.votes));
        }

        for alloc in &genesis.allocs {
            let addr: Address = alloc.address.parse()?;
            let acct = state_pb::Account {
                name: alloc.name.clone(),
                balance: alloc.balance,
                creation_time: genesis.timestamp,
                r#type: AccountType::Normal as i32,
                resource: Some(Default::default()),
                ..Default::default()
            };

            self.put_key(keys::Account(addr), acct)?;
            self.put_key(keys::AccountIndex(alloc.name.clone()), addr)?;
        }

        let genesis_block = genesis.to_indexed_block()?;
        self.put_key(keys::LatestBlockHash, *genesis_block.hash())?;
        self.put_key(DynamicProperty::LatestBlockNumber, 0)?;
        self.put_key(DynamicProperty::LatestBlockTimestamp, genesis_block.header.timestamp())?;
        self.put_key(DynamicProperty::LatestSolidBlockNumber, 0)?;

        // default block filled slots
        self.put_key(
            keys::BlockFilledSlots,
            vec![1; constants::NUM_OF_BLOCK_FILLED_SLOTS as usize],
        )?;

        Ok(())
    }
}

pub struct ReadOnlySolidStateDB {
    db: DB,
    cols: Vec<ColumnFamily>,
}

unsafe impl Send for ReadOnlySolidStateDB {}
unsafe impl Sync for ReadOnlySolidStateDB {}

impl ReadOnlySolidStateDB {
    // TODO: Implement ReadOnlySolidStateDB after completing basic migration
    pub fn new<P1: AsRef<Path>, P2: AsRef<Path>>(_db_path: P1, _tmp_path: P2) -> StateDB {
        unimplemented!("ReadOnlySolidStateDB::new - to be implemented after basic migration")
    }

    pub fn get<T, K: keys::Key<T>>(&self, _key: &K) -> Result<Option<T>, BoxError> {
        unimplemented!("ReadOnlySolidStateDB::get - to be implemented after basic migration")
    }

    pub fn catch_up_with_primary(&self) {
        unimplemented!("ReadOnlySolidStateDB::catch_up_with_primary - to be implemented after basic migration")
    }
}
