#include <iostream>
#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "rocksdb/cache.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::Cache;
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using std::cout;
using std::endl;
using ROCKSDB_NAMESPACE::NewLRUCache;

Status OpenDBDefault(const std::string &name, DB **dbptr) {
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  return DB::Open(options, name, dbptr);
}

void GetOptionsWithTableOptions(Options *options) {
   // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options->IncreaseParallelism();
  options->OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options->create_if_missing = true;   
  options->error_if_exists = true;
  options->enable_multi_batch_write = true;
  
  // build block cache.
  size_t capacity = 128 << 20;
  std::shared_ptr<Cache> cache = NewLRUCache(capacity);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  options->table_factory.reset(NewBlockBasedTableFactory(table_options));
}