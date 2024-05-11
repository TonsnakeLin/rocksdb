// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "example_common.h"
using std::string;
using std::vector;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
std::string kDBPath2 = "C:\\Windows\\TEMP\\rocksdb_simple_example2";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
std::string kDBPath2 = "/tmp/rocksdb_simple_example2";
string DBAlreadyExistsError = "error_if_exists is true";
#endif

std::string example_db_options = "create_if_missing=true;flush_verify_memtable_count=false";
std::string example_cf_options = "table_factory=PlainTable;prefix_extractor=rocksdb.CappedPrefix.13;comparator=leveldb.BytewiseComparator;compression_per_level=kBZip2Compression:kBZip2Compression:kBZip2Compression:kNoCompression:kZlibCompression:kBZip2Compression:kSnappyCompression;max_bytes_for_level_base=986;bloom_locality=8016;target_file_size_base=4294976376;memtable_huge_page_size=2557;max_successive_merges=5497;max_sequential_skip_in_iterations=4294971408;arena_block_size=1893;target_file_size_multiplier=35;min_write_buffer_number_to_merge=9;max_write_buffer_number=84;write_buffer_size=1653;max_compaction_bytes=64;max_bytes_for_level_multiplier=60;memtable_factory=SkipListFactory;compression=kNoCompression;bottommost_compression=kDisableCompressionOption;min_partial_merge_operands=7576;level0_stop_writes_trigger=33;num_levels=99;level0_slowdown_writes_trigger=22;level0_file_num_compaction_trigger=14;compaction_filter=urxcqstuwnCompactionFilter;soft_rate_limit=530.615385;soft_pending_compaction_bytes_limit=0;max_write_buffer_number_to_maintain=84;verify_checksums_in_compaction=false;merge_operator=aabcxehazrMergeOperator;memtable_prefix_bloom_size_ratio=0.4642;memtable_insert_with_hint_prefix_extractor=rocksdb.CappedPrefix.13;paranoid_file_checks=true;force_consistency_checks=true;inplace_update_num_locks=7429;optimize_filters_for_hits=false;level_compaction_dynamic_level_bytes=false;inplace_update_support=false;compaction_style=kCompactionStyleFIFO;purge_redundant_kvs_while_flush=true;hard_pending_compaction_bytes_limit=0;disable_auto_compactions=false;report_bg_io_stats=true;compaction_filter_factory=mpudlojcujCompactionFilterFactory;";
std::unordered_map<std::string, std::string> cf_options_map = {
      {"write_buffer_size", "1"},
      {"max_write_buffer_number", "2"},
      {"min_write_buffer_number_to_merge", "3"},
      {"max_write_buffer_number_to_maintain", "99"},
      {"compression", "kSnappyCompression"},
      {"compression_per_level",
       "kNoCompression:"
       "kSnappyCompression:"
       "kZlibCompression:"
       "kBZip2Compression:"
       "kLZ4Compression:"
       "kLZ4HCCompression:"
       "kXpressCompression:"
       "kZSTD:"
       "kZSTDNotFinalCompression"},
      {"bottommost_compression", "kLZ4Compression"},
      {"compression_opts", "4:5:6:7"},
      {"num_levels", "8"},
      {"level0_file_num_compaction_trigger", "8"},
      {"level0_slowdown_writes_trigger", "9"},
      {"level0_stop_writes_trigger", "10"},
      {"target_file_size_base", "12"},
      {"target_file_size_multiplier", "13"},
      {"max_bytes_for_level_base", "14"},
      {"level_compaction_dynamic_level_bytes", "true"},
      {"max_bytes_for_level_multiplier", "15.0"},
      {"max_bytes_for_level_multiplier_additional", "16:17:18"},
      {"max_compaction_bytes", "21"},
      {"soft_rate_limit", "1.1"},
      {"hard_rate_limit", "2.1"},
      {"hard_pending_compaction_bytes_limit", "211"},
      {"arena_block_size", "22"},
      {"disable_auto_compactions", "true"},
      {"compaction_style", "kCompactionStyleLevel"},
      {"verify_checksums_in_compaction", "false"},
      {"compaction_options_fifo", "23"},
      {"max_sequential_skip_in_iterations", "24"},
      {"inplace_update_support", "true"},
      {"report_bg_io_stats", "true"},
      {"compaction_measure_io_stats", "false"},
      {"inplace_update_num_locks", "25"},
      {"memtable_prefix_bloom_size_ratio", "0.26"},
      {"memtable_huge_page_size", "28"},
      {"bloom_locality", "29"},
      {"max_successive_merges", "30"},
      {"min_partial_merge_operands", "31"},
      {"prefix_extractor", "fixed:31"},
      {"optimize_filters_for_hits", "true"},
  };


void TestGetDBOPtionsFromString() {
  DBOptions base_options;
  DBOptions out_options;
  Status s = GetDBOptionsFromString(base_options, example_db_options, &out_options);
  cout << "base_options create_if_missing: "<< base_options.create_if_missing << endl;
  cout << "out_options create_if_missing:"<< out_options.create_if_missing << endl;
}


void TestGetColumnFamilyOptionsFromString() {
  ColumnFamilyOptions base_option, out_option;
  Status s = GetColumnFamilyOptionsFromString(base_option, example_cf_options, &out_option);
  if (!s.ok()) {
    cout << "failed to GetColumnFamilyOptionsFromString" << endl;
    return;
  }
  auto tf = out_option.table_factory.get();
  cout << "table_factory:" << tf->table_type() << endl;
}

void TestGetColumnFamilyOptionsFromStringMap() {
  ColumnFamilyOptions base_option, out_option;
  Status s = GetColumnFamilyOptionsFromMap(base_option, cf_options_map, &out_option);
  if (!s.ok()) {
    cout << "failed to GetColumnFamilyOptionsFromMap" << endl;
    return;
  }
  cout << "out_option.write_buffer_size:" << out_option.write_buffer_size << endl;
}

void TestSetDBOptions(DB *db) {
  std::unordered_map<std::string, std::string> ops = {
    {"create_if_missing", "true"},
  };
  db->SetDBOptions(ops);
}

Status batchInsertData(DB *db) {
  // atomically apply a set of updates
  WriteBatch batch;
  Status s;
  for (int i = 0; i < 1000; i++)
  {
    string key = string("lpkey").append(std::to_string(i));
    string value = string("value").append(std::to_string(i));
    batch.Put(key, value);
    if (i%99 == 0) {
      s = db->Write(WriteOptions(), &batch);
      if (!s.ok()) {
        return s;
      }
      batch.Clear();
    }    
  } 
  s = db->Write(WriteOptions(), &batch);
  FlushOptions flush_options;
  db->Flush(flush_options);
  return s;
}

using BatchesRef = vector<WriteBatch*> &;
Status batchInsertData2(DB *db) {
  // atomically apply a set of updates
  WriteBatch *batch = nullptr;
  Status s;
  vector<WriteBatch*> updates;
  // BatchesRef updates_ref = updates; 
  // BatchesRef & updates_rref = updates_ref;
  // vector<WriteBatch*> &(&updates_rref) = updates_ref;
  
  for (int i = 1; i <= 1000; i++)
  {
    if (batch == nullptr) {
      batch = new WriteBatch;
    }
    string key = string("lpkey").append(std::to_string(i));
    string value = string("value").append(std::to_string(i));
    batch -> Put(key, value);
    if (i % 100 == 0) {
      updates.push_back(batch);
      batch = nullptr;
    }    
  } 
  s = db->MultiBatchWrite(WriteOptions(), std::move(updates));
  assert(s.ok());
  FlushOptions flush_options;
  db->Flush(flush_options);
  // todo: delete updates
  return s;
}

bool ContainsString(const char *full_string, string &searched_string) {
  string::size_type idx = string(full_string).find(searched_string);

  if (idx != string::npos) 
    return true;
  
  return false;
}

int main() {
  DB* db = nullptr;
  DB* db2 = nullptr;

  /*
  TestGetDBOPtionsFromString();
  TestGetColumnFamilyOptionsFromString();
  TestGetColumnFamilyOptionsFromStringMap();
  */

  // open DB
  /*
  Status s = OpenDBDefault(kDBPath, &db);
  assert(s.ok());
  s = OpenDBDefault(kDBPath2, &db2);
  assert(s.ok());
  */
  Options options;
  GetOptionsWithTableOptions(&options);
  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok() && s.IsInvalidArgument() && ContainsString(s.getState(), DBAlreadyExistsError)) {
    s = DestroyDB(kDBPath, options);
    assert(s.ok());
    options.error_if_exists = false;
    s = DB::Open(options, kDBPath, &db);
  }
  assert(s.ok());
  // s = batchInsertData(db);
  s = batchInsertData2(db);
  assert(s.ok());

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  {
    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    // If the value is not pinned, the internal buffer must have the value.
    assert(pinnable_val.IsPinned() || string_val == "value");
  }

  PinnableSlice pinnable_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
  assert(s.IsNotFound());
  // Reset PinnableSlice after each use and before each reuse
  pinnable_val.Reset();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  assert(pinnable_val == "value");
  pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point

  if (nullptr != db) delete db;
  if (nullptr != db2) delete db2;

  return 0;
}
