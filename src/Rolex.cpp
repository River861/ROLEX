#include "Rolex.h"
#include "RdmaBuffer.h"
#include "Timer.h"
#include "LeafNode.h"
#include "Hash.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
#include <set>
#include <map>
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <random>
std::mutex debug_lock;

uint64_t lock_fail[MAX_APP_THREAD];
uint64_t write_handover_num[MAX_APP_THREAD];
uint64_t try_write_op[MAX_APP_THREAD];
uint64_t read_handover_num[MAX_APP_THREAD];
uint64_t try_read_op[MAX_APP_THREAD];
uint64_t read_leaf_retry[MAX_APP_THREAD];
uint64_t leaf_read_syn[MAX_APP_THREAD];
uint64_t try_read_leaf[MAX_APP_THREAD];
double load_factor_sum[MAX_APP_THREAD];
uint64_t split_hopscotch[MAX_APP_THREAD];
uint64_t correct_speculative_read[MAX_APP_THREAD];
uint64_t try_speculative_read[MAX_APP_THREAD];
uint64_t want_speculative_read[MAX_APP_THREAD];
std::map<uint64_t, uint64_t> range_cnt[MAX_APP_THREAD];

uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];

volatile bool need_stop = false;
volatile bool need_clear[MAX_APP_THREAD];

thread_local std::vector<CoroPush> RolexIndex::workers;
thread_local CoroQueue RolexIndex::busy_waiting_queue;

// Auxiliary structure for simplicity
thread_local std::map<GlobalAddress, GlobalAddress> RolexIndex::coro_syn_leaf_addrs[MAX_CORO_NUM+1];


RolexIndex::RolexIndex(DSM *dsm, std::vector<Key> &load_keys, uint16_t rolex_id) : dsm(dsm), rolex_id(rolex_id) {
  assert(dsm->is_register());
  std::fill(need_clear, need_clear + MAX_APP_THREAD, false);
  clear_debug_info();
  // Cache
  rolex_cache = new RolexCache(dsm, load_keys);
#ifdef SPECULATIVE_READ
  idx_cache = new IdxCache(define::kHotIdxCacheSize, dsm);
#endif
  // RDWC
  local_lock_table = new LocalLockTable();
}


inline void RolexIndex::before_operation(CoroPull* sink) {
  auto tid = dsm->getMyThreadID();
  if (need_clear[tid]) {
    lock_fail[tid]               = 0;
    write_handover_num[tid]      = 0;
    try_write_op[tid]            = 0;
    read_handover_num[tid]       = 0;
    try_read_op[tid]             = 0;
    read_leaf_retry[tid]         = 0;
    leaf_read_syn[tid]           = 0;
    try_read_leaf[tid]           = 0;
    // load_factor_sum[tid]         = 0;
    // split_hopscotch[tid]         = 0;
    correct_speculative_read[tid]= 0;
    try_speculative_read[tid]    = 0;
    want_speculative_read[tid]   = 0;
    range_cnt[tid].clear();
    need_clear[tid]              = false;
  }
}


inline GlobalAddress RolexIndex::get_leaf_address(int leaf_idx) {
  uint64_t offset = define::kLeafRegionStartOffset + (leaf_idx / MEMORY_NODE_NUM) * ROUND_UP(define::allocationLeafSize, 3);
  assert(offset + define::allocationLeafSize < define::synRegionOffset);
  return GlobalAddress{leaf_idx % MEMORY_NODE_NUM, offset};
}


inline std::pair<uint64_t, uint64_t> RolexIndex::get_lock_info(const GlobalAddress &node_addr) {
  auto lock_offset = get_unlock_info(node_addr);

  uint64_t leaf_lock_cas_offset     = ROUND_DOWN(lock_offset, 3);
  uint64_t leaf_lock_mask           = 1UL << ((lock_offset - leaf_lock_cas_offset) * 8UL);
  return std::make_pair(leaf_lock_cas_offset, leaf_lock_mask);
}


inline uint64_t RolexIndex::get_unlock_info(const GlobalAddress &node_addr) {
#ifdef SCATTERED_LEAF_METADATA
  static const uint64_t leaf_lock_offset         = ADD_CACHELINE_VERSION_SIZE(sizeof(ScatteredLeafNode), define::versionSize);
#else
  static const uint64_t leaf_lock_offset         = ADD_CACHELINE_VERSION_SIZE(sizeof(LeafNode), define::versionSize);
#endif
  return leaf_lock_offset;
}


void RolexIndex::lock_node(const GlobalAddress &node_addr, CoroPull* sink) {
  auto [lock_cas_offset, lock_mask] = get_lock_info(node_addr);
  auto cas_buffer = (dsm->get_rbuf(sink)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &node_addr) {
    return dsm->cas_mask_sync_without_sink(node_addr + lock_cas_offset, 0UL, ~0UL, cas_buffer, lock_mask, sink, &busy_waiting_queue);
  };
  int cnt = 0;
re_acquire:
  if (!acquire_lock(node_addr)){
    if (cnt ++ > 10000000) {
      std::cout << "dead lock!" << std::endl;
      assert(false);
    }
    if (sink != nullptr) {
      busy_waiting_queue.push(sink->get());
      (*sink)();
    }
    lock_fail[dsm->getMyThreadID()] ++;
    goto re_acquire;
  }
  return;
}

void RolexIndex::unlock_node(const GlobalAddress &node_addr, CoroPull* sink, bool async) {
  auto lock_offset = get_unlock_info(node_addr);
  auto zero_buffer = (dsm->get_rbuf(sink)).get_zero_8_byte();

  // unlock function
  auto unlock = [=](const GlobalAddress &node_addr){
    if (async) {
      dsm->write_without_sink((char *)zero_buffer, node_addr + lock_offset, sizeof(uint64_t), false, sink, &busy_waiting_queue);
    }
    else {
      dsm->write_sync_without_sink((char *)zero_buffer, node_addr + lock_offset, sizeof(uint64_t), sink, &busy_waiting_queue);
    }
  };
  unlock(node_addr);
  return;
}


void RolexIndex::insert(const Key &k, Value v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);
  auto& syn_leaf_addrs = coro_syn_leaf_addrs[sink ? sink->get() : 0];

  int read_leaf_cnt = 0;

  // handover
  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);

  try_write_op[dsm->getMyThreadID()] ++;
#ifdef TREE_ENABLE_WRITE_COMBINING
  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, sink);
  write_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (write_handover) {
    write_handover_num[dsm->getMyThreadID()]++;
    goto insert_finish;
  }

  {
  // 1. Fetching
  auto [l, r, insert_idx] = rolex_cache->search_from_cache_for_insert(k);
  std::vector<GlobalAddress> leaf_addrs;
  std::vector<LeafNode*> _;
  for (int i = l; i <= r; ++ i) leaf_addrs.emplace_back(get_leaf_address(i));  // without reading synonym leaves
  read_leaf_cnt += leaf_addrs.size();
#ifdef HOPSCOTCH_LEAF_NODE
  int hash_idx = get_hashed_leaf_entry_index(k);
  hopscotch_fetch_nodes(leaf_addrs, hash_idx, _, sink);
#else
  fetch_nodes(leaf_addrs, _, sink);
#endif

  // 2. Fine-grained locking and re-read
  GlobalAddress insert_leaf_addr = get_leaf_address(insert_idx);
  LeafNode* leaf = nullptr, *syn_leaf = nullptr;
  lock_node(insert_leaf_addr, sink);
  // re-read leaf + synonym leaf
  if (syn_leaf_addrs.find(insert_leaf_addr) == syn_leaf_addrs.end()) {
    read_leaf_cnt ++;
    fetch_node(insert_leaf_addr, leaf, sink, false);
    if (leaf->metadata.synonym_ptr != GlobalAddress::Null()) {
      leaf_read_syn[dsm->getMyThreadID()] ++;
      syn_leaf_addrs[insert_leaf_addr] = leaf->metadata.synonym_ptr;
      read_leaf_cnt ++;
      fetch_node(syn_leaf_addrs[insert_leaf_addr], syn_leaf, sink);
    }
  }
  else {
    std::vector<LeafNode*> two_leaves;
    read_leaf_cnt += 2;
    fetch_nodes(std::vector<GlobalAddress>{insert_leaf_addr, syn_leaf_addrs[insert_leaf_addr]}, two_leaves, sink);
    leaf = two_leaves.front();
    syn_leaf = two_leaves.back();
  }

  // 3. Insert k locally
  assert(leaf != nullptr);
  auto& records = leaf->records;
  int i;
  bool write_leaf = false, write_syn_leaf = false;
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->get_combining_value(k, v);
#endif

#ifdef ENABLE_VAR_SIZE_KV
  {
  // first write a new DataBlock out-of-place
  auto block_buffer = (dsm->get_rbuf(sink)).get_block_buffer();
  auto data_block = new (block_buffer) DataBlock(v);
  auto block_addr = dsm->alloc(define::dataBlockLen, PACKED_ADDR_ALIGN_BIT);
  dsm->write_sync_without_sink(block_buffer, block_addr, define::dataBlockLen, sink, &busy_waiting_queue);
  // change value into the DataPointer value pointing to the DataBlock
  v = (uint64_t)DataPointer(define::dataBlockLen, block_addr);
  }
#endif

#ifdef HOPSCOTCH_LEAF_NODE
  for (const auto& e : leaf->records) if (e.key == k) {  // existing key
    unlock_node(insert_leaf_addr, sink);
    goto insert_finish;
  }
  // use a leaf copy to hop since it may fail
  auto leaf_copy_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  memcpy(leaf_copy_buffer, (char*)leaf, define::allocationLeafSize);
  if (!hopscotch_insert_and_unlock((LeafNode*)leaf_copy_buffer, k, v, insert_leaf_addr, sink)) {  // return false(and remain locked) if need insert into synonym leaf
    // insert k into the synonym leaf
    GlobalAddress syn_leaf_addr = leaf->metadata.synonym_ptr;
    if (!syn_leaf) {  // allocate a new synonym leaf
      hopscotch_split_and_unlock(leaf, k, v, insert_leaf_addr, sink);
    }
    else {  // insert into old synonym leaf
      if (!hopscotch_insert_and_unlock(syn_leaf, k, v, syn_leaf_addr, sink, false)) {
        printf("synonmy leaf is hop-full!!\n");  // ASSERT: synonmy leaf is hop-full!!
        // assert(false);
      }
      unlock_node(insert_leaf_addr, sink);
    }
  }
#else
  for (i = 0; i < (int)define::leafSpanSize; ++ i) {
    const auto& e = records[i];
    if (e.key == k) {
      unlock_node(insert_leaf_addr, sink);
      goto insert_finish;
    }
    if (e.key == define::kkeyNull || e.key > k) break;
  }
  if (i == (int)define::leafSpanSize) {  // insert k into the synonym leaf
    write_syn_leaf = true;
    auto syn_addr = insert_into_syn_leaf_locally(k, v, syn_leaf, insert_leaf_addr.nodeID, sink);
    if (syn_addr == GlobalAddress::Max()) {  // existing key
      unlock_node(insert_leaf_addr, sink);
      goto insert_finish;
    }
    if (syn_addr != GlobalAddress::Null()) {  // new syn leaf
      write_leaf = true;
      syn_leaf_addrs[insert_leaf_addr] = syn_addr;
      leaf->metadata.synonym_ptr = syn_addr;
    }
  }
  else {  // insert k into the leaf
    write_leaf = true;
    int j = i;
    while (j < (int)define::leafSpanSize && records[j].key != define::kkeyNull) j ++;
    if (j == (int)define::leafSpanSize) {  // overflow the last k to the synonym leaf
      write_syn_leaf = true;
      const auto& last_e = records[j - 1];
      auto syn_addr = insert_into_syn_leaf_locally(last_e.key, last_e.value, syn_leaf, insert_leaf_addr.nodeID, sink);
      if (syn_addr == GlobalAddress::Max()) {  // existing key
        unlock_node(insert_leaf_addr, sink);
        goto insert_finish;
      }
      if (syn_addr != GlobalAddress::Null()) {  // new syn leaf
        syn_leaf_addrs[insert_leaf_addr] = syn_addr;
        leaf->metadata.synonym_ptr = syn_addr;
      }
      j = define::leafSpanSize - 1;
    }
    // move [i, j) => [i+1, j+1]
    if (j > 0) for (int k = j - 1; k >= i; -- k) records[k + 1] = records[k];
    records[i].update(k, v);
  }

  // 4. Writing and unlocking
  leaf_addrs.clear();
  std::vector<LeafNode*> leaves;
  if (write_leaf) leaf_addrs.emplace_back(insert_leaf_addr), leaves.emplace_back(leaf);
  if (write_syn_leaf) leaf_addrs.emplace_back(syn_leaf_addrs[insert_leaf_addr]), leaves.emplace_back(syn_leaf);
  write_nodes_and_unlock(leaf_addrs, leaves, insert_leaf_addr, sink);
#endif
  }
  range_cnt[dsm->getMyThreadID()][read_leaf_cnt] ++;

insert_finish:
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->release_local_write_lock(k, lock_res);
#endif
  return;
}


GlobalAddress RolexIndex::insert_into_syn_leaf_locally(const Key &k, Value v, LeafNode*& syn_leaf, int nodeID, CoroPull* sink) {
  GlobalAddress syn_leaf_addr{};
  if (!syn_leaf) {  // allocate a new synonym leaf
    syn_leaf_addr = dsm->alloc(define::allocationLeafSize);
    auto syn_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    syn_leaf = new (syn_buffer) LeafNode;
    syn_leaf->records[0].update(k, v);
  }
  else {  // store in the synonym leaf
    auto& syn_records = syn_leaf->records;
    int i;
    for (i = 0; i < (int)define::leafSpanSize; ++ i) {
      const auto& e = syn_records[i];
      if (e.key == k) {
        return GlobalAddress::Max();
      }
      if (e.key == define::kkeyNull || e.key > k) break;
    }
    assert(i != (int)define::leafSpanSize);  // ASSERT: synonym leaf is full!!
    int j = i;
    while (j < (int)define::leafSpanSize && syn_records[j].key != define::kkeyNull) j ++;
    assert(j != (int)define::leafSpanSize);  // ASSERT: synonym leaf is full!!
    // move [i, j) => [i+1, j+1]
    if (j > 0) for (int k = j - 1; k >= i; -- k) syn_records[k + 1] = syn_records[k];
    syn_records[i].update(k, v);
  }
  return syn_leaf_addr;
}


void RolexIndex::fetch_nodes(const std::vector<GlobalAddress>& leaf_addrs, std::vector<LeafNode*>& leaves, CoroPull* sink, bool update_local_slt) {
  try_read_leaf[dsm->getMyThreadID()] ++;
  std::vector<char*> raw_buffers;
  std::vector<RdmaOpRegion> rs;
#ifdef SCATTERED_LEAF_METADATA
  std::vector<char*> intermediate_buffers;
#endif

  for (const auto& leaf_addr : leaf_addrs) {
    auto raw_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    raw_buffers.emplace_back(raw_buffer);
    auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    leaves.emplace_back((LeafNode*) leaf_buffer);
#ifdef SCATTERED_LEAF_METADATA
    auto intermediate_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    intermediate_buffers.emplace_back(intermediate_buffer);
#endif
  }

re_fetch:
  rs.clear();
  for (int i = 0; i < leaf_addrs.size(); ++ i) {
    RdmaOpRegion r;
    r.source     = (uint64_t)raw_buffers[i];
    r.dest       = leaf_addrs[i].to_uint64();
    r.size       = define::transLeafSize;
    r.is_on_chip = false;
    rs.emplace_back(r);
  }
  dsm->read_batches_sync(rs, sink);
  // consistency check
  for (int i = 0; i < leaf_addrs.size(); ++ i) {
#ifdef SCATTERED_LEAF_METADATA
    if (!(LeafVersionManager::decode_node_versions(raw_buffers[i], intermediate_buffers[i]))) {
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_fetch;
    }
    MetadataManager::decode_node_metadata(intermediate_buffers[i], (char*)leaves[i]);
#else
    if (!(VerMng::decode_node_versions(raw_buffers[i], (char*)leaves[i]))) {
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_fetch;
    }
#endif
  }
  if (update_local_slt) for (int i = 0; i < (int)leaf_addrs.size(); ++ i) {
    if (leaves[i]->metadata.synonym_ptr != GlobalAddress::Null()) {
      coro_syn_leaf_addrs[sink ? sink->get() : 0][leaf_addrs[i]] = leaves[i]->metadata.synonym_ptr;
    }
  }
  return;
}


void RolexIndex::write_nodes_and_unlock(const std::vector<GlobalAddress>& leaf_addrs, const std::vector<LeafNode*>& leaves, const GlobalAddress& locked_leaf_addr, CoroPull* sink) {
  assert(leaf_addrs.size() == leaves.size());

  std::vector<RdmaOpRegion> rs;
  for (int i = 0; i < (int)leaf_addrs.size(); ++ i) {
    auto encoded_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#ifdef SCATTERED_LEAF_METADATA
    auto intermediate_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    MetadataManager::encode_node_metadata((char*)leaves[i], intermediate_leaf_buffer);
    LeafVersionManager::encode_node_versions(intermediate_leaf_buffer, encoded_leaf_buffer);
#else
    VerMng::encode_node_versions((char*)leaves[i], encoded_leaf_buffer);
#endif
    RdmaOpRegion r;
    r.source = (uint64_t)encoded_leaf_buffer;
    r.dest = leaf_addrs[i].to_uint64();
    r.size = define::transLeafSize;
    r.is_on_chip = false;
    rs.emplace_back(r);
  }
  // unlock
  RdmaOpRegion r;
  auto lock_offset = get_unlock_info(locked_leaf_addr);
  auto zero_buffer = dsm->get_rbuf(sink).get_zero_8_byte();
  r.source = (uint64_t)zero_buffer;
  r.dest = (locked_leaf_addr + lock_offset).to_uint64();
  r.size = sizeof(uint64_t);
  r.is_on_chip = false;
  rs.emplace_back(r);
  dsm->write_batches_sync(rs, sink);
  return;
}


void RolexIndex::fetch_node(const GlobalAddress& leaf_addr, LeafNode*& leaf, CoroPull* sink, bool update_local_slt) {
  try_read_leaf[dsm->getMyThreadID()] ++;

  auto raw_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#ifdef SCATTERED_LEAF_METADATA
  auto intermediate_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#endif
  leaf = (LeafNode *) leaf_buffer;
re_read:
  dsm->read_sync(raw_buffer, leaf_addr, define::transLeafSize, sink);
  // consistency check
#ifdef SCATTERED_LEAF_METADATA
  if (!(LeafVersionManager::decode_node_versions(raw_buffer, intermediate_buffer))) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
  MetadataManager::decode_node_metadata(intermediate_buffer, leaf_buffer);
#else
  if (!(VerMng::decode_node_versions(raw_buffer, leaf_buffer))) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
#endif
  if (update_local_slt) if (leaf->metadata.synonym_ptr != GlobalAddress::Null()) {
    coro_syn_leaf_addrs[sink ? sink->get() : 0][leaf_addr] = leaf->metadata.synonym_ptr;
  }
  return;
}

void RolexIndex::write_node_and_unlock(const GlobalAddress& leaf_addr, LeafNode* leaf, const GlobalAddress& locked_leaf_addr, CoroPull* sink) {
  auto encoded_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#ifdef SCATTERED_LEAF_METADATA
  auto intermediate_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  MetadataManager::encode_node_metadata((char*)leaf, intermediate_leaf_buffer);
  LeafVersionManager::encode_node_versions(intermediate_leaf_buffer, encoded_leaf_buffer);
#else
  VerMng::encode_node_versions((char*)leaf, encoded_leaf_buffer);
#endif

  std::vector<RdmaOpRegion> rs(2);
  rs[0].source = (uint64_t)encoded_leaf_buffer;
  rs[0].dest = leaf_addr.to_uint64();
  rs[0].size = define::transLeafSize;
  rs[0].is_on_chip = false;
  // unlock
  auto lock_offset = get_unlock_info(locked_leaf_addr);
  auto zero_buffer = dsm->get_rbuf(sink).get_zero_8_byte();
  rs[1].source = (uint64_t)zero_buffer;
  rs[1].dest = (locked_leaf_addr + lock_offset).to_uint64();
  rs[1].size = sizeof(uint64_t);
  rs[1].is_on_chip = false;
  dsm->write_batches_sync(rs, sink);
  return;
}


void RolexIndex::update(const Key &k, Value v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  int read_leaf_cnt = 0;

  // handover
  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);

  int kv_idx;
  try_write_op[dsm->getMyThreadID()]++;
#ifdef TREE_ENABLE_WRITE_COMBINING
  lock_res = local_lock_table->acquire_local_write_lock(k, v, &busy_waiting_queue, sink);
  write_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (write_handover) {
    write_handover_num[dsm->getMyThreadID()]++;
    goto update_finish;
  }

  {
#ifdef HOPSCOTCH_LEAF_NODE
  int hash_idx = get_hashed_leaf_entry_index(k);
#endif
  // 1. Fetching
  Value old_v;
  auto [ret, leaf_addr, lock_leaf_addr, cnt] = _search(k, old_v, sink);
  read_leaf_cnt += cnt;
  UNUSED(old_v);
  assert(ret);
  // 2. Fine-grained locking and re-read
  lock_node(lock_leaf_addr, sink);
  LeafNode* leaf;
#ifdef SPECULATIVE_READ
  want_speculative_read[dsm->getMyThreadID()] ++;
  auto old_addr = leaf_addr;
#ifdef HOPSCOTCH_LEAF_NODE
    auto p = std::make_pair(hash_idx, (hash_idx + define::hopRange) % define::leafSpanSize);
#else
    auto p = std::make_pair(0, define::leafSpanSize);
#endif
  if (speculative_read(leaf_addr, p, k, old_v, leaf, kv_idx, read_leaf_cnt, sink)) {
    UNUSED(old_v);
    goto update_entry;
  }
  if (speculative_read(leaf_addr, std::make_pair(p.first + define::leafSpanSize, p.second + define::leafSpanSize), k, v, leaf, kv_idx, read_leaf_cnt, sink)) {
    UNUSED(old_v);
    goto update_entry;
  }
  leaf_addr = old_addr;
#endif

#ifdef ENABLE_VAR_SIZE_KV
  auto write_indirect_value = [&](Value& v){
    // first write a new DataBlock out-of-place
    auto block_buffer = (dsm->get_rbuf(sink)).get_block_buffer();
    auto data_block = new (block_buffer) DataBlock(v);
    auto block_addr = dsm->alloc(define::dataBlockLen, PACKED_ADDR_ALIGN_BIT);
    dsm->write_sync_without_sink(block_buffer, block_addr, define::dataBlockLen, sink, &busy_waiting_queue);
    // change value into the DataPointer value pointing to the DataBlock
    v = (uint64_t)DataPointer(define::dataBlockLen, block_addr);
  };
#endif

  {
read_another:
  read_leaf_cnt ++;
#ifdef HOPSCOTCH_LEAF_NODE
  hopscotch_fetch_node(leaf_addr, hash_idx, leaf, sink);
#else
  fetch_node(leaf_addr, leaf, sink);
#endif
  // 3. Update k locally
  assert(leaf != nullptr);
  auto& records = leaf->records;
  bool key_is_found = false;
#ifdef HOPSCOTCH_LEAF_NODE
  for (int j = 0; j < (int)define::hopRange; ++ j) {
    kv_idx = (hash_idx + j) % define::leafSpanSize;
    auto& e = leaf->records[kv_idx];
    if (e.key == k) {
      key_is_found = true;
#ifdef TREE_ENABLE_WRITE_COMBINING
      local_lock_table->get_combining_value(k, v);
#endif
#ifdef ENABLE_VAR_SIZE_KV
      write_indirect_value(v);
#endif
      e.update(k, v);
      break;
    }
  }
#else
  for (kv_idx = 0; kv_idx < define::leafSpanSize; ++ kv_idx) {
    auto& e = records[kv_idx];
    if (e.key == k) {
      key_is_found = true;
#ifdef TREE_ENABLE_WRITE_COMBINING
      local_lock_table->get_combining_value(k, v);
#endif
#ifdef ENABLE_VAR_SIZE_KV
      write_indirect_value(v);
#endif
      e.update(k, v);
      break;
    }
  }
#endif
  if (!key_is_found) {  // key is moved to the synonym leaf
    assert(leaf_addr == lock_leaf_addr);
    assert(leaf->metadata.synonym_ptr != GlobalAddress::Null());
    leaf_addr = leaf->metadata.synonym_ptr;
    leaf_read_syn[dsm->getMyThreadID()] ++;
    goto read_another;
  }
  }

  // 4. Writing and unlock
#ifdef SPECULATIVE_READ
  idx_cache->add_to_cache(leaf_addr, (leaf_addr == lock_leaf_addr) ? kv_idx : (define::leafSpanSize + kv_idx), k);
update_entry:
  leaf->records[kv_idx].update(k, v);
#endif
#ifdef HOPSCOTCH_LEAF_NODE
  entry_write_and_unlock(leaf, kv_idx, leaf_addr, lock_leaf_addr, sink);
#else
  write_node_and_unlock(leaf_addr, leaf, lock_leaf_addr, sink);
#endif
  }
  range_cnt[dsm->getMyThreadID()][read_leaf_cnt] ++;

update_finish:
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->release_local_write_lock(k, lock_res);
#endif
  return;
}


bool RolexIndex::search(const Key &k, Value &v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  int read_leaf_cnt = 0;

  // handover
  bool search_res = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);
  bool read_handover = false;

  try_read_op[dsm->getMyThreadID()] ++;
#ifdef TREE_ENABLE_READ_DELEGATION
  lock_res = local_lock_table->acquire_local_read_lock(k, &busy_waiting_queue, sink);
  read_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (read_handover) {
    read_handover_num[dsm->getMyThreadID()]++;
    goto search_finish;
  }

  {
  auto [ret, _1, _2, cnt] = _search(k, v, sink);
  search_res = ret;
#ifdef ENABLE_VAR_SIZE_KV
  if (search_res) {
    // read the DataBlock
    auto block_len = ((DataPointer *)&v)->data_len;
    auto block_addr = ((DataPointer *)&v)->ptr;
    auto block_buffer = (dsm->get_rbuf(sink)).get_block_buffer();
    dsm->read_sync(block_buffer, (GlobalAddress)block_addr, block_len, sink);
    v = ((DataBlock*)block_buffer)->value;
  }
#endif
  read_leaf_cnt += cnt;
  }
  range_cnt[dsm->getMyThreadID()][read_leaf_cnt] ++;

search_finish:
#ifdef TREE_ENABLE_READ_DELEGATION
  local_lock_table->release_local_read_lock(k, lock_res, search_res, v);  // handover the ret leaf addr
#endif
  return search_res;
}


std::tuple<bool, GlobalAddress, GlobalAddress, int> RolexIndex::_search(const Key &k, Value &v, CoroPull* sink) {
  int read_leaf_cnt = 0;
  auto& syn_leaf_addrs = coro_syn_leaf_addrs[sink ? sink->get() : 0];

#ifdef HOPSCOTCH_LEAF_NODE
  int hash_idx = get_hashed_leaf_entry_index(k);
#endif

  // 1. Read predict leaves and the synonmy leaves
  auto [l, r] = rolex_cache->search_from_cache(k);

#ifdef SPECULATIVE_READ
  want_speculative_read[dsm->getMyThreadID()] ++;
  for (int i = l; i <= r; ++ i) {
    LeafNode* leaf;
    int kv_idx;
    auto leaf_addr = get_leaf_address(i);
#ifdef HOPSCOTCH_LEAF_NODE
    auto p = std::make_pair(hash_idx, (hash_idx + define::hopRange) % define::leafSpanSize);
#else
    auto p = std::make_pair(0, define::leafSpanSize);
#endif
    if (speculative_read(leaf_addr, p, k, v, leaf, kv_idx, read_leaf_cnt, sink)) {
      return std::make_tuple(true, leaf_addr, get_leaf_address(i), read_leaf_cnt);
    }
    if (speculative_read(leaf_addr, std::make_pair(p.first + define::leafSpanSize, p.second + define::leafSpanSize), k, v, leaf, kv_idx, read_leaf_cnt, sink)) {
      return std::make_tuple(true, leaf_addr, get_leaf_address(i), read_leaf_cnt);
    }
  }
#endif
re_read:
  std::vector<GlobalAddress> leaf_addrs;
  std::vector<LeafNode*> leaves;
  std::vector<GlobalAddress> locked_leaf_addrs;
  for (int i = l; i <= r; ++ i) { // leaves
    auto leaf_addr = get_leaf_address(i);
    leaf_addrs.emplace_back(leaf_addr);
    locked_leaf_addrs.emplace_back(leaf_addr);
  }
  for (int i = l; i <= r; ++ i) { // synonym leaves
    auto leaf_addr = get_leaf_address(i);
    if (syn_leaf_addrs.find(leaf_addr) != syn_leaf_addrs.end()) {
      leaf_addrs.emplace_back(syn_leaf_addrs[leaf_addr]);
      locked_leaf_addrs.emplace_back(leaf_addr);
    }
  }
  read_leaf_cnt += leaf_addrs.size();
#ifdef HOPSCOTCH_LEAF_NODE
  hopscotch_fetch_nodes(leaf_addrs, hash_idx, leaves, sink, false);
#else
  fetch_nodes(leaf_addrs, leaves, sink, false);
#endif
  // 2. Read cache-miss synonmy leaves (if exists)
  std::vector<GlobalAddress> append_leaf_addrs;
  std::vector<LeafNode*> append_leaves;
  std::vector<GlobalAddress> append_locked_leaf_addrs;
  for (int i = 0; i <= r - l; ++ i) {
    auto leaf_addr = leaf_addrs[i];
    auto leaf = leaves[i];
    if (leaf->metadata.synonym_ptr != GlobalAddress::Null()
        && syn_leaf_addrs.find(leaf_addr) == syn_leaf_addrs.end()) {
      syn_leaf_addrs[leaf_addr] = leaf->metadata.synonym_ptr;
      append_leaf_addrs.emplace_back(syn_leaf_addrs[leaf_addr]);
      append_locked_leaf_addrs.emplace_back(leaf_addr);
    }
  }
  if (!append_leaf_addrs.empty()) {
    leaf_read_syn[dsm->getMyThreadID()] ++;
    read_leaf_cnt += append_leaf_addrs.size();
#ifdef HOPSCOTCH_LEAF_NODE
    hopscotch_fetch_nodes(append_leaf_addrs, hash_idx, append_leaves, sink);
#else
    fetch_nodes(append_leaf_addrs, append_leaves, sink);
#endif
    leaf_addrs.insert(leaf_addrs.end(), append_leaf_addrs.begin(), append_leaf_addrs.end());
    leaves.insert(leaves.end(), append_leaves.begin(), append_leaves.end());
    locked_leaf_addrs.insert(locked_leaf_addrs.end(), append_locked_leaf_addrs.begin(), append_locked_leaf_addrs.end());
  }
  // 3. Search the fetched leaves
  assert(leaf_addrs.size() == leaves.size() && leaves.size() == locked_leaf_addrs.size());
  for (int i = 0; i < (int)leaves.size(); ++ i) {
#ifdef HOPSCOTCH_LEAF_NODE
    // check hopping consistency && search key from the segments
    uint16_t hop_bitmap = 0;
    for (int j = 0; j < (int)define::hopRange; ++ j) {
      int kv_idx = (hash_idx + j) % define::leafSpanSize;
      const auto& e = leaves[i]->records[kv_idx];
      if (e.key != define::kkeyNull && (int)get_hashed_leaf_entry_index(e.key) == hash_idx) {
        hop_bitmap |= 1U << (define::hopRange - j - 1);
        if (e.key == k) {  // optimization: if the target key is found, consistency check can be stopped
          v = e.value;
#ifdef SPECULATIVE_READ
          idx_cache->add_to_cache(leaf_addrs[i], (leaf_addrs[i] == locked_leaf_addrs[i]) ? kv_idx : (define::leafSpanSize + kv_idx), k);
#endif
          return std::make_tuple(true, leaf_addrs[i], locked_leaf_addrs[i], read_leaf_cnt);
        }
      }
    }
    if (hop_bitmap != leaves[i]->records[hash_idx].hop_bitmap) {
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_read;
    }
#else
    for (int kv_idx = 0; kv_idx < define::leafSpanSize; ++ kv_idx) {
      const auto& e = leaves[i]->records[kv_idx];
      if (e.key == k) {
        v = e.value;
#ifdef SPECULATIVE_READ
        idx_cache->add_to_cache(leaf_addrs[i], (leaf_addrs[i] == locked_leaf_addrs[i]) ? kv_idx : (define::leafSpanSize + kv_idx), k);
#endif
        return std::make_tuple(true, leaf_addrs[i], locked_leaf_addrs[i], read_leaf_cnt);
      }
    }
#endif
  }
  return std::make_tuple(false, GlobalAddress::Null(), GlobalAddress::Null(), read_leaf_cnt);
}


/*
  range query
  DO NOT support coroutines currently
*/
void RolexIndex::range_query(const Key &from, const Key &to, std::map<Key, Value> &ret) {  // [from, to)  TODO: HOPSCOTCH_LEAF_NODE
  assert(dsm->is_register());
  before_operation(nullptr);
  auto& syn_leaf_addrs = coro_syn_leaf_addrs[MAX_CORO_NUM];

  // 1. Read predict leaves and the synonmy leaves
  auto [l, r] = rolex_cache->search_range_from_cache(from, to);
  range_cnt[dsm->getMyThreadID()][r - l + 1] ++;
  std::vector<GlobalAddress> leaf_addrs;
  std::vector<LeafNode*> leaves;
  for (int i = l; i <= r; ++ i) leaf_addrs.emplace_back(get_leaf_address(i));  // leaves
  for (int i = l; i <= r; ++ i) { // leaves && synonym leaves
    auto leaf_addr = get_leaf_address(i);
    if (syn_leaf_addrs.find(leaf_addr) != syn_leaf_addrs.end()) {
      leaf_addrs.emplace_back(syn_leaf_addrs[leaf_addr]);
    }
  }
  fetch_nodes(leaf_addrs, leaves, nullptr, false);
  // 2. Read cache-miss synonmy leaves (if exists)
  std::vector<GlobalAddress> append_leaf_addrs;
  std::vector<LeafNode*> append_leaves;
  for (int i = 0; i <= r - l; ++ i) {
    auto leaf_addr = leaf_addrs[i];
    auto leaf = leaves[i];
    if (leaf->metadata.synonym_ptr != GlobalAddress::Null()
        && syn_leaf_addrs.find(leaf_addr) == syn_leaf_addrs.end()) {
      syn_leaf_addrs[leaf_addr] = leaf->metadata.synonym_ptr;
      append_leaf_addrs.emplace_back(syn_leaf_addrs[leaf_addr]);
    }
  }
  if (!append_leaf_addrs.empty()) {
    fetch_nodes(append_leaf_addrs, append_leaves, nullptr);
    leaf_addrs.insert(leaf_addrs.end(), append_leaf_addrs.begin(), append_leaf_addrs.end());
    leaves.insert(leaves.end(), append_leaves.begin(), append_leaves.end());
  }
  // 3. Search the fetched leaves
  assert(leaf_addrs.size() == leaves.size());
  for (const auto& leaf : leaves) {
    for (const auto& e : leaf->records) {
      if (e.key != define::kkeyNull && e.key >= from && e.key < to) {
        ret[e.key] = e.value;
      }
    }
  }
#ifdef ENABLE_VAR_SIZE_KV
  auto range_buffer = (dsm->get_rbuf(nullptr)).get_range_buffer();
  // read DataBlocks via doorbell batching
  std::map<Key, Value> indirect_values;
  std::vector<RdmaOpRegion> kv_rs;
  int kv_cnt = 0;
  for (const auto& [_, data_ptr] : ret) {
    auto data_addr = ((DataPointer*)&data_ptr)->ptr;
    auto data_len  = ((DataPointer*)&data_ptr)->data_len;
    RdmaOpRegion r;
    r.source     = (uint64_t)range_buffer + kv_cnt * define::dataBlockLen;
    r.dest       = ((GlobalAddress)data_addr).to_uint64();
    r.size       = data_len;
    r.is_on_chip = false;
    kv_rs.push_back(r);
    kv_cnt ++;
  }
  dsm->read_batches_sync(kv_rs);
  kv_cnt = 0;
  for (auto& [_, v] : ret) {
    auto data_block = (DataBlock*)(range_buffer + kv_cnt * define::dataBlockLen);
    v = data_block->value;
    kv_cnt ++;
  }
#endif
  return;
}


#ifdef HOPSCOTCH_LEAF_NODE
bool RolexIndex::hopscotch_insert_and_unlock(LeafNode* leaf, const Key& k, Value v, const GlobalAddress& node_addr, CoroPull* sink, bool is_locked_leaf) {
  auto& records = leaf->records;
  auto get_entry = [=, &records](int logical_idx) -> LeafEntry& {
    return records[(logical_idx + define::leafSpanSize) % define::leafSpanSize];
  };
  // caculate hash idx
  int hash_idx = get_hashed_leaf_entry_index(k);
  // find an empty slot
  int empty_idx = -1;
  for (int i = hash_idx; i < hash_idx + (int)define::leafSpanSize; ++ i) {
    if (get_entry(i).key == define::kkeyNull) {
      empty_idx = i;
      break;
    }
  }
  if (empty_idx < 0) return false;  // no empty slot
  // hop
  int j = empty_idx;
  std::vector<int> hopped_idxes;
next_hop:
  hopped_idxes.emplace_back(j % define::leafSpanSize);
  if (j < hash_idx + (int)define::hopRange) {
    get_entry(j).update(k, v);
    get_entry(hash_idx).set_hop_bit(j - hash_idx);
    segment_write_and_unlock(leaf, hash_idx, empty_idx % define::leafSpanSize, hopped_idxes, node_addr, sink, is_locked_leaf);
    return true;
  }
  for (int offset = define::hopRange - 1; offset > 0; -- offset) {
    int h = j - offset;
    int h_hash_idx = get_hashed_leaf_entry_index(get_entry(h).key);
    // corner case
    if (h - h_hash_idx < 0) h_hash_idx -= define::leafSpanSize;
    else if (h - h_hash_idx >= (int)define::hopRange) h_hash_idx += define::leafSpanSize;
    assert(h_hash_idx <= h);
    // hop h => j is ok
    if (h_hash_idx + (int)define::hopRange > j) {
      get_entry(j).update(get_entry(h).key, get_entry(h).value);
      get_entry(h_hash_idx).unset_hop_bit(h - h_hash_idx);
      get_entry(h_hash_idx).set_hop_bit(j - h_hash_idx);
      j = h;
      goto next_hop;
    }
  }
  return false;  // hop fails
}


void RolexIndex::hopscotch_split_and_unlock(LeafNode* leaf, const Key& k, Value v, const GlobalAddress& node_addr, CoroPull* sink) {
  split_hopscotch[dsm->getMyThreadID()] ++;
  auto& records = leaf->records;
  // calculate split_key
  auto split_key = hopscotch_get_split_key(records, k);

  // synonym node
  auto synonym_addr = dsm->alloc(define::allocationLeafSize, node_addr.nodeID);  // TODO: same MN
  auto synonym_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto synonym_leaf = new (synonym_buffer) LeafNode;

  // move data
  int non_empty_entry_cnt = 0;
  for (int i = 0; i < (int)define::leafSpanSize; ++ i) {
    auto& old_e = records[i];
    if (old_e.key != define::kkeyNull) {
      ++ non_empty_entry_cnt;
      if (old_e.key >= split_key) {
        int hash_idx = get_hashed_leaf_entry_index(old_e.key);
        // move
        synonym_leaf->records[i].update(old_e.key, old_e.value);
        old_e.update(define::kkeyNull, define::kValueNull);
        // update hop_bit
        auto offset = (i >= hash_idx ? i - hash_idx : i + (int)define::leafSpanSize - hash_idx);
        synonym_leaf->records[hash_idx].set_hop_bit(offset);
        records[hash_idx].unset_hop_bit(offset);
      }
    }
  }
  load_factor_sum[dsm->getMyThreadID()] += (double)non_empty_entry_cnt / define::leafSpanSize;
  // newly insert kv
  if (k < split_key) hopscotch_insert_locally(records, k, v);
  else hopscotch_insert_locally(synonym_leaf->records, k, v);
  // change metadata
  leaf->metadata.synonym_ptr = synonym_addr;
  // write synonym leaf
  auto encoded_synonym_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#ifdef SCATTERED_LEAF_METADATA
  auto intermediate_synonym_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  MetadataManager::encode_node_metadata(synonym_buffer, intermediate_synonym_buffer);
  LeafVersionManager::encode_node_versions(intermediate_synonym_buffer, encoded_synonym_buffer);
#else
  VerMng::encode_node_versions(synonym_buffer, encoded_synonym_buffer);
#endif
  dsm->write_sync_without_sink(encoded_synonym_buffer, synonym_addr, define::transLeafSize, sink, &busy_waiting_queue);

  // wirte split node and unlock
  auto encoded_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
#ifdef SCATTERED_LEAF_METADATA
  auto intermediate_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  MetadataManager::encode_node_metadata((char *)leaf, intermediate_leaf_buffer);
  LeafVersionManager::encode_node_versions(intermediate_leaf_buffer, encoded_leaf_buffer);
#else
  VerMng::encode_node_versions((char *)leaf, encoded_leaf_buffer);
#endif
  memset(encoded_leaf_buffer + define::transLeafSize, 0, sizeof(uint64_t));  // unlock
  dsm->write_sync_without_sink(encoded_leaf_buffer, node_addr, define::transLeafSize + sizeof(uint64_t), sink, &busy_waiting_queue);
}


Key RolexIndex::hopscotch_get_split_key(LeafEntry* records, const Key& k) {
  // calculate a proper split key to ensure k can be inserted after node split
  auto get_entry = [=](int logical_idx) -> const LeafEntry& {
    return records[(logical_idx + define::leafSpanSize) % define::leafSpanSize];
  };

  std::vector<Key> critical_keys;
  int hash_idx = get_hashed_leaf_entry_index(k);
  for (int empty_idx = hash_idx; empty_idx < hash_idx + (int)define::leafSpanSize; ++ empty_idx) {
    if (get_entry(empty_idx).key == define::kkeyNull) break;
    // try hopping assuming that get_entry(empty_idx) is empty
    int j = empty_idx;
next_hop:
    if (j < hash_idx + (int)define::hopRange) {
      critical_keys.emplace_back(get_entry(empty_idx).key);
      continue;
    }
    for (int offset = define::hopRange - 1; offset > 0; -- offset) {
      int h = j - offset;
      int h_hash_idx = get_hashed_leaf_entry_index(get_entry(h).key);
      // corner case
      if (h - h_hash_idx < 0) h_hash_idx -= define::leafSpanSize;
      else if (h - h_hash_idx >= (int)define::hopRange) h_hash_idx += define::leafSpanSize;
      // hop h => j is ok
      if (h_hash_idx + (int)define::hopRange > j) {
        j = h;
        goto next_hop;
      }
    }
  }
  assert(!critical_keys.empty());
  std::sort(critical_keys.begin(), critical_keys.end());
  return critical_keys.at(critical_keys.size() / 2);
}


void RolexIndex::hopscotch_insert_locally(LeafEntry* records, const Key& k, Value v) {
  auto get_entry = [=, &records](int logical_idx) -> LeafEntry& {
    return records[(logical_idx + define::leafSpanSize) % define::leafSpanSize];
  };
  // caculate hash idx
  int hash_idx = get_hashed_leaf_entry_index(k);
  // find an empty slot
  int j = -1;
  for (int i = hash_idx; i < hash_idx + (int)define::leafSpanSize; ++ i) {
    if (get_entry(i).key == define::kkeyNull) {
      j = i;
      break;
    }
  }
  // hop
  assert(j >= 0);
next_hop:
  if (j < hash_idx + (int)define::hopRange) {
    get_entry(j).update(k, v);
    get_entry(hash_idx).set_hop_bit(j - hash_idx);
    return;
  }
  for (int offset = define::hopRange - 1; offset > 0; -- offset) {
    int h = j - offset;
    int h_hash_idx = get_hashed_leaf_entry_index(get_entry(h).key);
    // corner case
    if (h - h_hash_idx < 0) h_hash_idx -= define::leafSpanSize;
    else if (h - h_hash_idx >= (int)define::hopRange) h_hash_idx += define::leafSpanSize;
    // hop h => j is ok
    if (h_hash_idx + (int)define::hopRange > j) {
      get_entry(j).update(get_entry(h).key, get_entry(h).value);
      get_entry(h_hash_idx).unset_hop_bit(h - h_hash_idx);
      get_entry(h_hash_idx).set_hop_bit(j - h_hash_idx);
      j = h;
      goto next_hop;
    }
  }
  assert(false);
}


void RolexIndex::hopscotch_fetch_node(const GlobalAddress& leaf_addr, int hash_idx, LeafNode*& leaf, CoroPull* sink, bool update_local_slt) {
  std::vector<LeafNode*> leaves;
  hopscotch_fetch_nodes(std::vector<GlobalAddress>{leaf_addr}, hash_idx, leaves, sink, update_local_slt);
  leaf = leaves.front();
  return;
}


void RolexIndex::hopscotch_fetch_nodes(const std::vector<GlobalAddress>& leaf_addrs, int hash_idx, std::vector<LeafNode*>& leaves, CoroPull* sink, bool update_local_slt) {
  try_read_leaf[dsm->getMyThreadID()] ++;
  std::vector<char*> raw_buffers;
  std::vector<RdmaOpRegion> rs;
#ifdef SCATTERED_LEAF_METADATA
  std::vector<char*> intermediate_buffers_l;
  std::vector<char*> intermediate_buffers_r;
#endif

  for (int i = 0; i < leaf_addrs.size(); ++ i) {
    auto raw_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    raw_buffers.emplace_back(raw_buffer);
    auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    leaves.emplace_back((LeafNode*) leaf_buffer);
#ifdef SCATTERED_LEAF_METADATA
    auto intermediate_buffer_l = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto intermediate_buffer_r = (dsm->get_rbuf(sink)).get_segment_buffer();
    intermediate_buffers_l.emplace_back(intermediate_buffer_l);
    intermediate_buffers_r.emplace_back(intermediate_buffer_r);
#endif
  }

  auto segment_size_r = std::min((int)define::hopRange, (int)define::leafSpanSize - hash_idx);
  auto segment_size_l = define::hopRange <= (int)define::leafSpanSize - hash_idx ? 0 : define::hopRange - ((int)define::leafSpanSize - hash_idx);

#ifdef SCATTERED_LEAF_METADATA
  auto [raw_offset_r, raw_len_r, first_offset_r] = LeafVersionManager::get_offset_info(hash_idx, segment_size_r);
  auto [raw_offset_l, raw_len_l, first_offset_l] = LeafVersionManager::get_offset_info(0, segment_size_l);
  assert(segment_size_l > 0 || !raw_len_l);

  if (raw_len_l) {  // read two hop segments (corner case)
re_fetch_2:
    rs.clear();
    for (int i = 0; i < leaf_addrs.size(); ++ i) {
      auto raw_segment_buffer_r = raw_buffers[i] + raw_offset_r;
      auto raw_segment_buffer_l = raw_buffers[i] + raw_offset_l;

      RdmaOpRegion r1, r2;
      r1.source     = (uint64_t)raw_segment_buffer_l;
      r1.dest       = (leaf_addrs[i] + raw_offset_l).to_uint64();
      r1.size       = raw_len_l;
      r1.is_on_chip = false;
      rs.emplace_back(r1);

      r2.source     = (uint64_t)raw_segment_buffer_r;
      r2.dest       = (leaf_addrs[i] + raw_offset_r).to_uint64();
      r2.size       = raw_len_r;
      r2.is_on_chip = false;
      rs.emplace_back(r2);
    }
    dsm->read_batches_sync(rs, sink);
    // consistency check
    for (int i = 0; i < leaf_addrs.size(); ++ i) {
      auto raw_segment_buffer_r = raw_buffers[i] + raw_offset_r;
      auto raw_segment_buffer_l = raw_buffers[i] + raw_offset_l;

      uint8_t segment_node_versions_r = 0, segment_node_versions_l = 0;
      LeafMetadata metadata_l, metadata_r;
      auto [first_metadata_offset_l, new_len_l] = MetadataManager::get_offset_info(0, segment_size_l);
      auto [first_metadata_offset_r, new_len_r] = MetadataManager::get_offset_info(hash_idx, segment_size_r);
      if (!LeafVersionManager::decode_segment_versions(raw_segment_buffer_l, intermediate_buffers_l[i], first_offset_l, segment_size_l, first_metadata_offset_l, new_len_l, segment_node_versions_l) ||
        !LeafVersionManager::decode_segment_versions(raw_segment_buffer_r, intermediate_buffers_r[i], first_offset_r, segment_size_r, first_metadata_offset_r, new_len_r, segment_node_versions_r) ||
        segment_node_versions_r != segment_node_versions_l) {  // consistency check
        read_leaf_retry[dsm->getMyThreadID()] ++;
        goto re_fetch_2;
      }
      bool has_metadata_l = MetadataManager::decode_segment_metadata(intermediate_buffers_l[i], (char*)&(leaves[i]->records[0]), first_metadata_offset_l, segment_size_l, metadata_l);
      bool has_metadata_r = MetadataManager::decode_segment_metadata(intermediate_buffers_r[i], (char*)&(leaves[i]->records[hash_idx]), first_metadata_offset_r, segment_size_r, metadata_r);
      assert(has_metadata_l || has_metadata_l);
      if (has_metadata_l && has_metadata_r) assert(metadata_l == metadata_r);
      leaves[i]->metadata = (has_metadata_l ? metadata_l : metadata_r);
    }
  }
  else {  // read only one hop segment
re_fetch_1:
    rs.clear();
    for (int i = 0; i < leaf_addrs.size(); ++ i) {
      auto raw_segment_buffer_r = raw_buffers[i] + raw_offset_r;

      RdmaOpRegion r2;
      r2.source     = (uint64_t)raw_segment_buffer_r;
      r2.dest       = (leaf_addrs[i] + raw_offset_r).to_uint64();
      r2.size       = raw_len_r;
      r2.is_on_chip = false;
      rs.emplace_back(r2);
    }
    dsm->read_batches_sync(rs, sink);
    // consistency check
    for (int i = 0; i < leaf_addrs.size(); ++ i) {
      auto raw_segment_buffer_r = raw_buffers[i] + raw_offset_r;

      uint8_t segment_node_versions_r = 0;
      auto [first_metadata_offset_r, new_len_r] = MetadataManager::get_offset_info(hash_idx, segment_size_r);
      if (!LeafVersionManager::decode_segment_versions(raw_segment_buffer_r, intermediate_buffers_r[i], first_offset_r, segment_size_r, first_metadata_offset_r, new_len_r, segment_node_versions_r)) {
        read_leaf_retry[dsm->getMyThreadID()] ++;
        goto re_fetch_1;
      }
      auto has_metadata = MetadataManager::decode_segment_metadata(intermediate_buffers_r[i], (char*)&(leaves[i]->records[hash_idx]), first_metadata_offset_r, segment_size_r, leaves[i]->metadata);
      assert(has_metadata);
    }
  }
#else
  auto [raw_offset_r, raw_len_r, first_offset_r] = VerMng::get_offset_info(hash_idx, segment_size_r);
  auto [raw_offset_l, raw_len_l, first_offset_l] = VerMng::get_offset_info(0, segment_size_l);
  assert(segment_size_l > 0 || !raw_len_l);

re_fetch:
  rs.clear();
  for (int i = 0; i < leaf_addrs.size(); ++ i) {
    const auto& leaf_addr = leaf_addrs[i];
    const auto& raw_buffer = raw_buffers[i];
    auto raw_segment_buffer_r = raw_buffer + raw_offset_r;
    auto raw_segment_buffer_l = raw_buffer + raw_offset_l;

    RdmaOpRegion r1, r2;
    r1.source     = (uint64_t)raw_buffer;
    r1.dest       = leaf_addr.to_uint64();
    r1.size       = raw_offset_l + raw_len_l;  // header + segment_l (corner case)
    r1.is_on_chip = false;
    rs.emplace_back(r1);

    r2.source     = (uint64_t)raw_segment_buffer_r;
    r2.dest       = (leaf_addr + raw_offset_r).to_uint64();
    r2.size       = raw_len_r;
    r2.is_on_chip = false;
    rs.emplace_back(r2);
  }
  dsm->read_batches_sync(rs, sink);
  // consistency check
  for (int i = 0; i < leaf_addrs.size(); ++ i) {
    const auto& raw_buffer = raw_buffers[i];
    auto raw_segment_buffer_r = raw_buffer + raw_offset_r;
    auto raw_segment_buffer_l = raw_buffer + raw_offset_l;
    uint8_t metadata_node_version = 0, segment_node_versions_r = 0, segment_node_versions_l = 0;
    if (!VerMng::decode_header_versions(raw_buffer, (char*)leaves[i], metadata_node_version) ||
        (segment_size_l > 0 && !VerMng::decode_segment_versions(raw_segment_buffer_l, (char*)&(leaves[i]->records[0]), first_offset_l, segment_size_l, segment_node_versions_l)) ||
        !VerMng::decode_segment_versions(raw_segment_buffer_r, (char*)&(leaves[i]->records[hash_idx]), first_offset_r, segment_size_r, segment_node_versions_r) ||
        metadata_node_version != segment_node_versions_r ||
        (segment_size_l > 0 && metadata_node_version != segment_node_versions_l)) {
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_fetch;
    }
  }
#endif
  // update syn_leaf_addrs
  if (update_local_slt) for (int i = 0; i < (int)leaf_addrs.size(); ++ i) {
    if (leaves[i]->metadata.synonym_ptr != GlobalAddress::Null()) {
      coro_syn_leaf_addrs[sink? sink->get() : 0][leaf_addrs[i]] = leaves[i]->metadata.synonym_ptr;
    }
  }
  return;
}


void RolexIndex::segment_write_and_unlock(LeafNode* leaf, int l_idx, int r_idx, const std::vector<int>& hopped_idxes, const GlobalAddress& node_addr, CoroPull* sink, bool need_unlock) {
  auto& records = leaf->records;
  const auto& metadata = leaf->metadata;
  if (l_idx <= r_idx) {  // update with one WRITE + unlock
    auto encoded_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
#ifdef SCATTERED_LEAF_METADATA
    // segment [l_idx, r_idx]
    auto intermediate_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(l_idx, r_idx - l_idx + 1);
    MetadataManager::encode_segment_metadata((char *)&records[l_idx], intermediate_segment_buffer, first_metadata_offset, r_idx - l_idx + 1, metadata);
    auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(l_idx, r_idx - l_idx + 1);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer, encoded_segment_buffer, first_offset, hopped_idxes, l_idx, r_idx, first_metadata_offset, new_len);
#else
    // segment [l_idx, r_idx]
    auto [raw_offset, raw_len, first_offset] = VerMng::get_offset_info(l_idx, r_idx - l_idx + 1);
    VerMng::encode_segment_versions((char *)&records[l_idx], encoded_segment_buffer, first_offset, hopped_idxes, l_idx, r_idx);
#endif
    // write segment and unlock
    if (!need_unlock) {
      dsm->write_sync_without_sink(encoded_segment_buffer, node_addr + raw_offset, raw_len, sink, &busy_waiting_queue);
    }
    else if (r_idx == define::leafSpanSize - 1) {
      memset(encoded_segment_buffer + raw_len, 0, sizeof(uint64_t));  // unlock
      dsm->write_sync_without_sink(encoded_segment_buffer, node_addr + raw_offset, raw_len + sizeof(uint64_t), sink, &busy_waiting_queue);
    }
    else {
      std::vector<RdmaOpRegion> rs(2);
      rs[0].source = (uint64_t)encoded_segment_buffer;
      rs[0].dest = (node_addr + raw_offset).to_uint64();
      rs[0].size = raw_len;
      rs[0].is_on_chip = false;

      auto lock_offset = get_unlock_info(node_addr);
      auto zero_buffer = dsm->get_rbuf(sink).get_zero_8_byte();
      rs[1].source = (uint64_t)zero_buffer;
      rs[1].dest = (node_addr + lock_offset).to_uint64();
      rs[1].size = sizeof(uint64_t);
      rs[1].is_on_chip = false;
      dsm->write_batch_sync_without_sink(&rs[0], 2, sink, &busy_waiting_queue);
    }
  }
  else {  // update with two WRITE + unlock  TODO: threshold => use one WRITE
    auto encoded_segment_buffer_1 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto encoded_segment_buffer_2 = (dsm->get_rbuf(sink)).get_segment_buffer();
#ifdef SCATTERED_LEAF_METADATA
    // segment [0, r_idx]
    auto intermediate_segment_buffer_1 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_1, new_len_1] = MetadataManager::get_offset_info(0, r_idx + 1);
    MetadataManager::encode_segment_metadata((char *)&records[0], intermediate_segment_buffer_1, first_metadata_offset_1, r_idx + 1, metadata);
    auto [raw_offset_1, raw_len_1, first_offset_1] = LeafVersionManager::get_offset_info(0, r_idx + 1);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer_1, encoded_segment_buffer_1, first_offset_1, hopped_idxes, 0, r_idx, first_metadata_offset_1, new_len_1);
    // segment [l_idx, SPAN_SIZE-1]
    auto intermediate_segment_buffer_2 = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset_2, new_len_2] = MetadataManager::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    MetadataManager::encode_segment_metadata((char *)&records[l_idx], intermediate_segment_buffer_2, first_metadata_offset_2, define::leafSpanSize - l_idx, metadata);
    auto [raw_offset_2, raw_len_2, first_offset_2] = LeafVersionManager::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer_2, encoded_segment_buffer_2, first_offset_2, hopped_idxes, l_idx, define::leafSpanSize - 1, first_metadata_offset_2, new_len_2);
#else
    // segment [0, r_idx]
    auto [raw_offset_1, raw_len_1, first_offset_1] = VerMng::get_offset_info(0, r_idx + 1);
    VerMng::encode_segment_versions((char *)&records[0], encoded_segment_buffer_1, first_offset_1, hopped_idxes, 0, r_idx);
    // segment [l_idx, SPAN_SIZE-1]
    auto [raw_offset_2, raw_len_2, first_offset_2] = VerMng::get_offset_info(l_idx, define::leafSpanSize - l_idx);
    VerMng::encode_segment_versions((char *)&records[l_idx], encoded_segment_buffer_2, first_offset_2, hopped_idxes, l_idx, define::leafSpanSize - 1);
#endif
    // write segments and unlock
    std::vector<RdmaOpRegion> rs(2);
    rs[0].source = (uint64_t)encoded_segment_buffer_1;
    rs[0].dest = (node_addr + raw_offset_1).to_uint64();
    rs[0].size = raw_len_1;
    rs[0].is_on_chip = false;

    if (!need_unlock) {
      rs[1].size = raw_len_2;
    }
    else {
      memset(encoded_segment_buffer_2 + raw_len_2, 0, sizeof(uint64_t));  // unlock
      rs[1].size = raw_len_2 + sizeof(uint64_t);
    }
    rs[1].source = (uint64_t)encoded_segment_buffer_2;
    rs[1].dest = (node_addr + raw_offset_2).to_uint64();
    rs[1].is_on_chip = false;
    dsm->write_batch_sync_without_sink(&rs[0], 2, sink, &busy_waiting_queue);
  }
  return;
}


void RolexIndex::entry_write_and_unlock(LeafNode* leaf, const int idx, const GlobalAddress& node_addr, const GlobalAddress& locked_leaf_addr, CoroPull* sink) {
  auto& entry = leaf->records[idx];
  const auto & metadata = leaf->metadata;
  auto encoded_entry_buffer = (dsm->get_rbuf(sink)).get_entry_buffer();
#if (defined SCATTERED_LEAF_METADATA && defined HOPSCOTCH_LEAF_NODE)
  auto get_info_and_encode_versions = [=, &entry, &metadata](int idx){
    auto intermediate_segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
    auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(idx);
    MetadataManager::encode_segment_metadata((char *)&entry, intermediate_segment_buffer, first_metadata_offset, 1, metadata);
    auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(idx);
    LeafVersionManager::encode_segment_versions(intermediate_segment_buffer, encoded_entry_buffer, first_offset, std::vector<int>{idx}, idx, idx, first_metadata_offset, new_len);
    return std::make_pair(raw_offset, raw_len);
  };
  auto [raw_offset, raw_len] = get_info_and_encode_versions(idx);
#else
  auto [raw_offset, raw_len, first_offset] = VerMng::get_offset_info(idx);
  VerMng::encode_entry_versions((char *)&entry, encoded_entry_buffer, first_offset);
#endif
  // write entry and unlock
  if (idx == define::leafSpanSize - 1 && node_addr == locked_leaf_addr) {
    memset(encoded_entry_buffer + raw_len, 0, sizeof(uint64_t));  // unlock
    dsm->write_sync_without_sink(encoded_entry_buffer, node_addr + raw_offset, raw_len + sizeof(uint64_t), sink, &busy_waiting_queue);
  }
  else {
    std::vector<RdmaOpRegion> rs(2);
    rs[0].source = (uint64_t)encoded_entry_buffer;
    rs[0].dest = (node_addr + raw_offset).to_uint64();
    rs[0].size = raw_len;
    rs[0].is_on_chip = false;

    auto lock_offset = get_unlock_info(locked_leaf_addr);
    auto zero_buffer = dsm->get_rbuf(sink).get_zero_8_byte();
    rs[1].source = (uint64_t)zero_buffer;
    rs[1].dest = (locked_leaf_addr + lock_offset).to_uint64();
    rs[1].size = sizeof(uint64_t);
    rs[1].is_on_chip = false;
    dsm->write_batch_sync_without_sink(&rs[0], 2, sink, &busy_waiting_queue);
  }
  return;
}
#endif


#ifdef SPECULATIVE_READ
bool RolexIndex::speculative_read(GlobalAddress& leaf_addr, std::pair<int, int> range, const Key &k, Value &v, LeafNode*& leaf,
                                  int& speculative_idx, int& read_leaf_cnt, CoroPull* sink) {
  auto& syn_leaf_addrs = coro_syn_leaf_addrs[sink ? sink->get() : 0];

  auto raw_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  leaf = (LeafNode *)leaf_buffer;
  if (idx_cache->search_idx_from_cache(leaf_addr, range.first, range.second, k, speculative_idx)) {
    if (speculative_idx >= define::leafSpanSize && syn_leaf_addrs.find(leaf_addr) == syn_leaf_addrs.end()) return false;
    // read entry
    try_speculative_read[dsm->getMyThreadID()] ++;
    read_leaf_cnt ++;
    if (speculative_idx >= define::leafSpanSize) {
      leaf_entry_read(syn_leaf_addrs[leaf_addr], speculative_idx - define::leafSpanSize, raw_leaf_buffer, leaf_buffer, sink);
    }
    else {
      leaf_entry_read(leaf_addr, speculative_idx, raw_leaf_buffer, leaf_buffer, sink);
    }
    const auto& entry = leaf->records[speculative_idx];
    if (entry.key == k) {
      correct_speculative_read[dsm->getMyThreadID()] ++;
      v = entry.value;
      idx_cache->add_to_cache(leaf_addr, speculative_idx, k);
      if (speculative_idx >= define::leafSpanSize) {
        leaf_addr = syn_leaf_addrs[leaf_addr];
        speculative_idx = speculative_idx - define::leafSpanSize;
      }
      return true;
    }
  }
  return false;
}


void RolexIndex::leaf_entry_read(const GlobalAddress& leaf_addr, const int idx, char *raw_leaf_buffer, char *leaf_buffer, CoroPull* sink) {
  auto leaf = (LeafNode *)leaf_buffer;
#ifdef SCATTERED_LEAF_METADATA
  auto [raw_offset, raw_len, first_offset] = LeafVersionManager::get_offset_info(idx);
  auto raw_entry_buffer = raw_leaf_buffer + raw_offset;
re_read:
  dsm->read_sync(raw_entry_buffer, leaf_addr + raw_offset, raw_len, sink);
  uint8_t entry_node_version = 0;
  auto intermediate_entry_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
  auto [first_metadata_offset, new_len] = MetadataManager::get_offset_info(idx);
  if (!LeafVersionManager::decode_segment_versions(raw_entry_buffer, intermediate_entry_buffer, first_offset, 1, first_metadata_offset, new_len, entry_node_version)) {
    goto re_read;
  }
  MetadataManager::decode_segment_metadata(intermediate_entry_buffer, (char*)&(leaf->records[idx]), first_metadata_offset, 1, leaf->metadata);
  return;
#else
  auto [raw_offset, raw_len, first_offset] = VerMng::get_offset_info(idx);
  auto raw_entry_buffer = raw_leaf_buffer + raw_offset;
re_read:
  // read metadata and the hop segment
  std::vector<RdmaOpRegion> rs(2);
  rs[0].source = (uint64_t)raw_leaf_buffer;
  rs[0].dest = leaf_addr.to_uint64();
  rs[0].size = define::bufferMetadataSize;  // header
  rs[0].is_on_chip = false;

  rs[1].source = (uint64_t)raw_entry_buffer;
  rs[1].dest = (leaf_addr + raw_offset).to_uint64();
  rs[1].size = raw_len;
  rs[1].is_on_chip = false;
  // note that the rs array will change by lower-level function
  dsm->read_batch_sync(&rs[0], 2, sink);
  uint8_t metadata_node_version = 0, entry_node_version = 0;
  // consistency check; note that: (segment_size_l > 0) => there are two segments
  if (!VerMng::decode_header_versions(raw_leaf_buffer, leaf_buffer, metadata_node_version) ||
      !VerMng::decode_segment_versions(raw_entry_buffer, (char*)&(leaf->records[idx]), first_offset, 1, entry_node_version) ||
      metadata_node_version != entry_node_version) {
    goto re_read;
  }
#endif
  return;
}
#endif


void RolexIndex::run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req, int req_num) {
  assert(coro_cnt <= MAX_CORO_NUM);
  // define coroutines
  for (int i = 0; i < coro_cnt; ++i) {
    RequstGen *gen = gen_func(dsm, req, req_num, i, coro_cnt);
    workers.emplace_back([=](CoroPull& sink) {
      coro_worker(sink, gen, work_func);
    });
  }
  // start running coroutines
  for (int i = 0; i < coro_cnt; ++i) {
    workers[i](i);
  }
  while (!need_stop) {
    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      workers[next_coro_id](next_coro_id);
    }
    if (!busy_waiting_queue.empty()) {
      auto next_coro_id = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      workers[next_coro_id](next_coro_id);
    }
  }
}


void RolexIndex::coro_worker(CoroPull &sink, RequstGen *gen, WorkFunc work_func) {
  rolex::Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();

  while (!need_stop) {
    auto r = gen->next();

    coro_timer.begin();
    work_func(this, r, &sink);
    auto us_10 = coro_timer.end() / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][sink.get()][us_10]++;

    busy_waiting_queue.push(sink.get());
    sink();
  }
}

void RolexIndex::statistics() {
  rolex_cache->statistics();
#ifdef SPECULATIVE_READ
  idx_cache->statistics();
#endif
}

void RolexIndex::clear_debug_info() {
  memset(lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(write_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_leaf_retry, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_leaf, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(leaf_read_syn, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(load_factor_sum, 0, sizeof(double) * MAX_APP_THREAD);
  memset(split_hopscotch, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(correct_speculative_read, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_speculative_read, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(want_speculative_read, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  for (int i = 0; i > MAX_APP_THREAD; ++ i) range_cnt[i].clear();
}
