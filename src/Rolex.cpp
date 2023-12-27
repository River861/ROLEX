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
std::mutex debug_lock;

uint64_t lock_fail[MAX_APP_THREAD];
uint64_t write_handover_num[MAX_APP_THREAD];
uint64_t try_write_op[MAX_APP_THREAD];
uint64_t read_handover_num[MAX_APP_THREAD];
uint64_t try_read_op[MAX_APP_THREAD];
uint64_t read_leaf_retry[MAX_APP_THREAD];
uint64_t leaf_read_syn[MAX_APP_THREAD];
uint64_t try_read_leaf[MAX_APP_THREAD];
std::map<uint64_t, uint64_t> range_cnt[MAX_APP_THREAD];

uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];

volatile bool need_stop = false;
volatile bool need_clear[MAX_APP_THREAD];

thread_local std::vector<CoroPush> RolexIndex::workers;
thread_local CoroQueue RolexIndex::busy_waiting_queue;

// Auxiliary structure for simplicity
thread_local std::map<GlobalAddress, GlobalAddress> RolexIndex::syn_leaf_addrs;


RolexIndex::RolexIndex(DSM *dsm, std::vector<Key> &load_keys, uint16_t rolex_id) : dsm(dsm), rolex_id(rolex_id) {
  assert(dsm->is_register());
  std::fill(need_clear, need_clear + MAX_APP_THREAD, false);
  clear_debug_info();
  // Cache
  rolex_cache = new RolexCache(dsm, load_keys);
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
    range_cnt[tid].clear();
    need_clear[tid]              = false;
  }
}


inline GlobalAddress RolexIndex::get_leaf_address(int leaf_idx) {
  return GlobalAddress{leaf_idx % MEMORY_NODE_NUM, define::kLeafRegionStartOffset + (leaf_idx / MEMORY_NODE_NUM) * ROUND_UP(define::allocationLeafSize, 3)};
}


inline std::pair<uint64_t, uint64_t> RolexIndex::get_lock_info(const GlobalAddress &node_addr) {
  auto lock_offset = get_unlock_info(node_addr);

  uint64_t leaf_lock_cas_offset     = ROUND_DOWN(lock_offset, 3);
  uint64_t leaf_lock_mask           = 1UL << ((lock_offset - leaf_lock_cas_offset) * 8UL);
  return std::make_pair(leaf_lock_cas_offset, leaf_lock_mask);
}


inline uint64_t RolexIndex::get_unlock_info(const GlobalAddress &node_addr) {
  static const uint64_t leaf_lock_offset         = ADD_CACHELINE_VERSION_SIZE(sizeof(LeafNode), define::versionSize);
  return leaf_lock_offset;
}


void RolexIndex::lock_node(const GlobalAddress &node_addr, CoroPull* sink) {
  auto [lock_cas_offset, lock_mask] = get_lock_info(node_addr);
  auto cas_buffer = (dsm->get_rbuf(sink)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &node_addr) {
    return dsm->cas_mask_sync_without_sink(node_addr + lock_cas_offset, 0UL, ~0UL, cas_buffer, lock_mask, sink, &busy_waiting_queue);
  };
re_acquire:
  if (!acquire_lock(node_addr)){
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

#ifdef HOPSCOTCH_LEAF_NODE
  // use a leaf copy to hop since it may fail
  auto leaf_copy_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  memcpy(leaf_copy_buffer, (char*)leaf, define::allocationLeafSize);
  if (!hopscotch_insert_and_unlock((LeafNode*)leaf_copy_buffer, k, v, insert_leaf_addr, sink)) {  // return false(and remain locked) if need insert into synonym leaf
    // insert k into the synonym leaf
    GlobalAddress syn_leaf_addr = leaf->metadata.synonym_ptr;
    if (!syn_leaf) {  // allocate a new synonym leaf
      syn_leaf_addr = dsm->alloc(define::allocationLeafSize);
      auto syn_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
      syn_leaf = new (syn_buffer) LeafNode;
      write_leaf = true;
    }
    if (hopscotch_insert_and_unlock(syn_leaf, k, v, syn_leaf_addr, sink, false)) {  // ASSERT: synonmy leaf is hop-full!!
      printf("synonmy leaf is hop-full!!\n");
      assert(false);
    }
    if (write_leaf) {  // new syn leaf
      syn_leaf_addrs[insert_leaf_addr] = syn_leaf_addr;
      leaf->metadata.synonym_ptr = syn_leaf_addr;
      // write syn_pointer and unlock
      std::vector<RdmaOpRegion> rs(2);
      rs[0].source = (uint64_t)leaf;
      rs[0].dest = insert_leaf_addr.to_uint64();
      rs[0].size = define::leafMetadataSize;
      rs[0].is_on_chip = false;
      // unlock
      auto lock_offset = get_unlock_info(insert_leaf_addr);
      auto zero_buffer = dsm->get_rbuf(sink).get_zero_8_byte();
      rs[1].source = (uint64_t)zero_buffer;
      rs[1].dest = (insert_leaf_addr + lock_offset).to_uint64();
      rs[1].size = sizeof(uint64_t);
      rs[1].is_on_chip = false;
      dsm->write_batches_sync(rs, sink);
    }
    else {
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
    auto syn_addr = insert_into_syn_leaf_locally(k, v, syn_leaf, sink);
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
      auto syn_addr = insert_into_syn_leaf_locally(last_e.key, last_e.value, syn_leaf, sink);
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


GlobalAddress RolexIndex::insert_into_syn_leaf_locally(const Key &k, Value v, LeafNode*& syn_leaf, CoroPull* sink) {
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

re_fetch:
  raw_buffers.clear();
  rs.clear();
  for (const auto& leaf_addr : leaf_addrs) {
    auto raw_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    raw_buffers.emplace_back(raw_buffer);

    RdmaOpRegion r;
    r.source     = (uint64_t)raw_buffer;
    r.dest       = leaf_addr.to_uint64();
    r.size       = define::transLeafSize;
    r.is_on_chip = false;
    rs.emplace_back(r);
  }
  dsm->read_batches_sync(rs, sink);
  // consistency check
  for (auto raw_buffer : raw_buffers) {
    auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    if (!(VerMng::decode_node_versions(raw_buffer, leaf_buffer))) {
      leaves.clear();
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_fetch;
    }
    leaves.emplace_back((LeafNode*) leaf_buffer);
  }
  if (update_local_slt) for (int i = 0; i < (int)leaf_addrs.size(); ++ i) {
    if (leaves[i]->metadata.synonym_ptr != GlobalAddress::Null()) {
      syn_leaf_addrs[leaf_addrs[i]] = leaves[i]->metadata.synonym_ptr;
    }
  }
  return;
}


void RolexIndex::write_nodes_and_unlock(const std::vector<GlobalAddress>& leaf_addrs, const std::vector<LeafNode*>& leaves, const GlobalAddress& locked_leaf_addr, CoroPull* sink) {
  assert(leaf_addrs.size() == leaves.size());

  std::vector<RdmaOpRegion> rs;
  for (int i = 0; i < (int)leaf_addrs.size(); ++ i) {
    auto encoded_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    VerMng::encode_node_versions((char*)leaves[i], encoded_leaf_buffer);
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
  leaf = new (leaf_buffer) LeafNode;
re_read:
  dsm->read_sync(raw_buffer, leaf_addr, define::transLeafSize, sink);
  // consistency check
  if (!(VerMng::decode_node_versions(raw_buffer, leaf_buffer))) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
  if (update_local_slt) if (leaf->metadata.synonym_ptr != GlobalAddress::Null()) {
    syn_leaf_addrs[leaf_addr] = leaf->metadata.synonym_ptr;
  }
  return;
}

void RolexIndex::write_node_and_unlock(const GlobalAddress& leaf_addr, LeafNode* leaf, const GlobalAddress& locked_leaf_addr, CoroPull* sink) {
  auto encoded_leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  VerMng::encode_node_versions((char*)leaf, encoded_leaf_buffer);

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
  // 1. Fetching
  Value old_v;
  auto [ret, leaf_addr, lock_leaf_addr, cnt] = _search(k, old_v, sink);
  read_leaf_cnt += cnt;
  UNUSED(old_v);
  assert(ret);
  // 2. Fine-grained locking and re-read
  lock_node(lock_leaf_addr, sink);
  LeafNode* leaf;
read_another:
  read_leaf_cnt ++;
#ifdef HOPSCOTCH_LEAF_NODE
  int hash_idx = get_hashed_leaf_entry_index(k);
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
      e.update(k, v);
      break;
    }
  }
#else
  for (auto& e : records) {
    if (e.key == define::kkeyNull) break;
    if (e.key == k) {
      key_is_found = true;
#ifdef TREE_ENABLE_WRITE_COMBINING
      local_lock_table->get_combining_value(k, v);
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
  // 4. Writing and unlock
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

  // 1. Read predict leaves and the synonmy leaves
  auto [l, r] = rolex_cache->search_from_cache(k);
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
  int hash_idx = get_hashed_leaf_entry_index(k);
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
    uint8_t hop_bitmap = 0U;
    for (int j = 0; j < (int)define::hopRange; ++ j) {
      const auto& e = leaves[i]->records[(hash_idx + j) % define::leafSpanSize];
      if (e.key != define::kkeyNull && (int)get_hashed_leaf_entry_index(e.key) == hash_idx) {
        hop_bitmap |= 1U << (define::hopRange - j - 1);
        if (e.key == k) {  // optimization: if the target key is found, consistency check can be stopped
          v = e.value;
          return std::make_tuple(true, leaf_addrs[i], locked_leaf_addrs[i], read_leaf_cnt);
        }
      }
    }
    if (hop_bitmap != leaves[i]->records[hash_idx].hop_bitmap) {
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_read;
    }
#else
    for (const auto& e : leaves[i]->records) {
      if (e.key == define::kkeyNull) break;
      if (e.key == k) {
        v = e.value;
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
  return;
}


#ifdef HOPSCOTCH_LEAF_NODE
bool RolexIndex::hopscotch_insert_and_unlock(LeafNode* leaf, const Key& k, Value v, const GlobalAddress& node_addr, CoroPull* sink, bool need_unlock) {
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
    segment_write_and_unlock(leaf, hash_idx, empty_idx % define::leafSpanSize, hopped_idxes, node_addr, sink, need_unlock);
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

  auto segment_size_r = std::min((int)define::hopRange, (int)define::leafSpanSize - hash_idx);
  auto segment_size_l = define::hopRange <= (int)define::leafSpanSize - hash_idx ? 0 : define::hopRange - ((int)define::leafSpanSize - hash_idx);

  auto [raw_offset_r, raw_len_r, first_offset_r] = VerMng::get_offset_info(hash_idx, segment_size_r);
  auto [raw_offset_l, raw_len_l, first_offset_l] = VerMng::get_offset_info(0, segment_size_l);
  assert(segment_size_l > 0 || !raw_len_l);


re_fetch:
  raw_buffers.clear();
  rs.clear();
  for (const auto& leaf_addr : leaf_addrs) {
    auto raw_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    raw_buffers.emplace_back(raw_buffer);
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
  for (auto raw_buffer : raw_buffers) {
    auto raw_segment_buffer_r = raw_buffer + raw_offset_r;
    auto raw_segment_buffer_l = raw_buffer + raw_offset_l;
    auto leaf_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
    auto leaf = new (leaf_buffer) LeafNode;
    uint8_t metadata_node_version = 0, segment_node_versions_r = 0, segment_node_versions_l = 0;
    if (!VerMng::decode_header_versions(raw_buffer, leaf_buffer, metadata_node_version) ||
        (segment_size_l > 0 && !VerMng::decode_segment_versions(raw_segment_buffer_l, (char*)&(leaf->records[0]), first_offset_l, segment_size_l, segment_node_versions_l)) ||
        !VerMng::decode_segment_versions(raw_segment_buffer_r, (char*)&(leaf->records[hash_idx]), first_offset_r, segment_size_r, segment_node_versions_r) ||
        metadata_node_version != segment_node_versions_r ||
        (segment_size_l > 0 && metadata_node_version != segment_node_versions_l)) {
      leaves.clear();
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_fetch;
    }
    leaves.emplace_back(leaf);
  }
  // update syn_leaf_addrs
  if (update_local_slt) for (int i = 0; i < (int)leaf_addrs.size(); ++ i) {
    if (leaves[i]->metadata.synonym_ptr != GlobalAddress::Null()) {
      syn_leaf_addrs[leaf_addrs[i]] = leaves[i]->metadata.synonym_ptr;
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
    std::vector<RdmaOpRegion> rs(3);
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
    MetadataManager::encode_segment_metadata((char *)&entry, intermediate_segment_buffer, first_metadata_offset, 1,
                                              LeafMetadata(metadata.h_version, 0, metadata.valid, metadata.sibling_ptr, metadata.fence_keys));
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
  for (int i = 0; i > MAX_APP_THREAD; ++ i) range_cnt[i].clear();
}
