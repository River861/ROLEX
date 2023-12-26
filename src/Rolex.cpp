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
  fetch_nodes(leaf_addrs, _, sink);

  // 2. Fine-grained locking and re-read
  GlobalAddress insert_leaf_addr = get_leaf_address(insert_idx);
  LeafNode* leaf = nullptr, *syn_leaf = nullptr;
  lock_node(insert_leaf_addr, sink);
  // re-read leaf + synonym leaf
  if (syn_leaf_addrs.find(insert_leaf_addr) == syn_leaf_addrs.end()) {
    fetch_node(insert_leaf_addr, leaf, sink, false);
    if (leaf->metadata.synonym_ptr != GlobalAddress::Null()) {
      leaf_read_syn[dsm->getMyThreadID()] ++;
      syn_leaf_addrs[insert_leaf_addr] = leaf->metadata.synonym_ptr;
      fetch_node(syn_leaf_addrs[insert_leaf_addr], syn_leaf, sink);
    }
  }
  else {
    std::vector<LeafNode*> two_leaves;
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
  }

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
  leaf = (LeafNode*) leaf_buffer;
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

  // handover
  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);

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
  auto [ret, leaf_addr, lock_leaf_addr] = _search(k, old_v, sink);
  UNUSED(old_v);
  // 2. Fine-grained locking and re-read
  lock_node(lock_leaf_addr, sink);
  LeafNode* leaf;
read_another:
  fetch_node(leaf_addr, leaf, sink);
  // 3. Update k locally
  assert(leaf != nullptr);
  auto& records = leaf->records;
  bool key_is_found = false;
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
  if (!key_is_found && leaf_addr == lock_leaf_addr) {  // key is moved to the synonym leaf
    assert(leaf->metadata.synonym_ptr != GlobalAddress::Null());
    leaf_addr = leaf->metadata.synonym_ptr;
    leaf_read_syn[dsm->getMyThreadID()] ++;
    goto read_another;
  }
  // 4. Writing and unlock
  write_node_and_unlock(leaf_addr, leaf, lock_leaf_addr, sink);
  }

update_finish:
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->release_local_write_lock(k, lock_res);
#endif
  return;
}


bool RolexIndex::search(const Key &k, Value &v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

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
  auto [ret, _1, _2] = _search(k, v, sink);
  search_res = ret;
  }

search_finish:
#ifdef TREE_ENABLE_READ_DELEGATION
  local_lock_table->release_local_read_lock(k, lock_res, search_res, v);  // handover the ret leaf addr
#endif
  return search_res;
}


std::tuple<bool, GlobalAddress, GlobalAddress> RolexIndex::_search(const Key &k, Value &v, CoroPull* sink) {
  // 1. Read predict leaves and the synonmy leaves
  auto [l, r] = rolex_cache->search_from_cache(k);
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
  fetch_nodes(leaf_addrs, leaves, sink, false);
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
    fetch_nodes(append_leaf_addrs, append_leaves, sink);
    leaf_addrs.insert(leaf_addrs.end(), append_leaf_addrs.begin(), append_leaf_addrs.end());
    leaves.insert(leaves.end(), append_leaves.begin(), append_leaves.end());
    locked_leaf_addrs.insert(locked_leaf_addrs.end(), append_locked_leaf_addrs.begin(), append_locked_leaf_addrs.end());
  }
  // 3. Search the fetched leaves
  assert(leaf_addrs.size() == leaves.size() && leaves.size() == locked_leaf_addrs.size());
  for (int i = 0; i < (int)leaves.size(); ++ i) {
    for (const auto& e : leaves[i]->records) {
      if (e.key == define::kkeyNull) break;
      if (e.key == k) {
        v = e.value;
        return std::make_tuple(true, leaf_addrs[i], locked_leaf_addrs[i]);
      }
    }
  }
  return std::make_tuple(false, GlobalAddress::Null(), GlobalAddress::Null());
}


/*
  range query
  DO NOT support coroutines currently
*/
void RolexIndex::range_query(const Key &from, const Key &to, std::map<Key, Value> &ret) {  // [from, to)
  assert(dsm->is_register());
  before_operation(nullptr);

  // 1. Read predict leaves and the synonmy leaves
  auto [l, r] = rolex_cache->search_range_from_cache(from, to);
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
      if (e.key == define::kkeyNull) break;
      if (e.key >= from && e.key < to) {
        ret[e.key] = e.value;
      }
    }
  }
  return;
}


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
}
