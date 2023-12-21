#include "Rolex.h"
#include "RdmaBuffer.h"
#include "Timer.h"
#include "LeafNode.h"
#include "InternalNode.h"
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

double cache_miss[MAX_APP_THREAD];
double cache_hit[MAX_APP_THREAD];
uint64_t lock_fail[MAX_APP_THREAD];
uint64_t write_handover_num[MAX_APP_THREAD];
uint64_t try_write_op[MAX_APP_THREAD];
uint64_t read_handover_num[MAX_APP_THREAD];
uint64_t try_read_op[MAX_APP_THREAD];
uint64_t read_leaf_retry[MAX_APP_THREAD];
uint64_t leaf_cache_invalid[MAX_APP_THREAD];
uint64_t leaf_read_sibling[MAX_APP_THREAD];
uint64_t try_read_leaf[MAX_APP_THREAD];
uint64_t retry_cnt[MAX_APP_THREAD][MAX_FLAG_NUM];

uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
volatile bool need_stop = false;
volatile bool need_clear[MAX_APP_THREAD];

thread_local std::vector<CoroPush> Rolex::workers;
thread_local CoroQueue Rolex::busy_waiting_queue;


Rolex::Rolex(DSM *dsm, std::vector<Key> &load_keys, uint16_t rolex_id) : dsm(dsm), rolex_id(rolex_id) {
  assert(dsm->is_register());
  std::fill(need_clear, need_clear + MAX_APP_THREAD, false);
  clear_debug_info();
  // Cache
  rolex_cache = new RolexCache(dsm, load_keys);
  // RDWC
  local_lock_table = new LocalLockTable();
}


inline void Rolex::before_operation(CoroPull* sink) {
  auto tid = dsm->getMyThreadID();
  if (need_clear[tid]) {
    cache_miss[tid]              = 0;
    cache_hit[tid]               = 0;
    lock_fail[tid]               = 0;
    write_handover_num[tid]      = 0;
    try_write_op[tid]            = 0;
    read_handover_num[tid]       = 0;
    try_read_op[tid]             = 0;
    read_leaf_retry[tid]         = 0;
    leaf_cache_invalid[tid]      = 0;
    leaf_read_sibling[tid]       = 0;
    try_read_leaf[tid]           = 0;
    std::fill(retry_cnt[tid], retry_cnt[tid] + MAX_FLAG_NUM, 0);
    need_clear[tid]              = false;
  }
}


inline std::pair<uint64_t, uint64_t> Rolex::get_lock_info(const GlobalAddress &node_addr, bool is_leaf) {
  auto lock_offset = get_unlock_info(node_addr, is_leaf);

  if (is_leaf) {
    uint64_t leaf_lock_cas_offset     = ROUND_DOWN(lock_offset, 3);
    uint64_t leaf_lock_mask           = 1UL << ((lock_offset - leaf_lock_cas_offset) * 8UL);
    return std::make_pair(leaf_lock_cas_offset, leaf_lock_mask);
  }
  else {
    uint64_t internal_lock_cas_offset = ROUND_DOWN(lock_offset, 3);
    uint64_t internal_lock_mask       = 1UL << ((lock_offset - internal_lock_cas_offset) * 8UL);
    return std::make_pair(internal_lock_cas_offset, internal_lock_mask);
  }
}


inline uint64_t Rolex::get_unlock_info(const GlobalAddress &node_addr, bool is_leaf) {
  static const uint64_t internal_lock_offset     = ADD_CACHELINE_VERSION_SIZE(sizeof(InternalNode), define::versionSize);
  static const uint64_t leaf_lock_offset         = ADD_CACHELINE_VERSION_SIZE(sizeof(LeafNode), define::versionSize);
  return (is_leaf ? leaf_lock_offset : internal_lock_offset) + get_hashed_remote_lock_index(node_addr) * 8UL;
}


void Rolex::lock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink) {
  auto [lock_cas_offset, lock_mask] = get_lock_info(node_addr, is_leaf);
  auto cas_buffer = (dsm->get_rbuf(sink)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &node_addr) {
    return dsm->cas_mask_sync_without_sink(node_addr + lock_cas_offset, 0UL, ~0UL, cas_buffer, lock_mask, sink, &busy_waiting_queue);
  };
re_acquire:
  if (!acquire_lock(node_addr)){
    // if (sink != nullptr) {
    //   busy_waiting_queue.push(sink->get());
    //   (*sink)();
    // }
    lock_fail[dsm->getMyThreadID()] ++;
    goto re_acquire;
  }
  return;
}

void Rolex::unlock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink, bool async) {
  auto lock_offset = get_unlock_info(node_addr, is_leaf);
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


void Rolex::insert(const Key &k, Value v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  // TODO
}


void Rolex::update(const Key &k, Value v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  // TODO
}


bool Rolex::search(const Key &k, Value &v, CoroPull* sink) {
  assert(dsm->is_register());
  before_operation(sink);

  // TODO
}


/*
  range query
  DO NOT support coroutines currently
*/
bool Rolex::range_query(const Key &from, const Key &to, std::map<Key, Value> &ret) {  // [from, to)
  assert(dsm->is_register());
  before_operation(nullptr);

  // TODO
}


void Rolex::run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req, int req_num) {
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


void Rolex::coro_worker(CoroPull &sink, RequstGen *gen, WorkFunc work_func) {
  Timer coro_timer;
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

void Rolex::statistics() {
  rolex_cache->statistics();
}

void Rolex::clear_debug_info() {
  memset(cache_miss, 0, sizeof(double) * MAX_APP_THREAD);
  memset(cache_hit, 0, sizeof(double) * MAX_APP_THREAD);
  memset(lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(write_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_leaf_retry, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(leaf_cache_invalid, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_leaf, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(retry_cnt, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_FLAG_NUM);
}
