#if !defined(_TREE_H_)
#define _TREE_H_

#include "DSM.h"
#include "Common.h"
#include "LocalLockTable.h"
#include "LeafVersionManager.h"
#include "VersionManager.h"

#include <atomic>
#include <city.h>
#include <functional>
#include <map>
#include <algorithm>
#include <queue>
#include <set>
#include <iostream>


/* Workloads */
enum RequestType : int {
  INSERT = 0,
  UPDATE,
  SEARCH,
  SCAN
};

struct Request {
  RequestType req_type;
  Key k;
  Value v;
  int range_size;
};


class RequstGen {
public:
  RequstGen() = default;
  virtual Request next() { return Request{}; }
};


/* Rolex */
using GenFunc = std::function<RequstGen *(DSM*, Request*, int, int, int)>;
#define MAX_FLAG_NUM 4
enum {
  FIRST_TRY,
  INVALID_LEAF,
  INVALID_NODE,
  FIND_NEXT
};


class RootEntry {
public:
  uint16_t level;
  PackedGAddr ptr;

  RootEntry(const uint16_t level, const GlobalAddress& ptr) : level(level), ptr(ptr) {}
  RootEntry(uint64_t val) : level(val & ((1UL << define::packedGaddrBit) - 1)), ptr(val >> define::packedGaddrBit) {}

  operator uint64_t() const { return ((uint64_t)ptr << 16) | level; }
  operator std::pair<uint16_t, GlobalAddress>() const { return std::make_pair(level, (GlobalAddress)ptr); }
} __attribute__((packed));

static_assert(sizeof(RootEntry) == 8);


class Rolex {
public:
  Rolex(DSM *dsm, uint16_t rolex_id = 0);

  using WorkFunc = std::function<void (Rolex *, const Request&, CoroPull *)>;
  void run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req = nullptr, int req_num = 0);

  void insert(const Key &k, Value v, CoroPull* sink = nullptr);   // NOTE: insert can also do update things if key exists
  void update(const Key &k, Value v, CoroPull* sink = nullptr);   // assert(false) if key is not found
  bool search(const Key &k, Value &v, CoroPull* sink = nullptr);  // return false if key is not found
  bool range_query(const Key &from, const Key &to, std::map<Key, Value> &ret);
  void statistics();
  void clear_debug_info();

private:
  // common
  void before_operation(CoroPull* sink);
  GlobalAddress get_root_ptr_ptr();
  RootEntry get_root_ptr(CoroPull* sink);

  // lock
  static std::pair<uint64_t, uint64_t> get_lock_info(const GlobalAddress &node_addr, bool is_leaf);
  static uint64_t get_unlock_info(const GlobalAddress &node_addr, bool is_leaf);
  void lock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink);
  void unlock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink, bool async = false);

  // coroutine
  void coro_worker(CoroPull &sink, RequstGen *gen, WorkFunc work_func);

private:
  DSM *dsm;
  LocalLockTable *local_lock_table;

  static thread_local std::vector<CoroPush> workers;
  static thread_local CoroQueue busy_waiting_queue;

  uint64_t rolex_id;
  GlobalAddress root_ptr_ptr;  // the address which stores root pointer;
};


#endif // _TREE_H_
