#if !defined(_TREE_H_)
#define _TREE_H_

#include "TreeCache.h"
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


/* Tree */
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


class Tree {
public:
  Tree(DSM *dsm, uint16_t tree_id = 0);

  using WorkFunc = std::function<void (Tree *, const Request&, CoroPull *)>;
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

  // cache
  void record_cache_hit_ratio(bool from_cache, int level);
  void cache_node(InternalNode* node);

  // lock
  static std::pair<uint64_t, uint64_t> get_lock_info(const GlobalAddress &node_addr, bool is_leaf);
  static uint64_t get_unlock_info(const GlobalAddress &node_addr, bool is_leaf);
  void lock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink);
  void unlock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink, bool async = false);

  // search
  bool leaf_node_search(const GlobalAddress& node_addr, const GlobalAddress& sibling_addr, const Key &k, Value &v, bool from_cache, CoroPull* sink);
  bool internal_node_search(GlobalAddress& node_addr, GlobalAddress& sibling_addr, const Key &k, uint16_t& level, bool from_cache, CoroPull* sink);

  // insert
  bool leaf_node_insert(const GlobalAddress& node_addr, const GlobalAddress& sibling_addr, const Key &k, Value v, bool from_cache, CoroPull* sink);
  bool internal_node_insert(const GlobalAddress& node_addr, const Key &k, const GlobalAddress &v, bool from_cache, uint8_t level, CoroPull* sink);

  // update
  bool leaf_node_update(const GlobalAddress& node_addr, const GlobalAddress& sibling_addr, const Key &k, Value v, bool from_cache, CoroPull* sink);

  // lower-level function
  void leaf_entry_read(const GlobalAddress& node_addr, const int idx, char *raw_leaf_buffer, char *leaf_buffer, CoroPull* sink, bool for_update=false);
  template <class NODE, class ENTRY, class VAL>
  void entry_write_and_unlock(NODE* node, const int idx, const Key& k, VAL v, const GlobalAddress& node_addr, CoroPull* sink);
  template <class NODE, class ENTRY, int TRANS_SIZE>
  void node_write_and_unlock(NODE* node, const GlobalAddress& node_addr, CoroPull* sink);
  void segment_write_and_unlock(LeafNode* leaf, int l_idx, int r_idx, const std::vector<int>& hopped_idxes, const GlobalAddress& node_addr, CoroPull* sink);

  template <class NODE, class ENTRY, class VAL, int SPAN_SIZE, int ALLOC_SIZE, int TRANS_SIZE>
  void node_split_and_unlock(NODE* node, const Key& k, VAL v, const GlobalAddress& node_addr, uint8_t level, CoroPull* sink);
  void insert_internal(const Key &k, const GlobalAddress& ptr, const RootEntry& root_entry, uint8_t target_level, CoroPull* sink);

  void coro_worker(CoroPull &sink, RequstGen *gen, WorkFunc work_func);

private:
  DSM *dsm;
  TreeCache *tree_cache;
  LocalLockTable *local_lock_table;

  static thread_local std::vector<CoroPush> workers;
  static thread_local CoroQueue busy_waiting_queue;

  uint64_t tree_id;
  std::atomic<uint16_t> rough_height;
  GlobalAddress root_ptr_ptr;  // the address which stores root pointer;
};


#endif // _TREE_H_
