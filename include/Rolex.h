#if !defined(_TREE_H_)
#define _TREE_H_

#include "DSM.h"
#include "Common.h"
#include "LocalLockTable.h"
#include "VersionManager.h"
#include "RolexCache.h"

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


class Rolex {
public:
  Rolex(DSM *dsm, std::vector<Key> &load_keys, uint16_t rolex_id = 0);

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
  void get_leaf_addresses(int l, int r, std::vector<GlobalAddress>& addrs);

  // lock
  static std::pair<uint64_t, uint64_t> get_lock_info(const GlobalAddress &node_addr, bool is_leaf);
  static uint64_t get_unlock_info(const GlobalAddress &node_addr, bool is_leaf);
  void lock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink);
  void unlock_node(const GlobalAddress &node_addr, bool is_leaf, CoroPull* sink, bool async = false);

  // coroutine
  void coro_worker(CoroPull &sink, RequstGen *gen, WorkFunc work_func);

private:
  DSM *dsm;
  RolexCache* rolex_cache;
  LocalLockTable *local_lock_table;

  static thread_local std::vector<CoroPush> workers;
  static thread_local CoroQueue busy_waiting_queue;

  uint64_t rolex_id;
};


#endif // _TREE_H_
