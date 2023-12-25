#if !defined(_ROLEX_CACHE_H_)
#define _ROLEX_CACHE_H_

#include "HugePageAlloc.h"
#include "Timer.h"
#include "DSM.h"

#include "rolex_libs/rolex/leaf_allocator.hpp"
#include "rolex_libs/rolex/model_allocator.hpp"
#include "rolex_libs/rolex/leaf_table.hpp"
#include "rolex_libs/rolex/rolex.hpp"
#include "rolex_libs/rolex/learned_cache.hpp"
#include "rolex_libs/rolex/remote_memory.hh"
#include "rolex_libs/rolex/remote_memory.hh"

#include <queue>
#include <atomic>
#include <vector>

using leaf_alloc_t = rolex::LeafAllocator<LeafNode, sizeof(LeafNode)>;
using model_alloc_t = rolex::ModelAllocator<__uint128_t>;
using remote_memory_t = rolex::RemoteMemory<leaf_alloc_t, model_alloc_t>;
using leaf_table_t = rolex::LeafTable<__uint128_t, Value, LeafNode, leaf_alloc_t>;
using rolex_t = rolex::Rolex<__uint128_t, Value, LeafNode, leaf_alloc_t, remote_memory_t, define::epsilon>;


class RolexCache {

public:
  RolexCache(DSM* dsm, const std::vector<Key> &load_keys);

  std::pair<int, int> search_from_cache(const Key &k);
  std::tuple<int, int, int> search_from_cache_for_insert(const Key &k);
  std::pair<int, int> search_range_from_cache(const Key &from, const Key &to);
  void statistics();

private:
  DSM *dsm;
  rolex_t* rolex_model;
  std::vector<__uint128_t> int128_keys;
};

inline RolexCache::RolexCache(DSM* dsm, const std::vector<Key> &load_keys) : dsm(dsm) {
  // processing data
  for (const auto& k : load_keys) int128_keys.emplace_back(key2int128(k));
  std::sort(int128_keys.begin(), int128_keys.end());
  int128_keys.erase(std::unique(int128_keys.begin(), int128_keys.end()), int128_keys.end());
  std::sort(int128_keys.begin(), int128_keys.end());
  for(int i = 1; i < int128_keys.size(); ++ i){
    assert(int128_keys[i] >= int128_keys[i - 1]);
  }

  // initial local models
  rolex::RCtrl* ctrl = new RCtrl(define::fakePort);
  rolex::RM_config conf(ctrl, define::modelRegionSize, define::fakeLeafRegionSize, define::fakeRegLeafRegion);
  remote_memory_t* RM = new remote_memory_t(conf);
  rolex_model = new rolex_t(RM, int128_keys, int128_keys);
}


inline std::pair<int, int> RolexCache::search_from_cache(const Key &k) {  // [l, r]
  auto key = key2int128(k);
  auto [l, r, leaf_idx_offset, _] = rolex_model->get_leaf_range(key);
  return std::make_tuple(leaf_idx_offset + l, leaf_idx_offset + r);
}


inline std::tuple<int, int, int> RolexCache::search_from_cache_for_insert(const Key &k) {  // [l, r, insert_leaf_idx]
  auto key = key2int128(k);
  auto [l, r, leaf_idx_offset, capacity_offset] = rolex_model->get_leaf_range(key);
  int global_key_idx = std::lower_bound(int128_keys.begin(), int128_keys.end(), key) - int128_keys.begin();
  int insert_idx = l + (global_key_idx - capacity_offset) / define::leafSpanSize;
  assert(insert_idx >= l && insert_idx <= r);
  return std::make_tuple(leaf_idx_offset + l, leaf_idx_offset + r, leaf_idx_offset + insert_idx);
}


inline std::pair<int, int> RolexCache::search_range_from_cache(const Key &from, const Key &to) {  // [l, r]
  auto from_key = key2int128(from);
  auto to_key = key2int128(to);
  auto [l, _1, leaf_idx_offset_from, _2]  = rolex_model->get_leaf_range(from_key);
  auto [_3, r, leaf_idx_offset_to  , _4] = rolex_model->get_leaf_range(to_key);
  return std::make_pair(leaf_idx_offset_from + l, leaf_idx_offset_to + r);
}

inline void RolexCache::statistics() {
  printf(" ----- [RolexCache] ----- \n");
  auto cache_size = rolex_model->get_consumed_cache_size();
  printf("comsumed cache size=%.3lf MB\n", cache_size);
}

#endif // _ROLEX_CACHE_H_
