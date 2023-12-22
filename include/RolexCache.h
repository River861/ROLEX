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


using leaf_alloc_t = LeafAllocator<LeafNode, sizeof(LeafNode)>;
using model_alloc_t = ModelAllocator<Key>;
using remote_memory_t = RemoteMemory<leaf_alloc_t, model_alloc_t>;
using leaf_table_t = LeafTable<Key, Value, LeafNode, leaf_alloc_t>;
using rolex_t = Rolex<Key, Value, LeafNode, leaf_alloc_t, remote_memory_t, define::epsilon>;


class RolexCache {

public:
  RolexCache(DSM* dsm, std::vector<Key> &load_keys);

  std::pair<int, int> search_from_cache(const Key &k);
  std::pair<int, int> search_range_from_cache(const Key &from, const Key &to);
  void statistics();

private:
  DSM *dsm;
  rolex_t* rolex_model;
};

inline RolexCache::RolexCache(DSM* dsm, std::vector<Key> &load_keys) : dsm(dsm) {
  // processing data
  std::sort(load_keys.begin(), load_keys.end());
  load_keys.erase(std::unique(load_keys.begin(), load_keys.end()), load_keys.end());
  std::sort(load_keys.begin(), load_keys.end());
  for(int i = 1; i < load_keys.size(); ++ i){
    assert(load_keys[i] >= load_keys[i - 1]);
  }

  // initial local models
  rolex::RCtrl* ctrl = new RCtrl(define::fakePort);
  rolex::RM_config conf(ctrl, define::modelRegionSize, define::fakeLeafRegionSize, define::fakeRegLeafRegion, define::preAllocLeafNum);
  remote_memory_t* RM = new remote_memory_t(conf);
  rolex_model = new rolex_t(RM, load_keys, load_keys);
}


inline std::pair<int, int> RolexCache::search_from_cache(const Key &k) {
  return rolex_model->get_leaf_range(k);
}


inline std::pair<int, int> RolexCache::search_range_from_cache(const Key &from, const Key &to) {
  auto [l, _]  = rolex_model->get_leaf_range(from);
  auto [__, r] = rolex_model->get_leaf_range(to);
  return std::make_pair(l, r);
}


inline void RolexCache::statistics() {
  printf(" ----- [RolexCache] ----- \n");
  auto cache_size = rolex_model->get_consumed_cache_size();
  printf("comsumed cache size=%.3lf MB\n", cache_size);
}

#endif // _ROLEX_CACHE_H_
