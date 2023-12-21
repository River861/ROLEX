#if !defined(_ROLEX_CACHE_H_)
#define _ROLEX_CACHE_H_

#include "HugePageAlloc.h"
#include "Timer.h"
#include "DSM.h"

#include "rolex_libs/rolex/trait.hpp"
#include "rolex_libs/rolex/remote_memory.hh"

#include <queue>
#include <atomic>
#include <vector>


class RolexCache {

public:
  RolexCache(DSM* dsm, std::vector<Key> &load_keys);

  void search_from_cache(const Key &k);
  void search_range_from_cache(const Key &from, const Key &to);
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
  rolex::RCtrl* ctrl = new RCtrl(define::fake_port);
  rolex::RM_config conf(ctrl, define::model_region_size, define::fake_leaf_region_size, define::fake_reg_leaf_region, define::leaf_num);
  remote_memory_t* RM = new remote_memory_t(conf);
  rolex_model = new rolex_t(RM, load_keys, load_keys);
}


inline void RolexCache::search_from_cache(const Key &k) {

}


inline void RolexCache::search_range_from_cache(const Key &from, const Key &to) {

}


inline void RolexCache::statistics() {
  printf(" ----- [RolexCache] ----- \n");
  auto cache_size = rolex_model->get_consumed_cache_size();
  printf("comsumed cache size=%.3lf MB\n", cache_size);
}

#endif // _ROLEX_CACHE_H_
