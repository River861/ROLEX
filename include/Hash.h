#if !defined(_HASH_H_)
#define _HASH_H_

#include "Common.h"
#include "Key.h"
#include "GlobalAddress.h"
#include "city.h"


inline uint32_t murmurhash(const void* key, int len, uint32_t seed) {
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t r1 = 15;
    const uint32_t r2 = 13;
    const uint32_t m = 5;
    const uint32_t n = 0xe6546b64;
 
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;
 
    uint32_t hash = seed;
 
    for (int i = 0; i < nblocks; i++) {
        uint32_t k = *(uint32_t*)(data + i * 4);
 
        k *= c1;
        k = (k << r1) | (k >> (32 - r1));
        k *= c2;
 
        hash ^= k;
        hash = (hash << r2) | (hash >> (32 - r2));
        hash = hash * m + n;
    }
 
    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);
    uint32_t k = 0;
 
    switch (len & 3) {
        case 3:
            k ^= tail[2] << 16;
        case 2:
            k ^= tail[1] << 8;
        case 1:
            k ^= tail[0];
            k *= c1;
            k = (k << r1) | (k >> (32 - r1));
            k *= c2;
            hash ^= k;
    }
 
    hash ^= len;
    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >> 16;
    return hash;
}


inline uint64_t get_hashed_local_lock_index(const Key& k) {
  // return CityHash64((char *)&k, sizeof(k)) % define::kLocalLockNum;
  uint64_t res = 0, cnt = 0;
  for (auto a : k) if (cnt ++ < 8) res = (res << 8) + a;
  return res % define::kLocalLockNum;
}


inline uint64_t get_hashed_local_lock_index(const GlobalAddress& addr) {
  // return CityHash64((char *)&addr, sizeof(addr)) % define::kLocalLockNum;
  return addr.to_uint64() % define::kLocalLockNum;
}

inline uint64_t get_hashed_leaf_entry_index(const Key& k) {
  // return CityHash64((char *)&k, sizeof(k)) % define::leafSpanSize;
  return (k.back() % 2) ? (CityHash64((char *)&k, sizeof(k)) % define::leafSpanSize) : ((uint64_t)murmurhash((char *)&k, sizeof(k), 0x716716) % define::leafSpanSize);
}

#define HASH_TABLE_SIZE 1000000
#define HASH_BUCKET_SIZE 16

inline uint64_t get_hashed_cache_table_index(const GlobalAddress& leaf_addr, int kv_idx) {
  // auto info = std::make_pair(leaf_addr, kv_idx);
  // return CityHash64((char *)&info, sizeof(info)) % HASH_TABLE_SIZE;
  return (leaf_addr.to_uint64() + kv_idx) % HASH_TABLE_SIZE;
}

#endif // _HASH_H_
