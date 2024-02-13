#if !defined(_METADATA_H_)
#define _METADATA_H_

#include "Common.h"
#include "GlobalAddress.h"
#include "Key.h"


/* Packed Global Address */
class PackedGAddr {  // 48-bit, used by node addr/leaf addr (not entry addr)
public:
  uint64_t mn_id    : define::mnIdBit;
  uint64_t offset   : define::offsetBit;

  PackedGAddr() : mn_id(0), offset(0) {}
  PackedGAddr(const GlobalAddress &addr) : mn_id(addr.nodeID), offset(addr.offset >> PACKED_ADDR_ALIGN_BIT) {}
  operator GlobalAddress() const { return GlobalAddress{mn_id, offset << PACKED_ADDR_ALIGN_BIT}; }

  PackedGAddr(uint64_t val) : mn_id(val & ((1UL << define::mnIdBit) - 1)), offset(val >> define::mnIdBit) {}
  operator uint64_t() const { return (offset << define::mnIdBit) | mn_id; }

} __attribute__((packed));

static_assert(sizeof(PackedGAddr) == define::packedGAddrSize);

inline bool operator==(const PackedGAddr &lhs, const PackedGAddr &rhs) {
  return (lhs.mn_id == rhs.mn_id) && (lhs.offset == rhs.offset);
}


#ifdef INFOMATION_EMBEDDED_LOCK
class InfoLock {
public:
  uint64_t vacancy_bitmap          : define::vacancyMapBit;
  uint64_t synonym_vacancy_bitmap  : define::vacancyMapBit;
  uint64_t _padding        : 1;
  uint64_t lock_bit        : 1;

public:
  InfoLock(uint64_t vacancy_bitmap, uint64_t synonym_vacancy_bitmap) : vacancy_bitmap(vacancy_bitmap), synonym_vacancy_bitmap(synonym_vacancy_bitmap), lock_bit(0) {}

  void update_vacancy(int l, int r, const std::vector<int>& empty_idxes, bool is_synonym=false) {  // [l, r]
    int span_size = define::leafSpanSize;
    int l_bit = find_bucket(l, span_size), r_bit = find_bucket(r, span_size);

    auto set_bit = [&](int i){
      if (is_synonym) synonym_vacancy_bitmap |= (1ULL << i);
      else vacancy_bitmap |= (1ULL << i);
    };

    if (l_bit <= r_bit) for (int i = l_bit; i <= r_bit; ++ i) set_bit(i);
    else {
      for (int i = 0; i <= r_bit; ++ i) set_bit(i);
      for (int i = l_bit; i < (int)define::vacancyMapBit; ++ i) set_bit(i);
    }
    for (int empty_idx : empty_idxes) {
      int i = find_bucket(empty_idx, span_size);
      if (l_bit <= r_bit) assert(i >= l_bit && i <= r_bit);
      else assert((i >= 0 && i <= r_bit) || (i >= l_bit && i < (int)define::vacancyMapBit));
      if (is_synonym) synonym_vacancy_bitmap &= ~(1ULL << i);
      else vacancy_bitmap &= ~(1ULL << i);
    }
  }

  int get_read_entry_num_from_bitmap(int start_idx, bool is_synonym=false) {
    int span_size = define::leafSpanSize;
    int s_bit = find_bucket(start_idx, span_size);

    for (int i = 0; i < (int)define::vacancyMapBit; ++ i) {
      int e_bit = (s_bit + i) % define::vacancyMapBit;
      bool is_empty;
      if (is_synonym) is_empty = !(synonym_vacancy_bitmap & (1ULL << e_bit));
      else is_empty = !(vacancy_bitmap & (1ULL << e_bit));
      if (is_empty) {  // empty
        auto [l_idx, r_idx] = get_idx_range_in_bucket(e_bit, span_size);  // [l_idx, r_idx]
        if (start_idx > l_idx) continue;  // !!Important
        int entry_num = (r_idx + span_size - start_idx + 1) % span_size;
        return entry_num ? entry_num : span_size;
      }
    }
    return span_size;
  }

private:
  int find_bucket(int kv_idx, int span_size) {  // 0~span_size => 0~bitmap_size
    double avg_per_bucket = static_cast<double>(span_size) / define::vacancyMapBit;
    int bucket = static_cast<int>(floor(kv_idx / avg_per_bucket));
    return bucket % define::vacancyMapBit;
  }

  std::pair<int, int> get_idx_range_in_bucket(int bucket, int span_size) {  // 0~bitmap_size => 0~span_size
    double avg_per_bucket = static_cast<double>(span_size) / define::vacancyMapBit;
    int l_idx = bucket ? static_cast<int>(floor(bucket * avg_per_bucket)) + 1 : 0;
    int r_idx = static_cast<int>(floor((bucket + 1) * avg_per_bucket));
    if (r_idx >= span_size) r_idx = span_size - 1;
    return std::make_pair(l_idx, r_idx);
  }
} __attribute__((packed));

static_assert(sizeof(InfoLock) == sizeof(uint64_t));
#endif


#ifdef ENABLE_VAR_LEN_KV
class DataPointer {
public:
  uint64_t    data_len : 64 - define::packedGaddrBit;
  PackedGAddr ptr;

  DataPointer(const uint64_t data_len, const GlobalAddress& ptr) : data_len(data_len), ptr(ptr) {}

  operator uint64_t() const { return ((uint64_t)ptr << 16) | data_len; }
  operator std::pair<uint64_t, GlobalAddress>() const { return std::make_pair(data_len, (GlobalAddress)ptr); }
} __attribute__((packed));

static_assert(sizeof(DataPointer) == 8);


class DataBlock {  // For brievity, we assume the whole key can be stored inline, and only changing the value size for evaluation
public:
  uint64_t rest_of_key_len = 0;
  uint64_t value_len;
  // Key rest_of_key;
  union {
  Value value;
  uint8_t _padding[define::indirectValLen];
  };

  DataBlock(const Value value) : rest_of_key_len(0), value_len(define::indirectValLen), value(value) {}
} __attribute__((packed));

static_assert(sizeof(DataBlock) == define::dataBlockLen);
#endif


/* Fence Keys */
struct FenceKeys {
  Key lowest;
  Key highest;

  FenceKeys() : lowest{} { lowest = lowest + define::kKeyMin; highest.fill(0xff); }
  FenceKeys(const Key& lowest, const Key& highest) : lowest(lowest), highest(highest) {}

  static FenceKeys Widest() {
    return FenceKeys();
  };
} __attribute__((packed));

static_assert(sizeof(FenceKeys) == define::keyLen * 2);

inline bool operator==(const FenceKeys &lhs, const FenceKeys &rhs) {
  return (lhs.lowest == rhs.lowest) && (lhs.highest == rhs.highest);
}

inline std::ostream &operator<<(std::ostream &os, const FenceKeys &obj) {
  os << "[" << key2int(obj.lowest) << ", " << key2int(obj.highest) << "]";
  return os;
}


/* Packed Versions */
struct PackedVersion {
  uint8_t node_version  : define::nodeVersionBit;
  uint8_t entry_version : define::entryVersionBit;

  PackedVersion() : node_version(0), entry_version(0) {}
} __attribute__((packed));

static_assert(sizeof(PackedVersion) == define::versionSize);


inline bool operator==(const PackedVersion &lhs, const PackedVersion &rhs) {
  return (lhs.entry_version == rhs.entry_version) && (lhs.node_version == rhs.node_version);
}

inline bool operator!=(const PackedVersion &lhs, const PackedVersion &rhs) {
  return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const PackedVersion &obj) {
  os << "[" << (int)obj.node_version << ", " << (int)obj.entry_version << "]";
  return os;
}

#endif // _METADATA_H_
