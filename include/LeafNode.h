#if !defined(_LEAF_NODE_H_)
#define _LEAF_NODE_H_

#include "Metadata.h"


/* Leaf Metadata: [obj_version, fence keys, sibling pointer] */
class LeafMetadata {
public:
  PackedVersion h_version;
  // metadata
  uint8_t level;  // always 0
  uint8_t valid;
  GlobalAddress sibling_ptr;
  FenceKeys fence_keys;

public:
  LeafMetadata() : h_version(), level(0), valid(1), sibling_ptr(), fence_keys() {}
  LeafMetadata(PackedVersion h_version, uint8_t level, uint8_t valid, GlobalAddress sibling_ptr, FenceKeys fence_keys) : h_version(h_version), level(level), valid(valid), sibling_ptr(sibling_ptr), fence_keys(fence_keys) {}
} __attribute__((packed));

static_assert(sizeof(LeafMetadata) == define::leafMetadataSize);

inline bool operator==(const LeafMetadata &lhs, const LeafMetadata &rhs) {
  return !strncmp((char *)&lhs, (char*)&rhs, sizeof(LeafMetadata));
}


/* Leaf Entry: [obj_version, key, value] */
class LeafEntry {
public:
  PackedVersion h_version;
  // kv
  Key key;
  union {
  Value value;
  uint8_t _padding[define::simulatedValLen];
  };

public:
  LeafEntry() : h_version(), key(define::kkeyNull), value(define::kValueNull) {}
  LeafEntry(const Key& k, const Value& v) : h_version(), key(k), value(v) {}

  void update(const Key& k, const Value& v) { key = k, value = v; }

  static LeafEntry Null() {
    static LeafEntry _zero;
    return _zero;
  };
} __attribute__((packed));

static_assert(sizeof(LeafEntry) == define::leafEntrySize);


/* Leaf Node: [lock, leaf metadata, [KV, ...]] */
class LeafNode {  // must be cacheline-align
public:
  // cacheline-versions will be embedded from here with an 64-byte offset (can be skipped if obj version is here)
  LeafMetadata metadata;
  LeafEntry records[define::leafSpanSize];

public:
  LeafNode() : metadata(), records{} {}

  bool is_root() const {
    return metadata.fence_keys == FenceKeys::Widest();
  }

  static const bool IS_LEAF = true;
} __attribute__((packed));

static_assert(sizeof(LeafNode) == sizeof(LeafMetadata) + sizeof(LeafEntry) * define::leafSpanSize);


inline bool operator==(const LeafNode &lhs, const LeafNode &rhs) {
  return !strncmp((char *)&lhs, (char*)&rhs, sizeof(LeafNode));
}

/* -------------Auxiliary Structures------------- */
class ScatteredMetadata {
public:
  PackedVersion h_version;
  uint8_t valid;
  GlobalAddress sibling_ptr;
  FenceKeys fence_keys;

  ScatteredMetadata(const LeafMetadata& metadata): h_version(metadata.h_version), valid(1), sibling_ptr(metadata.sibling_ptr), fence_keys(metadata.fence_keys) {}
} __attribute__((packed));

static_assert(sizeof(ScatteredMetadata) == define::scatterMetadataSize);

inline bool operator==(const ScatteredMetadata &lhs, const ScatteredMetadata &rhs) {
  return (lhs.sibling_ptr == rhs.sibling_ptr) && (lhs.fence_keys == rhs.fence_keys);
}


class LeafEntryGroup {
public:
  ScatteredMetadata metadata;
  LeafEntry records[define::hopRange];
} __attribute__((packed));

static_assert(sizeof(LeafEntryGroup) == sizeof(ScatteredMetadata) + sizeof(LeafEntry) * define::hopRange);


/* Scattered Leaf Node: [lock, leaf metadata, [scattered metadata, KV, KV...] * n] */
class ScatteredLeafNode {  // must be cacheline-align
public:
  LeafEntryGroup record_groups[define::entryGroupNum];
} __attribute__((packed));

static_assert(sizeof(ScatteredLeafNode) == sizeof(LeafEntryGroup) * define::entryGroupNum);

#endif // _LEAF_NODE_H_
