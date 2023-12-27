#if !defined(_LEAF_NODE_H_)
#define _LEAF_NODE_H_

#include "Metadata.h"


/* Leaf Metadata: [obj_version, fence keys, sibling pointer] */
class LeafMetadata {
public:
  PackedVersion h_version;
  // metadata
  GlobalAddress synonym_ptr;

public:
  LeafMetadata() : h_version(), synonym_ptr() {}
  LeafMetadata(PackedVersion h_version, GlobalAddress synonym_ptr) : h_version(h_version), synonym_ptr(synonym_ptr) {}
} __attribute__((packed));

static_assert(sizeof(LeafMetadata) == define::leafMetadataSize);

inline bool operator==(const LeafMetadata &lhs, const LeafMetadata &rhs) {
  return !strncmp((char *)&lhs, (char*)&rhs, sizeof(LeafMetadata));
}


/* Leaf Entry: [obj_version, key, value] */
class LeafEntry {
public:
  PackedVersion h_version;
#ifdef HOPSCOTCH_LEAF_NODE
  uint8_t hop_bitmap : define::hopRange;
#endif
  // kv
  Key key;
  union {
  Value value;
  uint8_t _padding[define::simulatedValLen];
  };

public:
#ifdef HOPSCOTCH_LEAF_NODE
  LeafEntry() : h_version(), hop_bitmap(0U), key(define::kkeyNull), value(define::kValueNull) {}
  LeafEntry(const Key& k, const Value& v) : h_version(), hop_bitmap(0U), key(k), value(v) {}
#else
  LeafEntry() : h_version(), key(define::kkeyNull), value(define::kValueNull) {}
  LeafEntry(const Key& k, const Value& v) : h_version(), key(k), value(v) {}
#endif

  void update(const Key& k, const Value& v) { key = k, value = v; }
#ifdef HOPSCOTCH_LEAF_NODE
  void set_hop_bit(int idx) {
    assert(idx >= 0 && idx < (int)define::hopRange && !(hop_bitmap & (1U << (define::hopRange - idx - 1))));
    hop_bitmap |= 1U << (define::hopRange - idx - 1);
  }
  void unset_hop_bit(int idx) {
    assert(idx >= 0 && idx < (int)define::hopRange && (hop_bitmap & (1U << (define::hopRange - idx - 1))));
    hop_bitmap &= ~(1U << (define::hopRange - idx - 1));
  }
#endif

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

  static uint64_t max_slot() { return define::leafSpanSize; }
} __attribute__((packed));

static_assert(sizeof(LeafNode) == sizeof(LeafMetadata) + sizeof(LeafEntry) * define::leafSpanSize);


inline bool operator==(const LeafNode &lhs, const LeafNode &rhs) {
  return !strncmp((char *)&lhs, (char*)&rhs, sizeof(LeafNode));
}


/* -------------Auxiliary Structures------------- */
class LeafEntryGroup {
public:
  LeafMetadata metadata;
  LeafEntry records[define::hopRange];
} __attribute__((packed));

static_assert(sizeof(LeafEntryGroup) == sizeof(LeafMetadata) + sizeof(LeafEntry) * define::hopRange);


/* Scattered Leaf Node: [lock, leaf metadata, [scattered metadata, KV, KV...] * n] */
class ScatteredLeafNode {  // must be cacheline-align
public:
  LeafEntryGroup record_groups[define::entryGroupNum];
} __attribute__((packed));

static_assert(sizeof(ScatteredLeafNode) == sizeof(LeafEntryGroup) * define::entryGroupNum);

#endif // _LEAF_NODE_H_

#endif // _LEAF_NODE_H_
