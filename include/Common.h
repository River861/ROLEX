#ifndef __COMMON_H__
#define __COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <queue>
#include <bitset>
#include <limits>

#include "Debug.h"
#include "HugePageAlloc.h"
#include "Rdma.h"

#include "WRLock.h"

// DEBUG-CACHE
#define TREE_ENABLE_CACHE
#define CACHE_MORE_INTERNAL_NODE
// #define NEED_CACHE_EVICTION

#define TREE_ENABLE_READ_DELEGATION
#define TREE_ENABLE_WRITE_COMBINING

// Environment Config
#define MAX_MACHINE 20
#define MEMORY_NODE_NUM 1
#define CPU_PHYSICAL_CORE_NUM 72  // [CONFIG]  72
#define MAX_CORO_NUM 8

#define LATENCY_WINDOWS 100000
#define PACKED_ADDR_ALIGN_BIT 8
#define CACHELINE_ALIGN_BIT 6
#define MAX_KEY_SPACE_SIZE 60000000
// #define KEY_SPACE_LIMIT
#define MESSAGE_SIZE 96 // byte
#define RAW_RECV_CQ_COUNT 4096 // 128
#define MAX_TREE_HEIGHT 20

// Auxiliary function
#define STRUCT_OFFSET(type, field)  ((char *)&((type *)(0))->field - (char *)((type *)(0)))
#define UNUSED(x) (void)(x)
#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))
#define ROUND_UP(x, n) (((x) + (1<<(n)) - 1) & ~((1<<(n)) - 1))
#define ROUND_DOWN(x, n) ((x) & ~((1<<(n)) - 1))
#define ADD_CACHELINE_VERSION_SIZE(x, cvs) ((x) + ((x)/(64-(cvs)) + ((x)%(64-(cvs))?1:0))*(cvs))


// app thread
#define MAX_APP_THREAD 65    // one additional thread for data statistics(main thread)  [CONFIG] 65
#define APP_MESSAGE_NR 96
#define POLL_CQ_MAX_CNT_ONCE 8

// dir thread
#define NR_DIRECTORY 1
#define DIR_MESSAGE_NR 128


void bindCore(uint16_t core);
char *getIP();
char *getMac();

inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}

#include <boost/coroutine2/all.hpp>
#include <boost/crc.hpp>

using CoroPush = boost::coroutines2::coroutine<int>::push_type;
using CoroPull = boost::coroutines2::coroutine<int>::pull_type;

// using CheckFunc = std::function<bool ()>;
// using CoroQueue = std::queue<std::pair<uint16_t, CheckFunc> >;
using CoroQueue = std::queue<uint16_t>;

namespace define {
// KV size
constexpr uint32_t keyLen = 8;
constexpr uint32_t simulatedValLen = 8;
}

using Key = std::array<uint8_t, define::keyLen>;
using Value = uint64_t;

namespace define {   // namespace define

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// Remote Allocation
constexpr uint64_t dsmSize           = 64;        // GB  [CONFIG] 64
constexpr uint64_t kChunkSize        = 16 * MB;   // B

// Local Allocation
constexpr uint64_t rdmaBufferSize     = 1;         // GB  [CONFIG] 4

// Cache (MB)
constexpr int kIndexCacheSize  = 600;  // MB including kHotIdxCacheSize
constexpr int kHotIdxCacheSize = 50;

// KV
constexpr uint64_t kKeyMin = 1;
#ifdef KEY_SPACE_LIMIT
constexpr uint64_t kKeyMax = 60000000;  // only for int workloads [CONFIG] 60000000
#endif
constexpr Key   kkeyNull   = Key{};
constexpr Value kValueNull = std::numeric_limits<Value>::min();
constexpr Value kValueMin = 1;
constexpr Value kValueMax = std::numeric_limits<Value>::max();

// Tree
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0);

// Packed GlobalAddress
constexpr uint32_t mnIdBit         = 8;
constexpr uint32_t offsetBit       = 48 - PACKED_ADDR_ALIGN_BIT;
constexpr uint32_t packedGaddrBit  = mnIdBit + offsetBit;
constexpr uint32_t packedGAddrSize = ROUND_UP(mnIdBit + offsetBit, 3) / 8;

// Version
constexpr uint32_t entryVersionBit = 4;
constexpr uint32_t nodeVersionBit  = 4;
constexpr uint32_t versionSize     = ROUND_UP(entryVersionBit + nodeVersionBit, 3) / 8;
constexpr uint32_t cachelineSize   = 64;
constexpr uint32_t blockSize       = cachelineSize - versionSize;

// Leaf Node
constexpr uint32_t leafSpanSize     = 128;
constexpr uint32_t scatterMetadataSize = versionSize + sizeof(uint8_t) + sizeof(uint64_t) + keyLen * 2;
constexpr uint32_t leafMetadataSize = versionSize + sizeof(uint8_t) * 2 + sizeof(uint64_t) + keyLen * 2;
constexpr uint32_t leafEntrySize = versionSize + keyLen + simulatedValLen;

// Internal Node
constexpr uint32_t internalSpanSize = 64;
constexpr uint32_t internalMetadataSize = versionSize + sizeof(uint8_t) * 2 + sizeof(uint64_t) * 3 + keyLen * 2;
constexpr uint32_t internalEntrySize = versionSize + keyLen + sizeof(uint64_t);

// Hopscotch Hashing
constexpr uint32_t hopRange = 8;
constexpr uint32_t entryGroupNum = leafSpanSize / hopRange + (leafSpanSize % hopRange);
constexpr uint32_t groupSize     = leafEntrySize * hopRange;

// Rdma Read/Write Size
constexpr uint32_t transLeafSize = ADD_CACHELINE_VERSION_SIZE(leafMetadataSize + leafEntrySize * leafSpanSize, versionSize);
constexpr uint32_t transInternalSize = ADD_CACHELINE_VERSION_SIZE(internalMetadataSize + internalEntrySize * internalSpanSize, versionSize);

// Allocation Size
constexpr uint32_t lock_pos_num = 1;
constexpr uint32_t allocationLeafSize = transLeafSize + lock_pos_num * 8UL;  // remain space for the lock
constexpr uint32_t allocationInternalSize = transInternalSize + lock_pos_num * 8UL;

// Rdma Buffer
constexpr int64_t  kPerThreadRdmaBuf  = rdmaBufferSize * GB / MAX_APP_THREAD;
constexpr int64_t  kPerCoroRdmaBuf    = kPerThreadRdmaBuf / MAX_CORO_NUM;
constexpr uint32_t bufferEntrySize    = ADD_CACHELINE_VERSION_SIZE(scatterMetadataSize + std::max(leafEntrySize, internalEntrySize), versionSize);
constexpr uint32_t bufferMetadataSize = ADD_CACHELINE_VERSION_SIZE(std::max(leafMetadataSize, internalMetadataSize), versionSize);

// On-chip Memory
constexpr uint64_t kLockStartAddr   = 0;
constexpr uint64_t kLockChipMemSize = ON_CHIP_SIZE * 1024;
constexpr uint64_t kLocalLockNum    = 4 * MB;  // tune to an appropriate value (as small as possible without affect the performance)
constexpr uint64_t kOnChipLockNum   = kLockChipMemSize * 8;  // 1bit-lock

// Greedy
constexpr uint64_t maxSizePerIO = 284;  // 284;
constexpr uint32_t maxLeafEntryPerIO = maxSizePerIO / leafEntrySize;
constexpr uint32_t maxInternalEntryPerIO = maxSizePerIO / internalEntrySize;
}


static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

#endif /* __COMMON_H__ */
