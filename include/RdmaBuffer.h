#if !defined(_RDMA_BUFFER_H_)
#define _RDMA_BUFFER_H_

#include "Common.h"

// abstract rdma registered buffer
class RdmaBuffer {

private:
  // async, buffer safty
  static const int kCasBufferCnt    = 64;
  static const int kPageBufferCnt   = 64;
  static const int kLeafBufferCnt   = 64;
  static const int kSegmentBufferCnt= 64;
  static const int kHeaderBufferCnt = 32;
  static const int kEntryBufferCnt  = 32;

  char *buffer;

  uint64_t *cas_buffer;
  char *leaf_buffer;
  char *segment_buffer;
  char *metadata_buffer;
  char *range_buffer;
  char *zero_byte;
  uint64_t *zero_8_byte;

  int cas_buffer_cur;
  int leaf_buffer_cur;
  int segment_buffer_cur;
  int metadata_buffer_cur;

public:
  RdmaBuffer() = default;

  void set_buffer(char *buffer) {
    // setup buffer partition
    this->buffer    = buffer;
    cas_buffer      = (uint64_t *)buffer;
    leaf_buffer     = (char     *)((char *)cas_buffer      + sizeof(uint64_t) * kCasBufferCnt);
    segment_buffer  = (char     *)((char *)leaf_buffer     + define::allocationLeafSize     * kLeafBufferCnt);
    metadata_buffer = (char     *)((char *)segment_buffer  + define::allocationLeafSize     * kSegmentBufferCnt);
    zero_byte       = (char     *)((char *)metadata_buffer + define::bufferMetadataSize     * kHeaderBufferCnt);
    zero_8_byte     = (uint64_t *)((char *)zero_byte       + sizeof(char));
    range_buffer    = (char     *)((char *)zero_8_byte     + sizeof(uint64_t));
    assert(range_buffer - buffer < define::kPerCoroRdmaBuf);
    // init counters
    cas_buffer_cur      = 0;
    leaf_buffer_cur     = 0;
    segment_buffer_cur  = 0;
    metadata_buffer_cur = 0;
    entry_buffer_cur    = 0;
  }

  uint64_t *get_cas_buffer() {
    cas_buffer_cur = (cas_buffer_cur + 1) % kCasBufferCnt;
    return cas_buffer + cas_buffer_cur;
  }

  char *get_leaf_buffer() {
    leaf_buffer_cur = (leaf_buffer_cur + 1) % kLeafBufferCnt;
    return leaf_buffer + leaf_buffer_cur * define::allocationLeafSize;
  }

  char *get_segment_buffer() {
    segment_buffer_cur = (segment_buffer_cur + 1) % kSegmentBufferCnt;
    return segment_buffer + segment_buffer_cur * define::allocationLeafSize;
  }

  char *get_metadata_buffer() {
    metadata_buffer_cur = (metadata_buffer_cur + 1) % kHeaderBufferCnt;
    return metadata_buffer + metadata_buffer_cur * define::bufferMetadataSize;
  }

  char *get_range_buffer() {
    return range_buffer;
  }

  char *get_zero_byte() {
    *zero_byte = '\0';
    return zero_byte;
  }

  uint64_t *get_zero_8_byte() {
    *zero_8_byte = 0ULL;  // should be zeroed explicitly due to underlying bug (TODO: debug)
    return zero_8_byte;
  }
};

#endif // _RDMA_BUFFER_H_
