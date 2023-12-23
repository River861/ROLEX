#if !defined(_VERSION_MANAGER_H_)
#define _VERSION_MANAGER_H_

#include "Common.h"
#include "Metadata.h"
#include "LeafNode.h"

#include <vector>


template <class NODE, class ENTRY>
class VersionManager {
public:
  VersionManager() {}

  static bool decode_node_versions(char *input_buffer, char *output_buffer);
  static void encode_node_versions(char *input_buffer, char *output_buffer);

  static std::tuple<uint64_t, uint64_t, uint64_t> get_offset_info(int start_entry_idx, int entry_num = 1);

private:
  static const int FIRST_OFFSET = std::min((uint64_t)define::cachelineSize, sizeof(NODE));  // the address of the first encoded cacheline version
};


/* Call after read the entire node. Both entry- and node-level consistency are checked. */
template <class NODE, class ENTRY>
inline bool VersionManager<NODE, ENTRY>::decode_node_versions(char *input_buffer, char *output_buffer) {
  auto node = (NODE *)output_buffer;
  auto& entries = node->records;  // auto& will parse entries as an array
  const auto& metadata = node->metadata;

  memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  for (uint64_t i = FIRST_OFFSET, j = FIRST_OFFSET; j < sizeof(NODE); i += define::blockSize, j += define::blockSize) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::blockSize, sizeof(NODE) - j));
    PackedVersion obj_version;
    if (j < STRUCT_OFFSET(NODE, records)) {
      obj_version = metadata.h_version;
    }
    else {
      obj_version = entries[(j - STRUCT_OFFSET(NODE, records)) / sizeof(ENTRY)].h_version;
    }
    // node- and entry-level consistency check
    if (obj_version != cacheline_version) {
      return false;
    }
  }
  // node-level joint consistency check
  const auto& node_version = entries[0].h_version.node_version;
  bool is_consistent = (node_version == metadata.h_version.node_version);
  for (const auto& entry : entries) is_consistent &= (node_version == entry.h_version.node_version);
  return is_consistent;
}


/* Call before write the entire node. Only the node_versions will be incremented. */
template <class NODE, class ENTRY>
inline void VersionManager<NODE, ENTRY>::encode_node_versions(char *input_buffer, char *output_buffer) {
  // increment node versions
  auto node = (NODE *)input_buffer;
  auto& entries = node->records;  // auto& will parse entries as an array
  auto& metadata = node->metadata;
  uint8_t node_version = entries[0].h_version.node_version;
  ++ node_version;
  metadata.h_version.node_version = node_version;
  for (auto& entry : entries) entry.h_version.node_version = node_version;

  // generate cacheline versions
  memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  for (uint64_t i = FIRST_OFFSET, j = FIRST_OFFSET; i < sizeof(NODE); i += define::blockSize, j += define::blockSize) {
    PackedVersion obj_version;
    if (i < STRUCT_OFFSET(NODE, records)) {
      obj_version = metadata.h_version;
    }
    else {
      obj_version = entries[(i - STRUCT_OFFSET(NODE, records)) / sizeof(ENTRY)].h_version;
    }
    memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::blockSize, sizeof(NODE) - i));
  }
}


template <class NODE, class ENTRY>
inline std::tuple<uint64_t, uint64_t, uint64_t> VersionManager<NODE, ENTRY>::get_offset_info(int start_entry_idx, int entry_num) {  // [raw_offset(encoded), raw length(encoded), first_offset(decoded)]
  uint64_t raw_offset, raw_length, first_offset;
  int end_entry_idx = start_entry_idx + entry_num;
  int decoded_start_offset = STRUCT_OFFSET(NODE, records[start_entry_idx]);
  int decoded_end_offset = STRUCT_OFFSET(NODE, records[end_entry_idx]);

  // calculate raw_offset, first_offset
  auto dist = decoded_start_offset - FIRST_OFFSET;
  if (dist < 0) {
    raw_offset = decoded_start_offset;
    first_offset = std::min((uint64_t)FIRST_OFFSET - decoded_start_offset, sizeof(ENTRY) * entry_num);
  }
  else {
    auto version_cnt = dist / define::blockSize + (dist % define::blockSize ? 1 : 0);
    raw_offset = decoded_start_offset + version_cnt * define::versionSize;
    first_offset = std::min((uint64_t)FIRST_OFFSET + version_cnt * define::blockSize - decoded_start_offset, sizeof(ENTRY) * entry_num);
  }
  // calculate raw_length
  auto get_raw_distance = [=](int decoded_len){
    if (decoded_len <= 0) {
      return decoded_len;
    }
    auto version_cnt = decoded_len / define::blockSize + (decoded_len % define::blockSize ? 1 : 0);
    return decoded_len + (int)(version_cnt * define::versionSize);
  };

  auto raw_end_distance = get_raw_distance(decoded_end_offset - FIRST_OFFSET);
  auto raw_start_distance = get_raw_distance(decoded_start_offset - FIRST_OFFSET);
  raw_length = raw_end_distance - raw_start_distance;

  return std::make_tuple(raw_offset, raw_length, first_offset);
}


#endif // _VERSION_MANAGER_H_