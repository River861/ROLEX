#if !defined(_LEAF_VERSION_MANAGER_H_)
#define _LEAF_VERSION_MANAGER_H_

#include "Common.h"
#include "Metadata.h"
#include "LeafNode.h"

#include <vector>


class LeafVersionManager {
public:
  LeafVersionManager() {}

  static bool decode_node_versions(char *input_buffer, char *output_buffer);
  static void encode_node_versions(char *input_buffer, char *output_buffer);

  static std::tuple<uint64_t, uint64_t, uint64_t> get_offset_info(int start_entry_idx, int entry_num = 1);

private:
  static const int FIRST_OFFSET = std::min((uint64_t)define::cachelineSize, sizeof(ScatteredLeafNode));  // the address of the first encoded cacheline version
};


/* Call after read the entire node. Both entry- and node-level consistency are checked. */
inline bool LeafVersionManager::decode_node_versions(char *input_buffer, char *output_buffer) {
  auto leaf = (ScatteredLeafNode *)output_buffer;

  memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  for (uint64_t i = FIRST_OFFSET, j = FIRST_OFFSET; j < sizeof(ScatteredLeafNode); i += define::blockSize, j += define::blockSize) {
    auto cacheline_version = *(PackedVersion *)(input_buffer + i);
    i += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::blockSize, sizeof(ScatteredLeafNode) - j));
    PackedVersion obj_version;
    auto group_idx = (j - STRUCT_OFFSET(ScatteredLeafNode, record_groups)) / sizeof(LeafEntryGroup);
    if (j < (uint64_t)STRUCT_OFFSET(ScatteredLeafNode, record_groups[group_idx].records)) {
      obj_version = leaf->record_groups[group_idx].metadata.h_version;
    }
    else {
      obj_version = leaf->record_groups[group_idx].records[(j - STRUCT_OFFSET(ScatteredLeafNode, record_groups[group_idx].records)) / sizeof(LeafEntry)].h_version;
    }
    // node- and entry-level consistency check
    if (obj_version != cacheline_version) {
      return false;
    }
  }
  // node-level joint consistency check
  const auto& node_version = leaf->record_groups[0].metadata.h_version.node_version;
  for (const auto& group : leaf->record_groups) {
    if (node_version != group.metadata.h_version.node_version) {
      return false;
    }
    for (const auto& entry : group.records) if (node_version != entry.h_version.node_version) {
      return false;
    }
  }
  return true;
}


/* Call before write the entire node. Only the node_versions will be incremented. */
inline void LeafVersionManager::encode_node_versions(char *input_buffer, char *output_buffer) {
  // increment node versions
  auto leaf = (ScatteredLeafNode *)input_buffer;
  uint8_t node_version = leaf->record_groups[0].metadata.h_version.node_version;
  ++ node_version;
  for (auto& group : leaf->record_groups) {
    group.metadata.h_version.node_version = node_version;
    for (auto& entry : group.records) {
      entry.h_version.node_version = node_version;
    }
  }

  // generate cacheline versions
  memcpy(output_buffer, input_buffer, FIRST_OFFSET);
  for (uint64_t i = FIRST_OFFSET, j = FIRST_OFFSET; i < sizeof(ScatteredLeafNode); i += define::blockSize, j += define::blockSize) {
    PackedVersion obj_version;
    auto group_idx = (i - STRUCT_OFFSET(ScatteredLeafNode, record_groups)) / sizeof(LeafEntryGroup);
    if (i < (uint64_t)STRUCT_OFFSET(ScatteredLeafNode, record_groups[group_idx].records)) {
      obj_version = leaf->record_groups[group_idx].metadata.h_version;
    }
    else {
      obj_version = leaf->record_groups[group_idx].records[(i - STRUCT_OFFSET(ScatteredLeafNode, record_groups[group_idx].records)) / sizeof(LeafEntry)].h_version;
    }
    memcpy(output_buffer + j, &obj_version, sizeof(PackedVersion));
    j += sizeof(PackedVersion);
    memcpy(output_buffer + j, input_buffer + i, std::min((size_t)define::blockSize, sizeof(ScatteredLeafNode) - i));
  }
}


inline std::tuple<uint64_t, uint64_t, uint64_t> LeafVersionManager::get_offset_info(int start_entry_idx, int entry_num) {  // [raw_offset(encoded), raw length(encoded), first_offset(decoded)]
  uint64_t raw_offset, raw_length, first_offset;
  int end_entry_idx = start_entry_idx + entry_num;
  int decoded_start_offset = (start_entry_idx % define::hopRange ?
                              STRUCT_OFFSET(ScatteredLeafNode, record_groups[start_entry_idx / define::hopRange].records[start_entry_idx % define::hopRange]) :
                              STRUCT_OFFSET(ScatteredLeafNode, record_groups[start_entry_idx / define::hopRange].metadata));
  int decoded_end_offset = (end_entry_idx % define::hopRange ?
                            STRUCT_OFFSET(ScatteredLeafNode, record_groups[end_entry_idx / define::hopRange].records[end_entry_idx % define::hopRange]) :
                            STRUCT_OFFSET(ScatteredLeafNode, record_groups[end_entry_idx / define::hopRange].metadata));

  // calculate raw_offset, first_offset
  auto dist = decoded_start_offset - FIRST_OFFSET;
  if (dist < 0) {
    raw_offset = decoded_start_offset;
    first_offset = std::min((int)FIRST_OFFSET - decoded_start_offset, decoded_end_offset - decoded_start_offset);
  }
  else {
    auto version_cnt = dist / define::blockSize + (dist % define::blockSize ? 1 : 0);
    raw_offset = decoded_start_offset + version_cnt * define::versionSize;
    first_offset = std::min((int)(FIRST_OFFSET + version_cnt * define::blockSize) - decoded_start_offset, decoded_end_offset - decoded_start_offset);
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

#endif // _LEAF_VERSION_MANAGER_H_