/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <cassert>
#include <cstddef>
#include <seastar/core/sstring.hh>
#include <unordered_map>
#include <vector>

namespace foo {

class consistent_map {
  size_t _num_buckets;
  size_t _num_shards;
  std::vector<unsigned int> _map;
  std::unordered_map<unsigned int, std::vector<uint32_t>> _per_shard_buckets;

 public:
  consistent_map(size_t num_buckets, size_t num_shards)
      : _num_buckets(num_buckets),
        _num_shards(num_shards),
        _map(_num_buckets),
        _per_shard_buckets(_num_shards) {
    assert(_num_buckets >= _num_shards);
    auto buckets_per_shard = _num_buckets / _num_shards;
    for (auto i = 0; i < _num_buckets; ++i) {
      unsigned int shard_id = i % _num_shards;
      _map[i] = shard_id;
      if (!_per_shard_buckets.contains(shard_id)) {
        _per_shard_buckets[shard_id] = std::vector<uint32_t>(buckets_per_shard);
      }
      _per_shard_buckets[shard_id].push_back(i);
    }
  }

  // no point in copying the container
  consistent_map(consistent_map&) = delete;
  consistent_map(const consistent_map&) = delete;

  ~consistent_map() = default;

  unsigned int get_shard(const seastar::sstring& key) const {
    auto hash = std::hash<seastar::sstring>()(key);
    return get_shard(hash);
  }

  unsigned int get_shard(size_t hash) const {
    auto bucket = hash % _num_buckets;
    return _map[bucket];
  }

  std::vector<uint32_t> get_shard_buckets(unsigned int shard_id) const {
    assert(_per_shard_buckets.contains(shard_id));
    return _per_shard_buckets.find(shard_id)->second;
  }
};

}  // namespace foo
