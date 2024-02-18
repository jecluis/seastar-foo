/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <fmt/format.h>

#include <atomic>
#include <cstdint>
#include <sstream>

namespace foo {

namespace stats {

class cache_stats {
  std::atomic<uint64_t> _hits;
  std::atomic<uint64_t> _misses;

 public:
  cache_stats() : _hits(0), _misses(0) {}
  cache_stats(cache_stats&) = delete;
  cache_stats(const cache_stats&) = delete;

  void hit() { _hits.fetch_add(1); }
  void miss() { _misses.fetch_add(1); }

  uint64_t hits() { return _hits.load(); }
  uint64_t misses() { return _misses.load(); }

  void print(std::ostringstream& oss) {
    oss << fmt::format("hits: {}, misses: {}", hits(), misses());
  }

  cache_stats& operator+=(cache_stats& rhs) {
    _hits.fetch_add(rhs.hits());
    _misses.fetch_add(rhs.misses());
    return *this;
  }
};

class shard_stats {
  cache_stats _cache_stats;

  std::atomic<uint64_t> _get;
  std::atomic<uint64_t> _put;
  std::atomic<uint64_t> _remove;
  std::atomic<uint64_t> _list;

 public:
  shard_stats() : _get(0), _put(0), _remove(0), _list(0) {}
  shard_stats(shard_stats&) = delete;
  shard_stats(const shard_stats&) = delete;

  cache_stats& cache() { return _cache_stats; }
  void put() { _put.fetch_add(1); }
  void get() { _get.fetch_add(1); }
  void remove() { _remove.fetch_add(1); }
  void list() { _list.fetch_add(1); }

  uint64_t gets() { return _get.load(); }
  uint64_t puts() { return _put.load(); }
  uint64_t removes() { return _remove.load(); }
  uint64_t lists() { return _list.load(); }

  void print(std::ostringstream& oss) {
    oss << "cache(";
    _cache_stats.print(oss);
    oss << "), "
        << fmt::format(
               "get: {}, put: {}, remove: {}, list: {}", gets(), puts(),
               removes(), lists()
           );
  }

  shard_stats& operator+=(shard_stats& rhs) {
    _cache_stats += rhs._cache_stats;
    _get.fetch_add(rhs.gets());
    _put.fetch_add(rhs.puts());
    _remove.fetch_add(rhs.removes());
    _list.fetch_add(rhs.lists());
    return *this;
  }
};

}  // namespace stats

}  // namespace foo
