/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <set>
#include <stdexcept>

#include "cache.hh"
#include "cmap.hh"
#include "utils.hh"

namespace foo {

namespace store {

class bucket_manifest {
  const int key_size = 255;
  const int fname_size = 20;

  std::string _manifest_path;
  std::map<std::string, std::string> _key_to_fname_map;
  std::set<std::string> _fname_set;

 public:
  bucket_manifest(const seastar::sstring& bucket_path)
      : _manifest_path(fmt::format("{}/manifest", bucket_path)) {}

  seastar::future<> init() {
    return load_manifest().handle_exception([](auto) {});
  }

  seastar::future<std::string> put(const std::string& key);
  std::optional<std::string> get(const std::string& key);
  bool exists(const std::string& key);
  seastar::future<> remove(const std::string& key);
  std::vector<std::string> list();

 private:
  seastar::future<> load_manifest();
  void load_entries(seastar::temporary_buffer<char>& tbuf);

  seastar::future<> write_manifest();
};

class store_bucket {
  std::map<seastar::sstring, seastar::sstring> _key_file_map;
  std::set<seastar::sstring> _existing_files;
  seastar::sstring _path;

 public:
  store_bucket(const seastar::sstring& path) : _path(path) {}
};

class store_shard {
  const foo::consistent_map _cmap;
  foo::cache::cache _cache;

 public:
  store_shard(
      foo::consistent_map& cmap, size_t cache_bucket_count,
      size_t max_cache_size, uint32_t cache_ttl
  )
      : _cmap(cmap), _cache(cache_bucket_count, max_cache_size, cache_ttl) {}

  store_shard(store_shard&) = delete;
  store_shard(const store_shard&) = delete;

  ~store_shard() = default;

  seastar::future<> put(
      const seastar::sstring& key, const seastar::sstring& value
  ) {
    auto bucket = _cmap.get_bucket(key);
    const auto target_shard = _cmap.get_shard(key);
    if (target_shard != seastar::this_shard_id()) {
      return seastar::make_exception_future(std::runtime_error("wrong shard"));
    }

    return seastar::make_ready_future<>();
  }
};

class sharded_store {};

seastar::future<uint32_t> open_or_create(
    const seastar::sstring& path, uint32_t num_buckets
);

}  // namespace store

}  // namespace foo
