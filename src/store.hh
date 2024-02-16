/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <set>

#include "cache.hh"
#include "cmap.hh"
#include "store_value.hh"

namespace foo {

namespace store {

// Maintains the mapping between this bucket's keys and filenames where data is
// kept.
class bucket_manifest {
  const int key_size = 255 + 1;  // key + terminator character
  const int fname_size = 20;

  std::string _manifest_path;
  std::map<std::string, std::string> _key_to_fname_map;
  std::set<std::string> _fname_set;

 public:
  bucket_manifest(const std::string& bucket_path)
      : _manifest_path(fmt::format("{}/manifest", bucket_path)) {}

  bucket_manifest(bucket_manifest&) = delete;
  bucket_manifest(const bucket_manifest&) = delete;

  ~bucket_manifest() = default;

  seastar::future<> init() {
    return load_manifest().handle_exception([](auto) {});
  }

  seastar::future<std::string> put(const std::string& key);
  std::optional<std::string> get(const std::string& key);
  bool exists(const std::string& key);
  seastar::future<std::optional<std::string>> remove(const std::string& key);
  std::set<std::string> list();

 private:
  seastar::future<> load_manifest();
  void load_entries(seastar::temporary_buffer<char>& tbuf);

  seastar::future<> write_manifest();
};

// Represents a bucket.
class store_bucket {
  seastar::sstring _path;
  bucket_manifest _manifest;

 public:
  store_bucket(const seastar::sstring& path) : _path(path), _manifest(_path) {}

  store_bucket(store_bucket&) = delete;
  store_bucket(const store_bucket&) = delete;

  ~store_bucket() = default;

  seastar::future<> init();

  seastar::future<> put(foo::store::insert_entry_ptr entry);
  seastar::future<foo::store::value_ptr> get(const seastar::sstring& key);
  seastar::future<> remove(const seastar::sstring& key);
  std::set<std::string> list();
};

class store_shard {
  using store_bucket_ptr = std::unique_ptr<store_bucket>;

  const seastar::sstring _store_path;
  const foo::consistent_map _cmap;
  foo::cache::cache _cache;

  // associative map of store buckets for this shard
  std::map<uint32_t, store_bucket_ptr> _buckets;

 public:
  store_shard(
      const seastar::sstring& store_path, foo::consistent_map&& cmap,
      size_t cache_bucket_count, size_t max_cache_size, uint32_t cache_ttl
  )
      : _store_path(store_path),
        _cmap(std::move(cmap)),
        _cache(cache_bucket_count, max_cache_size, cache_ttl) {}

  store_shard(store_shard&) = delete;
  store_shard(const store_shard&) = delete;

  ~store_shard() = default;

  // init this store shard, populate its buckets
  seastar::future<> init();

  seastar::future<> put(foo::store::insert_entry_ptr entry);
  seastar::future<foo::store::foreign_value_ptr> get(const seastar::sstring& key
  );
  seastar::future<bool> remove(const seastar::sstring& key);
  seastar::future<std::set<std::string>> list();

  seastar::future<> stop();
};

class lst_holder {
  std::vector<std::set<std::string>> _shards;

 public:
  lst_holder() : _shards(seastar::smp::count) {}
  lst_holder(lst_holder&) = delete;
  lst_holder(const lst_holder&) = delete;
  ~lst_holder() = default;

  void insert(const std::set<std::string>&& other);
  void agg(std::set<std::string>& res);
};

class sharded_store {
  seastar::distributed<store_shard>& _shards;
  seastar::lw_shared_ptr<foo::consistent_map> _cmap;

 public:
  sharded_store(seastar::distributed<store_shard>& shards) : _shards(shards) {}

  sharded_store(sharded_store&) = default;
  sharded_store(const sharded_store&) = default;

  ~sharded_store() = default;

  void init(uint32_t store_bucket_count) {
    _cmap = seastar::make_lw_shared<foo::consistent_map>(
        foo::consistent_map(store_bucket_count, seastar::smp::count)
    );
  }

  seastar::future<> put(
      const seastar::sstring&& key, const seastar::sstring&& value
  );

  seastar::future<foo::store::foreign_value_ptr> get(const seastar::sstring& key
  );

  seastar::future<bool> remove(const seastar::sstring& key);

  seastar::future<> list(seastar::lw_shared_ptr<lst_holder> out_lst);
};

seastar::future<uint32_t> open_or_create(
    const seastar::sstring& path, uint32_t num_buckets
);

}  // namespace store

}  // namespace foo
