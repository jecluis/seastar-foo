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
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <set>

#include "cache.hh"
#include "cmap.hh"
#include "store/bucket.hh"
#include "store_item.hh"

namespace foo {

namespace store {

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
  seastar::future<foo::store::foreign_value_ptr> get(
      foo::store::store_key_ptr key
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
