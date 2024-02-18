/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <set>
#include <utility>

#include "cache.hh"
#include "cmap.hh"
#include "stats.hh"
#include "store/bucket.hh"
#include "store/item.hh"

namespace foo {

namespace store {

class store_shard {
  using store_bucket_ptr = std::unique_ptr<store_bucket>;

  const seastar::sstring _store_path;
  const foo::consistent_map _cmap;
  foo::cache::cache _cache;
  // associative map of store buckets for this shard
  std::map<uint32_t, store_bucket_ptr> _buckets;
  // statistics for this shard
  foo::stats::shard_stats _stats;

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
  seastar::future<> remove(const seastar::sstring& key);
  seastar::future<std::set<std::string>> list();

  foo::stats::shard_stats& stats() { return _stats; };

  seastar::future<> stop();
};
}  // namespace store

}  // namespace foo
