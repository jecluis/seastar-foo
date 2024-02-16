/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store/shard.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/log.hh>
#include <stdexcept>

#include "store_item.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace store {

seastar::future<> store_shard::init() {
  auto shard_id = seastar::this_shard_id();
  auto bucket_ids = _cmap.get_shard_buckets(shard_id);

  for (auto& bid : bucket_ids) {
    auto bucket_path = fmt::format("{}/{}", _store_path, bid);
    _buckets[bid] = store_bucket_ptr(new store_bucket(bucket_path));
  }

  return seastar::parallel_for_each(
      _buckets.begin(), _buckets.end(),
      [](auto& entry) {
        applog.debug("init store bucket {}", entry.first);
        return entry.second->init();
      }
  );
}

seastar::future<> store_shard::put(foo::store::insert_entry_ptr entry) {
  const seastar::sstring& key = entry->key();
  auto bucket = _cmap.get_bucket(key);
  const auto target_shard = _cmap.get_shard(key);
  if (target_shard != seastar::this_shard_id()) {
    throw std::runtime_error("wrong shard");
  }

  if (!_buckets.contains(bucket)) {
    throw std::runtime_error("expected to own bucket");
  }
  co_await _buckets[bucket]->put(entry);
  auto res = _cache.put(entry);
  if (!res) {
    applog.error("unable to store key/value in cache");
  }
}

seastar::future<foo::store::foreign_value_ptr> store_shard::get(
    foo::store::store_key_ptr key
) {
  auto bucket = _cmap.get_bucket(key->key());
  const auto target_shard = _cmap.get_shard(key->key());
  if (target_shard != seastar::this_shard_id()) {
    throw std::runtime_error("wrong shard");
  }

  if (!_buckets.contains(bucket)) {
    throw std::runtime_error("expected to own bucket");
  }

  auto cache_value = _cache.get(key->key());
  if (cache_value) {
    // in cache, return value
    applog.debug("obtained key '{}' from cache", key->key());
    co_return foo::store::make_foreign_value_ptr(cache_value);
  }

  // must obtain from disk
  auto data = co_await _buckets[bucket]->get(key);
  if (data) {
    _cache.put_ptr(key->key(), data);
  }
  co_return foo::store::make_foreign_value_ptr(data);
}

seastar::future<bool> store_shard::remove(const seastar::sstring& key) {
  auto bucket = _cmap.get_bucket(key);
  const auto target_shard = _cmap.get_shard(key);
  if (target_shard != seastar::this_shard_id()) {
    return seastar::make_exception_future<bool>(std::runtime_error("wrong shard"
    ));
  }

  if (!_buckets.contains(bucket)) {
    return seastar::make_exception_future<bool>(
        std::runtime_error("expected to own bucket")
    );
  }

  return _buckets[bucket]->remove(key).then([this, key] {
    _cache.remove(key);
    return seastar::make_ready_future<bool>(true);
  });
}

seastar::future<std::set<std::string>> store_shard::list() {
  std::set<std::string> s;
  for (auto& b : _buckets) {
    const auto& k = b.second->list();
    s.insert(k.cbegin(), k.cend());
  }
  return seastar::make_ready_future<std::set<std::string>>(s);
}

seastar::future<> store_shard::stop() {
  applog.debug("stop store shard");
  return seastar::make_ready_future<>();
}

}  // namespace store

}  // namespace foo
