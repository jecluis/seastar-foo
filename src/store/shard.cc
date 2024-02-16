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
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>
#include <stdexcept>

#include "store/item.hh"
#include "store/lst.hh"

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

seastar::future<> store_shard::remove(const seastar::sstring& key) {
  auto bucket = _cmap.get_bucket(key);
  const auto target_shard = _cmap.get_shard(key);
  if (target_shard != seastar::this_shard_id()) {
    throw std::runtime_error("wrong shard");
  }

  if (!_buckets.contains(bucket)) {
    throw std::runtime_error("expected to own bucket");
  }

  // make 'key' live through 'co_await', even if the original 'key' goes away.
  const seastar::sstring skey(key);
  co_await _buckets[bucket]->remove(skey);
  _cache.remove(skey);
}

seastar::future<std::set<std::string>> store_shard::list() {
  auto lst = seastar::make_lw_shared<foo::store::lst_holder>(_buckets.size());

  co_await seastar::parallel_for_each(
      _buckets.begin(), _buckets.end(),
      [lst](auto& b) -> seastar::future<> {
        auto id = lst->get_id();
        lst->insert(id, b.second->list());
        co_return;
      }
  );

  std::set<std::string> s;
  lst->agg(s);
  co_return s;
}

seastar::future<> store_shard::stop() {
  applog.debug("stop store shard");
  return seastar::make_ready_future<>();
}

}  // namespace store

}  // namespace foo
