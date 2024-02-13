/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#include "sharded_cache.hh"

#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include "cache.hh"
#include "store_value.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace cache {

seastar::future<bool> sharded_cache::put(
    const seastar::sstring&& key, const seastar::sstring&& value
) {
  auto shard = get_shard_id(key);
  applog.debug("put key '{}' to shard {}", key, shard);
  if (seastar::this_shard_id() == shard) {
    return seastar::make_ready_future<bool>(
        _cache_peers.local().put(std::move(key), std::move(value))
    );
  }
  return _cache_peers.invoke_on(
      shard, &cache::put, std::forward<const seastar::sstring&&>(key),
      std::forward<const seastar::sstring&&>(value), false
  );
}

seastar::future<foo::store::value_ptr> sharded_cache::get(
    const seastar::sstring& key
) {
  auto shard = get_shard_id(key);
  applog.debug("get key '{}' from shard {}", key, shard);
  if (seastar::this_shard_id() == shard) {
    return seastar::make_ready_future<foo::store::value_ptr>(
        _cache_peers.local().get(key)
    );
  }
  return _cache_peers.invoke_on(shard, &cache::get, std::ref(key));
}

seastar::future<bool> sharded_cache::remove(const seastar::sstring& key) {
  auto shard = get_shard_id(key);
  applog.debug("remove key '{}' from shard {}", key, shard);
  if (seastar::this_shard_id() == shard) {
    return seastar::make_ready_future<bool>(_cache_peers.local().remove(key));
  }
  return _cache_peers.invoke_on(shard, &cache::remove, std::ref(key));
}

}  // namespace cache
}  // namespace foo
