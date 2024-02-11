/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

// This implementation is heavily influenced by seastar's "memcache"
// implementation. Without it, a lot of the mechanics here used would not have
// been possible to be investigated (namely boost::intrusive), and applied.
//
// We do not aim at implementing an optimal cache at this point.

#pragma once

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include "cache.hh"

namespace foo {

namespace cache {

class sharded_cache {
  seastar::distributed<cache>& _cache_peers;

 public:
  sharded_cache(seastar::distributed<cache>& peers) : _cache_peers(peers) {}

  sharded_cache(const sharded_cache&) = delete;
  sharded_cache(sharded_cache&) = delete;

  seastar::future<bool> put(
      const seastar::sstring&& key, const seastar::sstring&& value
  );

  seastar::future<cache_item_ptr> get(const seastar::sstring& key);
  seastar::future<bool> remove(const seastar::sstring& key);

 private:
  inline unsigned get_shard_id(const seastar::sstring& key) {
    return std::hash<seastar::sstring>()(key) % seastar::smp::count;
  }
};

}  // namespace cache

}  // namespace foo
