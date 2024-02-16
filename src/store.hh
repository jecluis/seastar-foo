/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include "cmap.hh"
#include "store/item.hh"
#include "store/lst.hh"
#include "store/shard.hh"

namespace foo {

namespace store {

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

  seastar::future<> remove(const seastar::sstring& key);

  seastar::future<seastar::lw_shared_ptr<lst_holder>> list();
};

seastar::future<uint32_t> open_or_create(
    const seastar::sstring& path, uint32_t num_buckets
);

}  // namespace store

}  // namespace foo
