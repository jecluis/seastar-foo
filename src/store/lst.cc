/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store/lst.hh"

#include <seastar/core/shard_id.hh>
#include <seastar/util/log.hh>
#include <set>
#include <string>

static seastar::logger applog(__FILE__);

namespace foo {

namespace store {

void lst_holder::insert(const std::set<std::string>&& other) {
  applog.debug("do lst_holder insert2");
  auto id = seastar::this_shard_id();
  _shards[id] = std::move(other);
}

void lst_holder::agg(std::set<std::string>& res) {
  for (auto& keys : _shards) {
    res.insert(keys.cbegin(), keys.cend());
  }
}

}  // namespace store

}  // namespace foo
