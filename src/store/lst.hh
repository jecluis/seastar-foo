/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <seastar/core/smp.hh>
#include <set>
#include <string>
#include <vector>

namespace foo {

namespace store {

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

}  // namespace store

}  // namespace foo
