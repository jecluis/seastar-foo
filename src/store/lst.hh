/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace foo {

namespace store {

class lst_holder {
  std::vector<std::set<std::string>> _entries;
  std::atomic<uint32_t> _n;

 public:
  lst_holder(uint32_t count) : _entries(count), _n(0) {}
  lst_holder(lst_holder&) = delete;
  lst_holder(const lst_holder&) = delete;
  ~lst_holder() = default;

  uint32_t get_id() {
    auto n = _n.fetch_add(1);
    assert(n < _entries.size());
    return n;
  }
  void insert(uint32_t n, const std::set<std::string>&& other);
  void agg(std::set<std::string>& res);
};

}  // namespace store

}  // namespace foo
