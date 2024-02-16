/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store/lst.hh"

#include <seastar/util/log.hh>
#include <set>
#include <string>

static seastar::logger applog(__FILE__);

namespace foo {

namespace store {

void lst_holder::insert(uint32_t n, const std::set<std::string>&& other) {
  applog.debug("do lst_holder insert, n: {}, size: {}", n, other.size());
  _entries[n] = std::move(other);
}

void lst_holder::agg(std::set<std::string>& res) {
  for (auto& entry : _entries) {
    res.insert(entry.cbegin(), entry.cend());
  }
}

}  // namespace store

}  // namespace foo
