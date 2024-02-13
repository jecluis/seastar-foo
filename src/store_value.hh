/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <cstddef>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

namespace foo {
namespace store {

// The 'store_value' class takes ownership of whatever 'data' pointer it is
// provided, and will free it up on destruction (via 'delete').
class store_value {
  const char* _data;
  size_t _size;

 public:
  store_value(const char* data, size_t size) : _data(data), _size(size) {}

  // don't allow copying, we should always create a new value.
  store_value(store_value&) = delete;
  store_value(const store_value&) = delete;

  ~store_value() { delete _data; }

  const char* data() { return _data; }
  size_t size() { return _size; }
};

using value_ptr = seastar::lw_shared_ptr<store_value>;

inline value_ptr make_value_ptr(const char* data, size_t size) {
  return seastar::make_lw_shared<store_value>(data, size);
}

// copies contents of the provided 'data' pointer to a new memory location, to
// be owned by the newly created store_value.
inline value_ptr make_value_ptr_by_copy(const char* data, size_t size) {
  char* buf = new char[size];
  memcpy(buf, data, size);
  return make_value_ptr(buf, size);
}

}  // namespace store
}  // namespace foo
