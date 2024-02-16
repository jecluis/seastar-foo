/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <cstddef>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

namespace foo {
namespace store {

// The 'store_value' class takes ownership of whatever 'data' pointer it is
// provided, and will free it up on destruction (via 'delete').
class store_value {
  char* _data;
  size_t _size;

 public:
  // copies contents of the provided 'data' pointer to a new memory location, to
  // be owned by this store_value
  store_value(const char* data, size_t size) : _size(size) {
    _data = new char[size];
    memcpy(_data, data, size);
  }

  // don't allow copying, we should always create a new value.
  store_value(store_value&) = delete;
  store_value(const store_value&) = delete;

  ~store_value() { delete[] _data; }

  const char* data() { return _data; }
  size_t size() { return _size; }
};

using value_ptr = seastar::lw_shared_ptr<store_value>;
using foreign_value_ptr = seastar::foreign_ptr<value_ptr>;

inline value_ptr make_value_ptr(const char* data, size_t size) {
  return seastar::make_lw_shared<store_value>(data, size);
}

inline value_ptr make_value_ptr_by_copy(const char* data, size_t size) {
  return make_value_ptr(data, size);
}

inline foreign_value_ptr make_foreign_value_ptr(value_ptr value) {
  return seastar::make_foreign<value_ptr>(value);
}

class store_insert_entry {
  const std::string _key;
  value_ptr _value;

 public:
  store_insert_entry(
      const seastar::sstring& key, const seastar::sstring&& value
  )
      : _key(key),
        _value(make_value_ptr_by_copy(value.c_str(), value.size())) {}

  const seastar::sstring key() { return _key; }
  value_ptr value() { return _value; }
};

using insert_entry_ptr = seastar::lw_shared_ptr<store_insert_entry>;

inline insert_entry_ptr make_insert_entry_ptr(
    const seastar::sstring& key, const seastar::sstring&& value
) {
  return seastar::make_lw_shared<store_insert_entry>(key, std::move(value));
}

}  // namespace store
}  // namespace foo
