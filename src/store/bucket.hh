/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <seastar/core/future.hh>

#include "store/manifest.hh"
#include "store_item.hh"

namespace foo {

namespace store {

// Represents a bucket.
class store_bucket {
  seastar::sstring _path;
  bucket_manifest _manifest;

 public:
  store_bucket(const seastar::sstring& path) : _path(path), _manifest(_path) {}

  store_bucket(store_bucket&) = delete;
  store_bucket(const store_bucket&) = delete;

  ~store_bucket() = default;

  seastar::future<> init();

  seastar::future<> put(foo::store::insert_entry_ptr entry);
  seastar::future<foo::store::value_ptr> get(foo::store::store_key_ptr key);
  seastar::future<> remove(const seastar::sstring& key);
  std::set<std::string> list();
};

}  // namespace store

}  // namespace foo
