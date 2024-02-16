/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store/bucket.hh"

#include <optional>
#include <seastar/core/file-types.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>
#include <stdexcept>

#include "store/item.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace store {

seastar::future<> store_bucket::init() {
  applog.debug("init store bucket at '{}'", _path);

  auto ftype = co_await seastar::engine().file_type(_path);
  if (!ftype.has_value()) {
    co_await seastar::make_directory(_path);
  } else if (*ftype != seastar::directory_entry_type::directory) {
    throw std::runtime_error(
        fmt::format("path {} exists but is not a directory", _path)
    );
  }

  co_await _manifest.init();
}

seastar::future<> store_bucket::put(foo::store::insert_entry_ptr entry) {
  const seastar::sstring& key = entry->key();
  auto value = entry->value();
  applog.debug(
      "write value for key '{}' on shard {}", key, seastar::this_shard_id()
  );
  auto k = std::string(key.begin(), key.end());
  auto fname = co_await _manifest.put(k);
  auto fpath = fmt::format("{}/{}", _path, fname);
  auto flags = seastar::open_flags::create | seastar::open_flags::truncate |
               seastar::open_flags::rw;
  applog.debug("write to file {}", fpath);
  applog.debug("write key {}", key);
  applog.debug("write value size {}", value->size());
  applog.debug(
      "write contents to file {} key {} size {}", fpath, key, value->size()
  );
  auto f = co_await seastar::open_file_dma(fpath, flags);
  co_await f.dma_write(0, value->data(), value->size());
  co_await f.flush();
  co_await f.close();
}

// Read data from disk
seastar::future<foo::store::value_ptr> store_bucket::get(
    foo::store::store_key_ptr key
) {
  auto fname = _manifest.get(key->key());
  if (!fname) {
    co_return nullptr;
  }
  auto fpath = fmt::format("{}/{}", _path, *fname);
  auto flags = seastar::open_flags::ro;
  applog.debug("read value for key '{}' at '{}'", key->key(), fpath);

  auto f = co_await seastar::open_file_dma(fpath, flags);
  auto fsize = co_await f.size();
  auto buf = co_await f.dma_read_exactly<char>(0, fsize);
  co_return foo::store::make_value_ptr_by_copy(buf.get(), buf.size());
}

seastar::future<> store_bucket::remove(const seastar::sstring& key) {
  return _manifest.remove(key).then([this](auto fname) {
    if (fname) {
      auto fpath = fmt::format("{}/{}", _path, *fname);
      return seastar::remove_file(fpath);
    }
    return seastar::make_ready_future<>();
  });
}

std::set<std::string> store_bucket::list() {
  return _manifest.list();
}

}  // namespace store

}  // namespace foo
