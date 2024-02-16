/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store.hh"

#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <cstdint>
#include <optional>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>
#include <stdexcept>
#include <utility>

#include "store/item.hh"
#include "store/lst.hh"
#include "store/shard.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace store {

// inits a store directory
seastar::future<> init_store(
    const seastar::sstring& path, uint32_t num_buckets
) {
  auto fname = fmt::format("{}/num_buckets", path);
  auto flags = seastar::open_flags::create | seastar::open_flags::rw;
  applog.debug("write num buckets {} to file", num_buckets);
  auto buckets_str = fmt::format("{}", num_buckets);

  auto f = co_await seastar::open_file_dma(fname, flags);
  co_await f.dma_write(0, buckets_str.c_str(), buckets_str.size());
  co_await f.flush();
  co_await f.close();
}

// Creates a store directory, populating the control file that keeps the number
// of buckets being used.
seastar::future<> create_store(
    const seastar::sstring& path, uint32_t num_buckets
) {
  applog.info("creating store at {}", path);

  // we need to persist 'path' for long enough to finish the various calls we'll
  // do, so we need a copy in case 'path' doesn't live long enough while we wait
  // for the various coroutines to finish.
  const seastar::sstring spath(path);
  co_await seastar::make_directory(spath);
  co_await init_store(spath, num_buckets);
}

// Opens a store directory, returning the number of buckets being used.
seastar::future<std::optional<uint32_t>> open_store(const seastar::sstring& path
) {
  auto fname = fmt::format("{}/num_buckets", path);
  auto flags = seastar::open_flags::ro;
  applog.debug("read num buckets file at {}", fname);

  // check for 'num_buckets' existence.
  auto ftype = co_await seastar::engine().file_type(fname);
  if (!ftype.has_value()) {
    // 'num_buckets' file does not exist; store not inited.
    co_return std::nullopt;
  } else if (*ftype != seastar::directory_entry_type::regular) {
    throw std::runtime_error(
        fmt::format("wrongly initialized store, 'num_buckets' is not a file")
    );
  }

  auto f = co_await seastar::open_file_dma(fname, flags);
  auto fsize = co_await f.size();
  assert(fsize > 0);

  auto buf = co_await f.dma_read_exactly<char>(0, fsize);
  try {
    co_return boost::lexical_cast<uint32_t>(buf.get(), buf.size());
  } catch (const boost::bad_lexical_cast&) {
    throw std::runtime_error("unable to obtain store's number of buckets");
  }
}

// Either open an existing store, or create it if it doesn't exist.
// If the store is being created, a control file to store the number of buckets
// is written.
seastar::future<uint32_t> open_or_create(
    const seastar::sstring& path, uint32_t num_buckets
) {
  const seastar::sstring spath(path);

  auto ftype = co_await seastar::engine().file_type(spath);
  if (!ftype.has_value()) {
    co_await create_store(spath, num_buckets);
    co_return num_buckets;

  } else if (*ftype != seastar::directory_entry_type::directory) {
    throw std::runtime_error(
        fmt::format("path {} exists but is not a directory", spath)
    );
  }

  auto nbuckets = co_await open_store(spath);
  if (!nbuckets.has_value()) {
    // store was probably not initialized.
    co_await init_store(spath, num_buckets);
    co_return num_buckets;
  }
  co_return *nbuckets;
}

seastar::future<> sharded_store::put(
    const seastar::sstring&& key, const seastar::sstring&& value
) {
  auto shard = _cmap->get_shard(key);
  applog.debug("put key {} on shard {}", key, shard);

  if (seastar::this_shard_id() == shard) {
    auto item = foo::store::make_insert_entry_ptr(key, std::move(value));
    return _shards.local().put(item);
  }
  return _shards.invoke_on(
      shard,
      [key = std::move(key),
       value = std::move(value)](store_shard& s) -> seastar::future<> {
        // we need to allocate the shared_ptr here so it doesn't cross shard
        // boundaries.
        auto item = foo::store::make_insert_entry_ptr(key, std::move(value));
        co_await s.put(item);
      }
  );
}

seastar::future<foo::store::foreign_value_ptr> sharded_store::get(
    const seastar::sstring& key
) {
  auto shard = _cmap->get_shard(key);
  applog.debug("get key {} on shard {}", key, shard);

  if (seastar::this_shard_id() == shard) {
    auto skey = foo::store::make_store_key_ptr(key);
    return _shards.local().get(skey);
  }

  return _shards.invoke_on(shard, [key](store_shard& s) {
    auto skey = foo::store::make_store_key_ptr(key);
    return s.get(skey);
  });
}

seastar::future<> sharded_store::remove(const seastar::sstring& key) {
  auto shard = _cmap->get_shard(key);
  applog.debug("remove key {} on shard {}", key, shard);
  return _shards.invoke_on(shard, &store_shard::remove, key);
}

seastar::future<> sharded_store::list(seastar::lw_shared_ptr<lst_holder> out_lst
) {
  applog.debug("list from all shards");
  return _shards.invoke_on_all(
      [out_lst](store_shard& shard) mutable -> seastar::future<> {
        applog.debug("list on shard {}", seastar::this_shard_id());
        const auto keys = co_await shard.list();
        out_lst->insert(std::move(keys));
      }
  );
}

}  // namespace store

}  // namespace foo
