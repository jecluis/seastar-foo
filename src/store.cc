/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store.hh"

#include <atomic>
#include <boost/lexical_cast.hpp>
#include <cstdint>
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

// Creates a store directory, populating the control file that keeps the number
// of buckets being used.
seastar::future<> create_store(
    const seastar::sstring& path, uint32_t num_buckets
) {
  applog.info("creating store at {}", path);

  return seastar::make_directory(path).then([path, num_buckets] {
    auto fname = fmt::format("{}/num_buckets", path);
    auto flags = seastar::open_flags::create | seastar::open_flags::rw;
    applog.debug("write num buckets {} to file", num_buckets);
    auto buckets_str = fmt::format("{}", num_buckets);

    return seastar::with_file(
        seastar::open_file_dma(fname, flags),
        [buckets_str = std::move(buckets_str)](seastar::file& f) {
          // run async, ensure we wait for dma_write() while we hold
          // 'buckets_str'.
          return seastar::async([buckets_str = std::move(buckets_str), &f] {
            (void)f.dma_write(0, buckets_str.c_str(), buckets_str.size()).get();
          });
        }
    );
  });
}

// Opens a store directory, returning the number of buckets being used.
seastar::future<uint32_t> open_store(const seastar::sstring& path) {
  auto fname = fmt::format("{}/num_buckets", path);
  auto flags = seastar::open_flags::ro;
  applog.debug("read num buckets file at {}", fname);

  return seastar::with_file(
      seastar::open_file_dma(fname, flags),
      [](seastar::file& f) {
        auto fsize = f.size().get();
        assert(fsize > 0);

        return f.dma_read_exactly<char>(0, fsize).then(
            [](seastar::temporary_buffer<char> tbuf) {
              try {
                return seastar::make_ready_future<uint32_t>(
                    boost::lexical_cast<uint32_t>(tbuf.get(), tbuf.size())
                );
              } catch (const boost::bad_lexical_cast&) {
                return seastar::make_exception_future<uint32_t>(
                    std::runtime_error(
                        "unable to obtain store's number of buckets"
                    )
                );
              }
            }
        );
      }
  );
}

// Either open an existing store, or create it if it doesn't exist.
// If the store is being created, a control file to store the number of buckets
// is written.
seastar::future<uint32_t> open_or_create(
    const seastar::sstring& path, uint32_t num_buckets
) {
  return seastar::engine().file_type(path).then(
      [path, num_buckets](std::optional<seastar::directory_entry_type> st) {
        if (!st.has_value()) {
          // create directory, init.
          return create_store(path, num_buckets).then([num_buckets] {
            return seastar::make_ready_future<uint32_t>(num_buckets);
          });
        } else if (*st != seastar::directory_entry_type::directory) {
          return seastar::make_exception_future<uint32_t>(std::runtime_error(
              fmt::format("path {} exists but is not a directory", path)
          ));
        }

        return open_store(path);
      }
  );
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

seastar::future<bool> sharded_store::remove(const seastar::sstring& key) {
  auto shard = _cmap->get_shard(key);
  applog.debug("remove key {} on shard {}", key, shard);
  return _shards.invoke_on(shard, &store_shard::remove, key);
}

struct op_holder {
  std::atomic<int> op;

  op_holder() : op(0) {}
};

seastar::future<> sharded_store::list(seastar::lw_shared_ptr<lst_holder> out_lst
) {
  applog.debug("list2 from all shards");
  seastar::lw_shared_ptr<op_holder> op = seastar::make_lw_shared<op_holder>();
  return _shards.invoke_on_all(
      [op, out_lst](store_shard& shard) mutable -> seastar::future<> {
        int _op = op->op.fetch_add(1);
        applog.debug(
            "list2 on shard {} (op {})", seastar::this_shard_id(), _op
        );
        const auto keys = co_await shard.list();
        out_lst->insert(std::move(keys));
        co_return;
      }
  );
}

}  // namespace store

}  // namespace foo
