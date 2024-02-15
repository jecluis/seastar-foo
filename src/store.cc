/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store.hh"

#include <atomic>
#include <boost/lexical_cast.hpp>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>
#include <set>
#include <stdexcept>
#include <utility>

#include "store_value.hh"
#include "utils.hh"

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

seastar::future<> bucket_manifest::load_manifest() {
  applog.debug("load manifest at '{}'", _manifest_path);
  auto flags = seastar::open_flags::ro;
  return seastar::with_file(
      seastar::open_file_dma(_manifest_path, flags),
      [this](seastar::file& f) {
        auto fsize = f.size().get();
        return f.dma_read<char>(0, fsize).then(
            [this](seastar::temporary_buffer<char> tbuf) {
              load_entries(tbuf);
              return seastar::make_ready_future<>();
            }
        );
      }
  );
}

void bucket_manifest::load_entries(seastar::temporary_buffer<char>& tbuf) {
  applog.debug("load entries from buffer size {}", tbuf.size());
  size_t pos = 0;
  while (pos < tbuf.size() && (pos + key_size + fname_size) <= tbuf.size()) {
    const char* buf = tbuf.get();
    const char* key_pos = buf + pos;
    const char* fname_pos = key_pos + key_size;

    auto raw_key = std::string(key_pos, key_size);
    auto fpos = raw_key.find_first_of('\0');
    auto key = std::string(raw_key.c_str(), fpos);
    auto fname = std::string(fname_pos, fname_size);
    applog.debug("load manifest, key '{}' fname '{}'", key, fname);

    _key_to_fname_map[key] = fname;
    _fname_set.insert(fname);

    pos += key_size + fname_size;
  }
}

seastar::future<> bucket_manifest::write_manifest() {
  applog.debug(
      "write manifest to '{}', keys {} fnames {}", _manifest_path,
      _key_to_fname_map.size(), _fname_set.size()
  );

  auto flags = seastar::open_flags::create | seastar::open_flags::truncate |
               seastar::open_flags::rw;
  return seastar::with_file(
      seastar::open_file_dma(_manifest_path, flags),
      [this](seastar::file& f) {
        auto manifest_size = _key_to_fname_map.size() * (key_size + fname_size);

        constexpr size_t alignment(4096u);
        auto tbuf =
            seastar::allocate_aligned_buffer<char>(manifest_size, alignment);
        auto buf = tbuf.get();
        memset(buf, '\0', manifest_size);

        size_t pos = 0;
        for (const auto& [key, value] : _key_to_fname_map) {
          assert(pos < manifest_size);
          assert(pos + key_size + fname_size <= manifest_size);
          assert(key.size() < key_size);
          memcpy(buf + pos, key.c_str(), key.size());
          memcpy(buf + pos + key_size, value.c_str(), fname_size);
          pos += key_size + fname_size;
        }

        return seastar::async([tbuf = std::move(tbuf), manifest_size, &f] {
          (void)f.dma_write(0, tbuf.get(), manifest_size).get();
        });
      }
  );
}

seastar::future<std::string> bucket_manifest::put(const std::string& key) {
  applog.debug("put into manifest key '{}'", key);
  const auto& it = _key_to_fname_map.find(key);
  if (it != _key_to_fname_map.cend()) {
    applog.debug("key already exists, fname '{}'", it->second);
    return seastar::make_ready_future<std::string>(it->second);
  }

  applog.debug("generate new fname for key '{}'", key);
  std::string fname;
  while (true) {
    fname = foo::gen_rnd_str(fname_size);
    if (!_fname_set.contains(fname)) {
      break;
    }
  }
  applog.debug("adding key '{}' to manifest, fname '{}'", key, fname);

  _key_to_fname_map[key] = fname;
  _fname_set.insert(fname);
  return write_manifest().then([key, fname = std::move(fname)] {
    applog.debug("wrote manifest with new key '{}'", key);
    return seastar::make_ready_future<std::string>(std::move(fname));
  });
}

std::optional<std::string> bucket_manifest::get(const std::string& key) {
  applog.debug("obtain key '{}' ({}b) from manifest", key, key.size());
  std::string lst;
  for (auto& [k, v] : _key_to_fname_map) {
    lst += fmt::format("'{} ({}b)' ", k, k.size());
  }
  applog.debug("keys in manifest: {}", lst);
  const auto& it = _key_to_fname_map.find(key);
  if (it == _key_to_fname_map.cend()) {
    return std::nullopt;
  }
  return it->second;
}

bool bucket_manifest::exists(const std::string& key) {
  const auto& it = _key_to_fname_map.find(key);
  return (it != _key_to_fname_map.cend());
}

seastar::future<std::optional<std::string>> bucket_manifest::remove(
    const std::string& key
) {
  applog.debug("remove key '{}' from manifest", key);
  const auto& it = _key_to_fname_map.find(key);
  if (it == _key_to_fname_map.cend()) {
    applog.debug("key '{}' does not exist in manifest", key);
    return seastar::make_ready_future<std::optional<std::string>>(std::nullopt);
  }
  auto fname = it->second;
  _fname_set.erase(fname);
  _key_to_fname_map.erase(it);

  return write_manifest().then([fname] {
    return seastar::make_ready_future<std::optional<std::string>>(fname);
  });
}

std::set<std::string> bucket_manifest::list() {
  std::set<std::string> s;
  for (const auto& v : _key_to_fname_map) {
    s.insert(v.first);
  }
  return s;
}

seastar::future<> store_bucket::init() {
  applog.debug("init store bucket at '{}'", _path);
  return seastar::engine()
      .file_type(_path)
      .then([this](std::optional<seastar::directory_entry_type> st) {
        if (!st.has_value()) {
          applog.debug("create store bucket directory at {}", _path);
          return seastar::make_directory(_path);
        } else if (*st != seastar::directory_entry_type::directory) {
          return seastar::make_exception_future<>(std::runtime_error(
              fmt::format("path {} exists but is not a directory", _path)
          ));
        }
        return seastar::make_ready_future<>();
      })
      .then([this] { return _manifest.init(); });
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
    const seastar::sstring& key
) {
  auto fname = _manifest.get(key);
  if (!fname) {
    return seastar::make_ready_future<foo::store::value_ptr>(nullptr);
  }
  auto fpath = fmt::format("{}/{}", _path, *fname);
  auto flags = seastar::open_flags::ro;
  applog.debug("read value for key '{}' at '{}'", key, fpath);

  return seastar::with_file(
      seastar::open_file_dma(fpath, flags),
      [](seastar::file& f) {
        auto fsize = f.size().get();

        return f.dma_read_exactly<char>(0, fsize).then([](auto buf) {
          return seastar::make_ready_future<foo::store::value_ptr>(
              foo::store::make_value_ptr_by_copy(buf.get(), buf.size())
          );
        });
      }
  );
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

seastar::future<> store_shard::init() {
  auto shard_id = seastar::this_shard_id();
  auto bucket_ids = _cmap.get_shard_buckets(shard_id);

  for (auto& bid : bucket_ids) {
    auto bucket_path = fmt::format("{}/{}", _store_path, bid);
    _buckets[bid] = store_bucket_ptr(new store_bucket(bucket_path));
  }

  return seastar::parallel_for_each(
      _buckets.begin(), _buckets.end(),
      [](auto& entry) {
        applog.debug("init store bucket {}", entry.first);
        return entry.second->init();
      }
  );
}

seastar::future<> store_shard::put(foo::store::insert_entry_ptr entry) {
  const seastar::sstring& key = entry->key();
  auto bucket = _cmap.get_bucket(key);
  const auto target_shard = _cmap.get_shard(key);
  if (target_shard != seastar::this_shard_id()) {
    throw std::runtime_error("wrong shard");
  }

  if (!_buckets.contains(bucket)) {
    throw std::runtime_error("expected to own bucket");
  }
  co_await _buckets[bucket]->put(entry);
  auto res = _cache.put(entry);
  if (!res) {
    applog.error("unable to store key/value in cache");
  }
}

seastar::future<foo::store::value_ptr> store_shard::get(
    const seastar::sstring& key
) {
  auto bucket = _cmap.get_bucket(key);
  const auto target_shard = _cmap.get_shard(key);
  if (target_shard != seastar::this_shard_id()) {
    return seastar::make_exception_future<foo::store::value_ptr>(
        std::runtime_error("wrong shard")
    );
  }

  if (!_buckets.contains(bucket)) {
    return seastar::make_exception_future<foo::store::value_ptr>(
        std::runtime_error("expected to own bucket")
    );
  }

  auto cache_value = _cache.get(key);
  if (cache_value) {
    // in cache, return value
    applog.debug("obtained key '{}' from cache", key);
    return seastar::make_ready_future<foo::store::value_ptr>(cache_value);
  }

  // must obtain from disk
  return _buckets[bucket]->get(key).then([this, key](auto data) {
    if (data) {
      _cache.put_ptr(key, data);
    }
    return seastar::make_ready_future<foo::store::value_ptr>(data);
  });
}

seastar::future<bool> store_shard::remove(const seastar::sstring& key) {
  auto bucket = _cmap.get_bucket(key);
  const auto target_shard = _cmap.get_shard(key);
  if (target_shard != seastar::this_shard_id()) {
    return seastar::make_exception_future<bool>(std::runtime_error("wrong shard"
    ));
  }

  if (!_buckets.contains(bucket)) {
    return seastar::make_exception_future<bool>(
        std::runtime_error("expected to own bucket")
    );
  }

  return _buckets[bucket]->remove(key).then([this, key] {
    _cache.remove(key);
    return seastar::make_ready_future<bool>(true);
  });
}

seastar::future<std::set<std::string>> store_shard::list() {
  std::set<std::string> s;
  for (auto& b : _buckets) {
    const auto& k = b.second->list();
    s.insert(k.cbegin(), k.cend());
  }
  return seastar::make_ready_future<std::set<std::string>>(s);
}

seastar::future<> store_shard::stop() {
  applog.debug("stop store shard");
  return seastar::make_ready_future<>();
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

seastar::future<foo::store::value_ptr> sharded_store::get(
    const seastar::sstring& key
) {
  auto shard = _cmap->get_shard(key);
  applog.debug("get key {} on shard {}", key, shard);
  return _shards.invoke_on(shard, &store_shard::get, key);
}

seastar::future<bool> sharded_store::remove(const seastar::sstring& key) {
  auto shard = _cmap->get_shard(key);
  applog.debug("remove key {} on shard {}", key, shard);
  return _shards.invoke_on(shard, &store_shard::remove, key);
}

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
