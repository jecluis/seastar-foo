/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store.hh"

#include <boost/lexical_cast.hpp>
#include <cstdint>
#include <cstring>
#include <optional>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>
#include <stdexcept>

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

    auto key = std::string(key_pos, key_size);
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
  applog.debug("obtain key '{}' from manifest", key);
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

seastar::future<> bucket_manifest::remove(const std::string& key) {
  applog.debug("remove key '{}' from manifest", key);
  const auto& it = _key_to_fname_map.find(key);
  if (it == _key_to_fname_map.cend()) {
    applog.debug("key '{}' does not exist in manifest", key);
    return seastar::make_ready_future<>();
  }
  _fname_set.erase(it->second);
  _key_to_fname_map.erase(it);

  return write_manifest();
}

std::vector<std::string> bucket_manifest::list() {
  std::vector<std::string> keys;
  for (const auto& [key, _] : _key_to_fname_map) {
    keys.push_back(key);
  }
  return keys;
}

}  // namespace store

}  // namespace foo
