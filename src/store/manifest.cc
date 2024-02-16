/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store/manifest.hh"

#include <optional>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include "utils.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace store {

// read manifest from disk
seastar::future<> bucket_manifest::load_manifest() {
  applog.debug("load manifest at '{}'", _manifest_path);
  auto flags = seastar::open_flags::ro;

  auto f = co_await seastar::open_file_dma(_manifest_path, flags);
  auto fsize = co_await f.size();
  auto buf = co_await f.dma_read<char>(0, fsize);
  load_entries(buf);
  co_await f.close();
}

// load individual manifest entries from the buffer read from disk
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

// write manifest to disk
seastar::future<> bucket_manifest::write_manifest() {
  applog.debug(
      "write manifest to '{}', keys {} fnames {}", _manifest_path,
      _key_to_fname_map.size(), _fname_set.size()
  );

  auto flags = seastar::open_flags::create | seastar::open_flags::truncate |
               seastar::open_flags::rw;

  auto f = co_await seastar::open_file_dma(_manifest_path, flags);
  auto manifest_size = _key_to_fname_map.size() * (key_size + fname_size);

  constexpr size_t alignment(4096u);
  auto tbuf = seastar::allocate_aligned_buffer<char>(manifest_size, alignment);
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

  co_await f.dma_write(0, tbuf.get(), manifest_size);
  co_await f.flush();
  co_await f.close();
}

seastar::future<std::string> bucket_manifest::put(const std::string& key) {
  applog.debug("put into manifest key '{}'", key);
  const auto& it = _key_to_fname_map.find(key);
  if (it != _key_to_fname_map.cend()) {
    applog.debug("key already exists, fname '{}'", it->second);
    co_return it->second;
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

  // keep a copy of key in case it doesn't live long enough across co_await
  const seastar::sstring skey(key);

  _key_to_fname_map[skey] = fname;
  _fname_set.insert(fname);
  co_await write_manifest();
  applog.debug("wrote manifest with new key '{}'", skey);
  co_return fname;
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
    co_return std::nullopt;
  }
  auto fname = it->second;
  _fname_set.erase(fname);
  _key_to_fname_map.erase(it);

  co_await write_manifest();
  co_return fname;
}

std::set<std::string> bucket_manifest::list() {
  std::set<std::string> s;
  for (const auto& v : _key_to_fname_map) {
    s.insert(v.first);
  }
  return s;
}

}  // namespace store

}  // namespace foo
