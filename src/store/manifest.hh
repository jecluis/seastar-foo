/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <fmt/format.h>

#include <map>
#include <seastar/core/future.hh>
#include <set>

namespace foo {

namespace store {

// Maintains the mapping between this bucket's keys and filenames where data is
// kept.
class bucket_manifest {
  const int key_size = 255 + 1;  // key + terminator character
  const int fname_size = 20;

  std::string _manifest_path;
  std::map<std::string, std::string> _key_to_fname_map;
  std::set<std::string> _fname_set;

 public:
  bucket_manifest(const std::string& bucket_path)
      : _manifest_path(fmt::format("{}/manifest", bucket_path)) {}

  bucket_manifest(bucket_manifest&) = delete;
  bucket_manifest(const bucket_manifest&) = delete;

  ~bucket_manifest() = default;

  seastar::future<> init() {
    return load_manifest().handle_exception([](auto) {});
  }

  seastar::future<std::string> put(const std::string& key);
  std::optional<std::string> get(const std::string& key);
  bool exists(const std::string& key);
  seastar::future<std::optional<std::string>> remove(const std::string& key);
  std::set<std::string> list();

 private:
  seastar::future<> load_manifest();
  void load_entries(seastar::temporary_buffer<char>& tbuf);

  seastar::future<> write_manifest();
};

}  // namespace store

}  // namespace foo
