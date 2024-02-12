/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

#include "sharded_cache.hh"

namespace foo {

namespace httpd {

constexpr int MAX_KEY_LEN = 255;

class cache_get_handler : public seastar::httpd::handler_base {
  foo::cache::sharded_cache& _cache;

 public:
  cache_get_handler(foo::cache::sharded_cache& cache) : _cache(cache) {}

  virtual seastar::future<std::unique_ptr<seastar::http::reply>> handle(
      const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
      std::unique_ptr<seastar::http::reply> rep
  );
};

}  // namespace httpd

}  // namespace foo
