/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

#include "store.hh"

namespace foo {

namespace httpd {

constexpr int MAX_KEY_LEN = 255;

class store_get_handler : public seastar::httpd::handler_base {
  seastar::lw_shared_ptr<foo::store::sharded_store> _store;

 public:
  store_get_handler(seastar::lw_shared_ptr<foo::store::sharded_store> store)
      : _store(store) {}

  virtual seastar::future<std::unique_ptr<seastar::http::reply>> handle(
      const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
      std::unique_ptr<seastar::http::reply> rep
  );
};

class store_put_handler : public seastar::httpd::handler_base {
  seastar::lw_shared_ptr<foo::store::sharded_store> _store;

 public:
  store_put_handler(seastar::lw_shared_ptr<foo::store::sharded_store> store)
      : _store(store) {}

  virtual seastar::future<std::unique_ptr<seastar::http::reply>> handle(
      const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
      std::unique_ptr<seastar::http::reply> rep
  );
};

}  // namespace httpd

}  // namespace foo
