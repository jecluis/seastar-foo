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

class store_httpd_handler : public seastar::httpd::handler_base {
 protected:
  foo::store::sharded_store& _store;

 public:
  store_httpd_handler(foo::store::sharded_store& store) : _store(store) {}

  using http_request = std::unique_ptr<seastar::http::request>;
  using http_reply = std::unique_ptr<seastar::http::reply>;

  virtual seastar::future<http_reply> handle(
      const seastar::sstring& path, http_request req, http_reply rep
  );

  virtual seastar::future<http_reply> handle_request(
      const seastar::sstring& path, http_request req, http_reply rep
  ) = 0;
};

// GET
class store_get_handler : public store_httpd_handler {
 public:
  store_get_handler(foo::store::sharded_store& store)
      : store_httpd_handler(store) {}

  virtual seastar::future<http_reply> handle_request(
      const seastar::sstring& path, http_request req, http_reply rep
  );
};

// PUT
class store_put_handler : public store_httpd_handler {
 public:
  store_put_handler(foo::store::sharded_store& store)
      : store_httpd_handler(store) {}

  virtual seastar::future<http_reply> handle_request(
      const seastar::sstring& path, http_request req, http_reply rep
  );
};

// DELETE
class store_delete_handler : public store_httpd_handler {
 public:
  store_delete_handler(foo::store::sharded_store& store)
      : store_httpd_handler(store) {}

  virtual seastar::future<http_reply> handle_request(
      const seastar::sstring& path, http_request req, http_reply rep
  );
};

// List objects
class store_list_handler : public store_httpd_handler {
 public:
  store_list_handler(foo::store::sharded_store& store)
      : store_httpd_handler(store) {}

  virtual seastar::future<http_reply> handle_request(
      const seastar::sstring& path, http_request req, http_reply rep
  );
};

}  // namespace httpd

}  // namespace foo
