/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "httpd.hh"

#include <seastar/core/future.hh>
#include <seastar/http/url.hh>
#include <seastar/util/log.hh>

#include "store_value.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace httpd {

seastar::future<std::unique_ptr<seastar::http::reply>>
store_get_handler::handle(
    const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
    std::unique_ptr<seastar::http::reply> rep
) {
  auto bad_request = [&rep] {
    rep->set_status(seastar::http::reply::status_type::bad_request).done();
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
        std::move(rep)
    );
  };

  applog.info("got GET request from {}", req->get_client_address());
  if (!req->param.exists("key")) {
    return bad_request();
  }

  // NOTE(joao): The request parameters will be populated, even if no
  // additional path is supplied. Instead, the 'key' parameter will be an
  // empty string. Very confusing, I'd argue a bug even.
  const seastar::sstring& raw_key = req->param.at("key");
  if (raw_key.empty()) {
    return bad_request();
  }

  // ignore the path's forward slash.
  const seastar::sstring& encoded_key = raw_key.substr(1);
  // NOTE(joao): even if the behavior may be buggy, it seems that if the value
  // is not empty, it will also not contain solely the forward slash; but it
  // will contain the forward slash if it's not empty.
  assert(!encoded_key.empty());

  seastar::sstring key;
  if (!seastar::http::internal::url_decode(encoded_key, key)) {
    return bad_request();
  }

  if (key.size() > MAX_KEY_LEN) {
    return bad_request();
  }

  applog.debug("obtain key '{}'", key);

  return _store->get(key).then([rep = std::move(rep),
                                key](foo::store::value_ptr data) mutable {
    if (!data) {
      applog.debug("key '{}' not available", key);
      rep->set_status(seastar::http::reply::status_type::not_found).done();
      return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
          std::move(rep)
      );
    }

    applog.debug("found item with key '{}'", key);
    seastar::sstring body_text(data->data(), data->size());
    rep->write_body("text", body_text);
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
        std::move(rep)
    );
  });
}

}  // namespace httpd

}  // namespace foo
