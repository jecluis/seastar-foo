/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "httpd.hh"

#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/http/url.hh>
#include <seastar/util/log.hh>

#include "seastar/http/reply.hh"
#include "store_value.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace httpd {

seastar::future<std::unique_ptr<seastar::http::reply>> _return_bad_request(
    std::unique_ptr<seastar::http::reply> rep
) {
  rep->set_status(seastar::http::reply::status_type::bad_request).done();
  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
      std::move(rep)
  );
}

std::optional<seastar::sstring> _get_param_key(seastar::http::request* req) {
  if (!req->param.exists("key")) {
    return std::nullopt;
  }

  // NOTE(joao): The request parameters will be populated, even if no
  // additional path is supplied. Instead, the 'key' parameter will be an
  // empty string. Very confusing, I'd argue a bug even.
  const seastar::sstring& raw_key = req->param.at("key");
  if (raw_key.empty()) {
    return std::nullopt;
  }

  // ignore the path's forward slash.
  const seastar::sstring& encoded_key = raw_key.substr(1);
  // NOTE(joao): even if the behavior may be buggy, it seems that if the value
  // is not empty, it will also not contain solely the forward slash; but it
  // will contain the forward slash if it's not empty.
  assert(!encoded_key.empty());

  seastar::sstring key;
  if (!seastar::http::internal::url_decode(encoded_key, key)) {
    return std::nullopt;
  }

  if (key.size() > MAX_KEY_LEN) {
    return std::nullopt;
  }
  return key;
}

seastar::future<std::unique_ptr<seastar::http::reply>>
store_get_handler::handle(
    const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
    std::unique_ptr<seastar::http::reply> rep
) {
  applog.info("got GET request from {}", req->get_client_address());
  auto key = _get_param_key(req.get());
  if (!key) {
    return _return_bad_request(std::move(rep));
  }

  applog.debug("obtain key '{}'", *key);

  return _store.get(*key).then([rep = std::move(rep),
                                key](foo::store::value_ptr data) mutable {
    if (!data) {
      applog.debug("key '{}' not available", *key);
      rep->set_status(seastar::http::reply::status_type::not_found).done();
      return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
          std::move(rep)
      );
    }

    applog.debug("found item with key '{}'", *key);
    seastar::sstring body_text(data->data(), data->size());
    rep->write_body("text", body_text);
    return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(
        std::move(rep)
    );
  });
}

seastar::future<std::unique_ptr<seastar::http::reply>>
store_put_handler::handle(
    const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
    std::unique_ptr<seastar::http::reply> rep
) {
  applog.info("got PUT request from {}", req->get_client_address());
  auto key = _get_param_key(req.get());
  if (!key) {
    return _return_bad_request(std::move(rep));
  }

  auto insert_key = *key;
  auto& content = req->content;
  auto content_size = req->content_length;

  applog.debug("add key '{}' size {}", *key, content_size);

  return _store.put(std::move(insert_key), std::move(content))
      .then([rep = std::move(rep)]() mutable {
        rep->set_status(seastar::http::reply::status_type::ok).done();
        return seastar::make_ready_future<
            std::unique_ptr<seastar::http::reply>>(std::move(rep));
      });
}

}  // namespace httpd

}  // namespace foo
