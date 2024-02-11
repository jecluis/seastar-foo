/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/printf.h>

#include <cstdint>
#include <exception>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/url.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

#include "cache.hh"
#include "sharded_cache.hh"

using namespace seastar;

namespace bpo = boost::program_options;

static logger applog(__FILE__);

constexpr int MAX_KEY_LEN = 255;

class cache_handler : public httpd::handler_base {
  foo::cache::sharded_cache& _cache;

 public:
  cache_handler(foo::cache::sharded_cache& cache) : _cache(cache) {}

  virtual seastar::future<std::unique_ptr<http::reply>> handle(
      const sstring& path, std::unique_ptr<http::request> req,
      std::unique_ptr<http::reply> rep
  ) {
    auto bad_request = [&rep] {
      rep->set_status(http::reply::status_type::bad_request).done();
      return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
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
    if (!http::internal::url_decode(encoded_key, key)) {
      return bad_request();
    }

    if (key.size() > MAX_KEY_LEN) {
      return bad_request();
    }

    applog.debug("obtain key '{}'", key);

    return _cache.get(key).then([rep = std::move(rep),
                                 key](foo::cache::cache_item_ptr item) mutable {
      if (!item) {
        applog.debug("key '{}' not available", key);
        rep->set_status(http::reply::status_type::not_found).done();
        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
      }

      applog.debug("found item with key '{}'", key);
      rep->write_body("text", item->value());
      return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
    });
  }
};

int main(int argc, char** argv) {
  seastar::distributed<foo::cache::cache> cache_peers;
  foo::cache::sharded_cache cache(cache_peers);

  app_template app;

  app.add_options()(
      "port", bpo::value<uint16_t>()->default_value(1337), "http server port"
  );
  app.add_options()(
      "address", bpo::value<sstring>()->default_value("0.0.0.0"),
      "http server address"
  );
  app.add_options()(
      "cache-max-size", bpo::value<size_t>()->default_value(100 * 1 << 20),
      "maximum cache size, in bytes"
  );
  app.add_options()(
      "cache-ttl", bpo::value<uint32_t>()->default_value(60),
      "cache entry time to live, in seconds"
  );
  app.add_options()(
      "cache-buckets", bpo::value<size_t>()->default_value(1 << 7),
      "number of backing buckets for the cache"
  );

  try {
    app.run(argc, argv, [&] {
      auto&& config = app.configuration();
      uint16_t httpd_port = config["port"].as<uint16_t>();
      net::inet_address addr(config["address"].as<sstring>());
      auto httpd_addr = make_ipv4_address({addr, httpd_port});

      size_t cache_size = config["cache-max-size"].as<size_t>();
      uint32_t cache_ttl = config["cache-ttl"].as<uint32_t>();
      size_t cache_bucket_count = config["cache-buckets"].as<size_t>();

      applog.debug("httpd addr: {}", httpd_addr);
      applog.debug(
          "cache max size: {}, ttl: {}, buckets: {}", cache_size, cache_ttl,
          cache_bucket_count
      );

      engine().at_exit([&] { return cache_peers.stop(); });

      return cache_peers.start(cache_bucket_count, cache_size, cache_ttl).then([&] {
        return seastar::async([&cache, httpd_addr] {
          // keep httpd server alive for as long as the task is not interrupted.
          seastar::condition_variable httpd_cond;

          auto httpd_server = new httpd::http_server_control();

          // ensure we stop the httpd server when we exit, probably because of
          // an interrupt.
          engine().at_exit([httpd_server, &httpd_cond] {
            applog.info("stopping httpd");
            return httpd_server->stop().then([&httpd_cond] {
              httpd_cond.broadcast();
            });
          });

          // start httpd server, set up routes, and listen.
          // listening involves invoking the underlying 'http_server's listen
          // function on all shards.
          // however, because the future returns after invoking the listen
          // function (instead of actively waiting while listening), our thread
          // will have to wait for a reason to stop the underlying 'http_server'
          // -- we'll do that with a condition, which will be triggered once the
          // http_server is stopped.
          //
          (void)httpd_server->start("httpd")
              .then([httpd_addr] {
                applog.info("starting httpd server at {}", httpd_addr);
              })
              .then([&cache, httpd_server] {
                (void)httpd_server->set_routes([&cache](httpd::routes& r) {
                  r.add(
                      httpd::operation_type::GET,
                      httpd::url("/get").remainder("key"),
                      new cache_handler(std::ref(cache))
                  );
                });
              })
              .then([httpd_server, httpd_addr] {
                applog.debug("listen on {}", httpd_addr);
                (void)httpd_server->listen(httpd_addr)
                    .handle_exception([httpd_addr](auto ep) {
                      applog.error("error listening on {}: {}", httpd_addr, ep);
                      return make_exception_future<>(ep);
                    })
                    .then([httpd_addr] {
                      applog.info("httpd listening on {}", httpd_addr);
                    });
              })
              .then([&httpd_cond] {
                applog.info("waiting for httpd server...");
                return httpd_cond.wait().then([] {
                  applog.info("httpd interrupted, leave.");
                });
              })
              .finally([] { applog.info("finish"); })
              .get();
        });
      });
    });
  } catch (...) {
    applog.error("couldn't start application: {}", std::current_exception());
    return 1;
  }
  return 0;
}
