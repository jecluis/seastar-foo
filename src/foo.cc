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
#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/common.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/url.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

#include "cmap.hh"
#include "httpd.hh"
#include "store.hh"

using namespace seastar;

namespace bpo = boost::program_options;

static logger applog(__FILE__);

int main(int argc, char** argv) {
  seastar::distributed<foo::store::store_shard> store_shards;
  foo::store::sharded_store store(store_shards);

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
      "maximum cache size, in bytes, per shard"
  );
  app.add_options()(
      "cache-ttl", bpo::value<uint32_t>()->default_value(60),
      "cache entry time to live, in seconds"
  );
  app.add_options()(
      "cache-buckets", bpo::value<size_t>()->default_value(1 << 7),
      "number of backing buckets for the cache"
  );
  app.add_options()(
      "store-buckets", bpo::value<size_t>()->default_value(1 << 7),
      "number of file system buckets"
  );
  app.add_options()(
      "store-path", bpo::value<seastar::sstring>()->required(), "path to store"
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

      size_t store_bucket_count = config["store-buckets"].as<size_t>();
      seastar::sstring store_path = config["store-path"].as<seastar::sstring>();

      foo::consistent_map_ptr cmap =
          seastar::make_lw_shared<foo::consistent_map>(
              foo::consistent_map(store_bucket_count, seastar::smp::count)
          );

      store.init(cmap);

      applog.debug("httpd addr: {}", httpd_addr);
      applog.debug(
          "cache max size: {}, ttl: {}, buckets: {}", cache_size, cache_ttl,
          cache_bucket_count
      );

      engine().at_exit([&] {
        applog.info("stop store shards");
        return store_shards.stop();
      });

      return foo::store::open_or_create(store_path, store_bucket_count)
          .then([store_path](uint32_t num_buckets) {
            applog.info(
                "opened store at {} with {} buckets", store_path, num_buckets
            );
            return make_ready_future<>();
          })
          .then([cmap] {
            auto buckets = cmap->get_shard_buckets(seastar::this_shard_id());
            applog.debug("owned buckets: {}", buckets.size());
            for (auto& bid : buckets) {
              applog.debug("owning bucket {}", bid);
            }
          })
          .then([&store_shards, store_path, cmap, cache_bucket_count,
                 cache_size, cache_ttl] {
            applog.debug("start store shards");
            return store_shards.start(
                store_path, cmap, cache_bucket_count, cache_size, cache_ttl
            );
          })
          .then([&store_shards] {
            return store_shards.invoke_on_all([](auto& shard) {
              return shard.init();
            });
          })
          .then([&store, httpd_addr] {
            return seastar::async([&store, httpd_addr] {
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
                  .then([&store, httpd_server] {
                    (void)httpd_server->set_routes([&store](httpd::routes& r) {
                      r.add(
                          httpd::operation_type::GET,
                          httpd::url("/get").remainder("key"),
                          new foo::httpd::store_get_handler(store)
                      );
                      r.add(
                          httpd::operation_type::PUT,
                          httpd::url("/put").remainder("key"),
                          new foo::httpd::store_put_handler(store)
                      );
                      r.add(
                          httpd::operation_type::DELETE,
                          httpd::url("/remove").remainder("key"),
                          new foo::httpd::store_delete_handler(store)
                      );
                      r.add(
                          httpd::operation_type::GET, httpd::url("/list"),
                          new foo::httpd::store_list_handler(store)
                      );
                    });
                  })
                  .then([httpd_server, httpd_addr] {
                    applog.debug("listen on {}", httpd_addr);
                    (void)httpd_server->listen(httpd_addr)
                        .handle_exception([httpd_addr](auto ep) {
                          applog.error(
                              "error listening on {}: {}", httpd_addr, ep
                          );
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
