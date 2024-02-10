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

#include <exception>
#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

using namespace seastar;

namespace bpo = boost::program_options;

static logger applog(__FILE__);

class cache_handler : public httpd::handler_base {
 public:
  virtual seastar::future<std::unique_ptr<http::reply>> handle(
      const sstring& path, std::unique_ptr<http::request> req,
      std::unique_ptr<http::reply> rep
  ) {
    applog.info("got GET request from {}", req->get_client_address());
    rep->write_body("json", "{'test': 'foo'}");
    return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
  }
};

int main(int argc, char** argv) {
  app_template app;

  app.add_options()(
      "port", bpo::value<uint16_t>()->default_value(1337), "http server port"
  );
  app.add_options()(
      "address", bpo::value<sstring>()->default_value("0.0.0.0"),
      "http server address"
  );

  try {
    app.run(argc, argv, [&] {
      auto&& config = app.configuration();
      uint16_t httpd_port = config["port"].as<uint16_t>();
      net::inet_address addr(config["address"].as<sstring>());
      auto httpd_addr = make_ipv4_address({addr, httpd_port});

      applog.debug("httpd addr: {}", httpd_addr);

      return seastar::async([httpd_addr] {
        // keep httpd server alive for as long as the task is not interrupted.
        seastar::condition_variable httpd_cond;

        auto httpd_server = new httpd::http_server_control();

        // ensure we stop the httpd server when we exit, probably because of an
        // interrupt.
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
            .then([httpd_server] {
              (void)httpd_server->set_routes([](httpd::routes& r) {
                r.add(
                    httpd::operation_type::GET, httpd::url("/get"),
                    new cache_handler()
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
  } catch (...) {
    applog.error("couldn't start application: {}", std::current_exception());
    return 1;
  }
  return 0;
}
