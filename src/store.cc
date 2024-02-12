/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "store.hh"

#include <boost/lexical_cast.hpp>
#include <cstdint>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>
#include <stdexcept>

static seastar::logger applog(__FILE__);

namespace foo {

namespace store {

seastar::future<> create_store(
    const seastar::sstring& path, uint32_t num_buckets
) {
  applog.info("creating store at {}", path);

  return seastar::make_directory(path).then([path, num_buckets] {
    auto fname = fmt::format("{}/num_buckets", path);
    auto flags = seastar::open_flags::create | seastar::open_flags::rw;
    applog.debug("write num buckets {} to file", num_buckets);
    auto buckets_str = fmt::format("{}", num_buckets);

    return seastar::with_file(
        seastar::open_file_dma(fname, flags),
        [buckets_str = std::move(buckets_str)](seastar::file& f) {
          // run async, ensure we wait for dma_write() while we hold
          // 'buckets_str'.
          return seastar::async([buckets_str = std::move(buckets_str), &f] {
            (void)f.dma_write(0, buckets_str.c_str(), buckets_str.size()).get();
          });
        }
    );
  });
}

seastar::future<uint32_t> open_store(const seastar::sstring& path) {
  auto fname = fmt::format("{}/num_buckets", path);
  auto flags = seastar::open_flags::ro;
  applog.debug("read num buckets file at {}", fname);

  return seastar::with_file(
      seastar::open_file_dma(fname, flags),
      [](seastar::file& f) {
        auto fsize = f.size().get();
        assert(fsize > 0);

        return f.dma_read_exactly<char>(0, fsize).then(
            [](seastar::temporary_buffer<char> tbuf) {
              try {
                return seastar::make_ready_future<uint32_t>(
                    boost::lexical_cast<uint32_t>(tbuf.get(), tbuf.size())
                );
              } catch (const boost::bad_lexical_cast&) {
                return seastar::make_exception_future<uint32_t>(
                    std::runtime_error(
                        "unable to obtain store's number of buckets"
                    )
                );
              }
            }
        );
      }
  );
}

seastar::future<uint32_t> open_or_create(
    const seastar::sstring& path, uint32_t num_buckets
) {
  return seastar::async([path, num_buckets] {
    return seastar::engine()
        .file_type(path)
        .then([path,
               num_buckets](std::optional<seastar::directory_entry_type> st) {
          if (!st.has_value()) {
            // create directory, init.
            return create_store(path, num_buckets).then([num_buckets] {
              return seastar::make_ready_future<uint32_t>(num_buckets);
            });
          } else if (*st != seastar::directory_entry_type::directory) {
            return seastar::make_exception_future<uint32_t>(std::runtime_error(
                fmt::format("path {} exists but is not a directory", path)
            ));
          }

          return open_store(path);
        })
        .get();
  });
}

}  // namespace store

}  // namespace foo
