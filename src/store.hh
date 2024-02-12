/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

namespace foo {

namespace store {

class store_shard {};

class sharded_store {};

seastar::future<uint32_t> open_or_create(
    const seastar::sstring& path, uint32_t num_buckets
);

}  // namespace store

}  // namespace foo
