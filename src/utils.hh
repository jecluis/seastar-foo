/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#pragma once

#include <algorithm>
#include <random>
#include <string>

namespace foo {

static const std::string avail_chars(
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
);

inline std::string gen_rnd_str(size_t len) {
  auto rnd = std::mt19937(std::random_device()());
  auto dist = std::uniform_int_distribution<int>(0, avail_chars.length() - 1);

  std::string s(len, '\0');
  std::generate_n(s.begin(), 20, [&rnd, &dist] {
    return avail_chars[dist(rnd)];
  });
  return s;
}
}  // namespace foo
