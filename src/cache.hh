/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

// This implementation is heavily influenced by seastar's "memcache"
// implementation. Without it, a lot of the mechanics here used would not have
// been possible to be investigated (namely boost::intrusive), and applied.
//
// We do not aim at implementing an optimal cache at this point.

#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/intrusive/options.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer-set.hh>
#include <seastar/util/log.hh>

#include "store_value.hh"

namespace foo {
namespace cache {

class not_enough_space_error : public std::exception {};

namespace bi = boost::intrusive;

using clock_type = seastar::lowres_clock;

// Cache entry
class cache_item {
 public:
  using time_point = clock_type::time_point;
  using duration = time_point::duration;

  bi::list_member_hook<> timer_link;
  bi::unordered_set_member_hook<> cache_link;
  bi::list_member_hook<> lru_link;
  bi::list_member_hook<> lst_link;

 private:
  size_t _key_hash;
  seastar::sstring _key;
  // seastar::sstring _value;
  foo::store::value_ptr _value;
  time_point _expiration;
  uint32_t _refcnt;

  void init(foo::store::value_ptr value, uint32_t ttl) {
    _value = value;
    // calculate expiration based on provided ttl
    touch(ttl);
    // keep key's hash so we don't have to calculate it every time we want it.
    _key_hash = std::hash<seastar::sstring>()(_key);
  }

 public:
  cache_item(
      const seastar::sstring&& key, const seastar::sstring&& value, uint32_t ttl
  )
      : _key(key), _refcnt(0) {
    // move value data to a shared_ptr created by us.
    char* buf = new char[value.size()];
    memcpy(buf, value.c_str(), value.size());
    auto v = foo::store::make_value_ptr(buf, value.size());
    init(v, ttl);
  }

  cache_item(
      const seastar::sstring& key, foo::store::value_ptr value, uint32_t ttl
  )
      : _key(key), _refcnt(0) {
    init(value, ttl);
  }

  cache_item(const cache_item&) = delete;
  cache_item(cache_item&&) = delete;
  ~cache_item() = default;

  size_t hash() const { return _key_hash; }
  size_t key_size() const { return _key.size(); }
  size_t value_size() const { return _value->size(); }
  time_point get_timeout() const { return _expiration; }

  const seastar::sstring& key() const { return _key; }
  const foo::store::value_ptr value() const { return _value; }

  void touch(uint64_t ttl) {
    _expiration = clock_type::now() + std::chrono::seconds(ttl);
  }

  // required by timer_set
  bool cancel() { return false; }

  // required by intrusive unordered_set
  // NOTE(joao): needs to be 'friend', although it's not clear "why" right now.
  friend std::size_t hash_value(const cache_item& item) { return item.hash(); }

  // required by intrusive list
  // NOTE(joao): needs to be 'friend', although it's not clear "why" right now.
  friend bool operator==(const cache_item& a, const cache_item& b) {
    return (
        a.hash() == b.hash() && a.key() == b.key() && a.value() == b.value()
    );
  }

  friend inline void intrusive_ptr_add_ref(cache_item* item) {
    assert(item->_refcnt >= 0);
    ++item->_refcnt;
  }

  friend inline void intrusive_ptr_release(cache_item* item) {
    --item->_refcnt;
    assert(item->_refcnt >= 0);
    if (item->_refcnt == 0) {
      delete item;
    }
  }
};

using cache_item_ptr = seastar::foreign_ptr<boost::intrusive_ptr<cache_item>>;

/*
 * Used to perform less expensive lookups on an intrusive unordered_set, as
 * seen in
 *
 * https://www.boost.org/doc/libs/1_84_0/doc/html/intrusive/advanced_lookups_insertions.html
 * 
 */
class item_cmp {
  bool compare(const seastar::sstring& key, const cache_item& it) const {
    size_t key_hash = std::hash<seastar::sstring>()(key);
    return (it.hash() == key_hash && key == it.key());
  }

 public:
  bool operator()(const cache_item& it, const seastar::sstring& key) const {
    return compare(key, it);
  }

  bool operator()(const seastar::sstring& key, const cache_item& it) const {
    return compare(key, it);
  }
};

// The actual cache
class cache {
  using cache_type = bi::unordered_set<
      cache_item, bi::member_hook<
                      cache_item, bi::unordered_set_member_hook<>,
                      &cache_item::cache_link>>;
  using lru_type = bi::list<
      cache_item,
      bi::member_hook<
          cache_item, bi::list_member_hook<>, &cache_item::lru_link>>;

  size_t _max_cache_size;
  size_t _estimated_cache_size;
  uint32_t _ttl;
  std::vector<cache_type::bucket_type> _buckets;
  cache_type _cache;
  lru_type _lru;
  seastar::timer_set<cache_item, &cache_item::timer_link> _exp_timers;
  seastar::timer<clock_type> _timer;

  void _drop(const seastar::sstring& key);
  bool _put(cache_item* item);
  void flush();

 public:
  cache(size_t bucket_count, size_t max_cache_size, uint32_t ttl)
      : _max_cache_size(max_cache_size),
        _estimated_cache_size(0),
        _ttl(ttl),
        _buckets(bucket_count),
        _cache(cache_type::bucket_traits(_buckets.data(), bucket_count)) {
    _timer.set_callback([&] { expire(); });
  }
  cache(const cache&) = delete;
  cache(cache&) = delete;

  ~cache() {
    // clear cache
    flush();
  }

  inline cache_type::iterator find(const seastar::sstring& key) {
    return _cache.find(key, std::hash<seastar::sstring>(), item_cmp());
  }

  bool put(
      const seastar::sstring&& key, const seastar::sstring&& value,
      bool local = true
  );
  // unfortunately, it seems 'invoke_on' gets confused if we have two 'put()'
  // functions with different arguments.
  bool put_ptr(const seastar::sstring& key, foo::store::value_ptr value);

  bool remove(const seastar::sstring& key);

  // Obtain an item from cache, if available. If not available returns nullptr.
  foo::store::value_ptr get(const seastar::sstring& key);

 private:
  // Remove a given item from the cache, freeing up space taken.
  void remove_item(cache_item& item, bool expired, bool in_cache = true);

  // Returns the estimated size of a given item.
  size_t item_size(cache_item& item) {
    return sizeof(cache_item) + item.key_size() + item.value_size();
  }

  // Checks whether there's enough available space in the cache for the
  // additional required bytes.
  bool has_required_space(size_t required) {
    return _estimated_cache_size + required <= _max_cache_size;
  }

  // Find or create required space in the cache.
  // Will remove the least recently used items until enough space is freed.
  void find_cache_space(size_t required) {
    if (required > _max_cache_size) {
      throw not_enough_space_error();
    }

    if (has_required_space(required)) {
      return;
    }

    while (!_cache.empty()) {
      auto& item = _lru.back();
      remove_item(item, false);

      if (has_required_space(required)) {
        break;
      }
    }
  }

  // Drop items that have hit their time to live in the cache.
  void expire() {
    auto expired = _exp_timers.expire(clock_type::now());
    while (!expired.empty()) {
      auto& item = *expired.begin();
      expired.pop_front();
      remove_item(item, true);
    }
    _timer.arm(_exp_timers.get_next_timeout());
  }
};

}  // namespace cache
}  // namespace foo
