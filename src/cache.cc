/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#include "cache.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include "store_value.hh"

static seastar::logger applog(__FILE__);

namespace foo {

namespace cache {

void cache::_drop(const seastar::sstring& key) {
  auto it = find(key);
  if (it != _cache.end()) {
    // exists, replace
    applog.debug("key '{}' already exists in cache, remove", key);
    auto& existing = *it;
    remove_item(existing, false);
  }
}

bool cache::_put(cache_item* item) {
  size_t required_size = item_size(*item);

  try {
    find_cache_space(required_size);
  } catch (not_enough_space_error) {
    applog.error(
        "not enough space in cache to store additional {} bytes", required_size
    );
    return false;
  }

  _cache.insert(*item);
  _lru.push_front(*item);
  _exp_timers.insert(*item);
  intrusive_ptr_add_ref(item);
  // This should not be needed, because the item we're adding will always have
  // a later timeout than whatever is the next timeout, except if this item is
  // the only one in the cache and was thus removed earlier in this function.
  // However, since 'timer_set' does not recalculate the next timeout on
  // removal, we will very likely end up with the same value anyway.
  _timer.rearm(_exp_timers.get_next_timeout());
  _estimated_cache_size += required_size;
  applog.debug(
      "added new entry: key '{}', required size '{}', cache size '{}'",
      item->key(), required_size, _estimated_cache_size
  );
  return true;
}

bool cache::put(
    const seastar::sstring&& key, const seastar::sstring&& value, bool local
) {
  applog.debug("put key '{}' into cache", key);

  _drop(key);
  cache_item* new_item =
      (local ? new cache_item(std::move(key), std::move(value), _ttl)
             : new cache_item(
                   std::forward<const seastar::sstring&&>(key),
                   std::forward<const seastar::sstring&&>(value), _ttl
               ));
  return _put(new_item);
}

bool cache::put_ptr(const seastar::sstring& key, foo::store::value_ptr value) {
  applog.debug("put key '{}' into cache", key);
  _drop(key);
  assert(value);
  cache_item* new_item = new cache_item(key, value, _ttl);
  return _put(new_item);
}

void cache::remove_item(cache_item& item, bool expired, bool in_cache) {
  applog.debug("remove key '{}' from cache", item.key());
  if (in_cache) {
    _cache.erase(_cache.iterator_to(item));
  }
  if (!expired) {
    _exp_timers.remove(item);
  }
  _lru.remove(item);
  _estimated_cache_size -= item_size(item);
  intrusive_ptr_release(&item);
}

bool cache::remove(const seastar::sstring& key) {
  applog.debug("remove key '{}' from cache", key);
  auto item = find(key);
  if (item == _cache.end()) {
    applog.debug("no such key '{}' in cache", key);
    return false;
  }
  remove_item(*item, false);
  return true;
}

foo::store::value_ptr cache::get(const seastar::sstring& key) {
  applog.debug("get key '{}'", key);
  auto it = find(key);
  if (it == _cache.end()) {
    applog.debug("no such key '{}' in cache", key);
    return nullptr;
  }
  it->touch(_ttl);
  _lru.remove(*it);
  _lru.push_front(*it);
  return it->value();
}

void cache::flush() {
  applog.debug("flush cache {} entries", _cache.size());
  _cache.erase_and_dispose(
      _cache.begin(), _cache.end(),
      [this](cache_item* item) { remove_item(*item, false, false); }
  );
}

}  // namespace cache

}  // namespace foo
