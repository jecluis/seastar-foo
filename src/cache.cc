/* Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#include "cache.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

static seastar::logger applog(__FILE__);

namespace foo {

namespace cache {

template <bool IsLocal>
bool cache::put(const seastar::sstring&& key, const seastar::sstring&& value) {
  applog.debug("put key '{}' into cache", key);
  auto it = find(key);
  if (it != _cache.end()) {
    // exists, replace
    applog.debug("key '{}' already exists in cache, remove", key);
    auto& existing = *it;
    remove_item(existing, false);
  }
  cache_item* new_item =
      (IsLocal ? new cache_item(std::move(key), std::move(value), _ttl)
               : new cache_item(
                     std::forward<const seastar::sstring&&>(key),
                     std::forward<const seastar::sstring&&>(value), _ttl
                 ));
  size_t required_size = item_size(*new_item);

  try {
    find_cache_space(required_size);
  } catch (not_enough_space_error) {
    applog.error(
        "not enough space in cache to store additional {} bytes", required_size
    );
    return false;
  }

  _cache.insert(*new_item);
  _lru.push_front(*new_item);
  _exp_timers.insert(*new_item);
  intrusive_ptr_add_ref(new_item);
  // This should not be needed, because the item we're adding will always have
  // a later timeout than whatever is the next timeout, except if this item is
  // the only one in the cache and was thus removed earlier in this function.
  // However, since 'timer_set' does not recalculate the next timeout on
  // removal, we will very likely end up with the same value anyway.
  _timer.rearm(_exp_timers.get_next_timeout());
  _estimated_cache_size += required_size;
  applog.debug(
      "added new entry: key '{}', required size '{}', cache size '{}'", key,
      required_size, _estimated_cache_size
  );
  return true;
}

void cache::remove_item(cache_item& item, bool expired) {
  applog.debug("remove key '{}' from cache", item.key());
  _cache.erase(_cache.iterator_to(item));
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

cache_item_ptr cache::get(const seastar::sstring& key) {
  applog.debug("get key '{}'", key);
  auto it = find(key);
  if (it == _cache.end()) {
    applog.debug("no such key '{}' in cache", key);
    return nullptr;
  }
  it->touch(_ttl);
  _lru.remove(*it);
  _lru.push_front(*it);
  return cache_item_ptr(&*it);
}

}  // namespace cache

}  // namespace foo
