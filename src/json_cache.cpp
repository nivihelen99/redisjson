#include "redisjson++/json_cache.h"
#include <stdexcept> // For std::out_of_range with unordered_map::at

namespace redisjson {

JSONCache::JSONCache(size_t max_size, std::chrono::seconds default_ttl)
    : _max_size(max_size), _default_ttl(default_ttl), _caching_enabled(true) {
    if (max_size == 0) {
        _caching_enabled = false; // Or throw, depending on desired behavior for 0 max_size
    }
}

void JSONCache::enable_caching(bool enabled) {
    std::lock_guard<std::mutex> lock(_mutex);
    _caching_enabled = enabled;
    if (!_caching_enabled) {
        _cache.clear();
        _lru_list.clear();
    }
}

bool JSONCache::is_caching_enabled() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _caching_enabled;
}

void JSONCache::set_cache_size(size_t max_size) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (max_size == 0) {
        _caching_enabled = false; // Or throw
        _cache.clear();
        _lru_list.clear();
    }
    _max_size = max_size;
    while (_cache.size() > _max_size && _max_size > 0) {
        _evict();
    }
}

void JSONCache::set_ttl(std::chrono::seconds ttl) {
    std::lock_guard<std::mutex> lock(_mutex);
    _default_ttl = ttl;
}

void JSONCache::put(const std::string& key, const json& value, std::chrono::seconds ttl_override) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_caching_enabled || _max_size == 0) {
        return;
    }

    if (_cache.size() >= _max_size) {
        _evict();
    }

    auto it = _cache.find(key);
    if (it != _cache.end()) {
        // Key exists, update it and move to front of LRU
        _lru_list.erase(it->second.lru_iterator);
    }

    _lru_list.push_front(key);

    CacheEntry entry;
    entry.value = value;
    auto effective_ttl = (ttl_override.count() > 0) ? ttl_override : _default_ttl;
    if (effective_ttl.count() > 0) {
        entry.expiry_time = std::chrono::steady_clock::now() + effective_ttl;
    } else {
        // No expiry (or very long expiry)
        entry.expiry_time = std::chrono::time_point<std::chrono::steady_clock>::max();
    }
    entry.lru_iterator = _lru_list.begin();

    _cache[key] = entry;
}

std::optional<json> JSONCache::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_caching_enabled) {
        return std::nullopt;
    }

    auto it = _cache.find(key);
    if (it == _cache.end()) {
        // Cache miss
        return std::nullopt;
    }

    CacheEntry& entry = it->second;

    if (std::chrono::steady_clock::now() > entry.expiry_time) {
        // Cache entry expired
        _lru_list.erase(entry.lru_iterator);
        _cache.erase(it);
        return std::nullopt;
    }

    // Cache hit: move to front of LRU list
    _lru_list.erase(entry.lru_iterator);
    _lru_list.push_front(key);
    entry.lru_iterator = _lru_list.begin();

    return entry.value;
}

void JSONCache::invalidate(const std::string& key) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _cache.find(key);
    if (it != _cache.end()) {
        _lru_list.erase(it->second.lru_iterator);
        _cache.erase(it);
    }
}

void JSONCache::clear_cache() {
    std::lock_guard<std::mutex> lock(_mutex);
    _cache.clear();
    _lru_list.clear();
}

CacheStats JSONCache::get_stats() const {
    std::lock_guard<std::mutex> lock(_mutex);
    CacheStats stats;
    // Actual hits/misses would need to be tracked in get() and put()
    // For this stub, we'll just report size.
    // stats.hits = _hits; // Need to add _hits and _misses counters
    // stats.misses = _misses;
    stats.current_size = _cache.size();
    stats.max_size = _max_size;
    return stats;
}

void JSONCache::_evict() {
    // Assumes _mutex is already held
    if (_lru_list.empty()) {
        return;
    }
    // Evict least recently used item (back of the list)
    const std::string& lru_key = _lru_list.back();
    _cache.erase(lru_key);
    _lru_list.pop_back();
}

} // namespace redisjson
