#pragma once

#include "common_types.h"
#include <string>
#include <chrono>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <list>
#include <mutex>

namespace redisjson {

using json = nlohmann::json;

struct CacheStats {
    size_t hits = 0;
    size_t misses = 0;
    size_t current_size = 0;
    size_t max_size = 0;
};

class JSONCache {
public:
    explicit JSONCache(size_t max_size = 1000, std::chrono::seconds default_ttl = std::chrono::seconds(300));

    void enable_caching(bool enabled);
    bool is_caching_enabled() const;

    void set_cache_size(size_t max_size);
    void set_ttl(std::chrono::seconds ttl);

    void put(const std::string& key, const json& value, std::chrono::seconds ttl = std::chrono::seconds(0));
    std::optional<json> get(const std::string& key);

    void invalidate(const std::string& key);
    void clear_cache();

    CacheStats get_stats() const;

private:
    struct CacheEntry {
        json value;
        std::chrono::time_point<std::chrono::steady_clock> expiry_time;
        std::list<std::string>::iterator lru_iterator;
    };

    void _evict();

    std::unordered_map<std::string, CacheEntry> _cache;
    std::list<std::string> _lru_list; // For LRU eviction
    mutable std::mutex _mutex;

    size_t _max_size;
    std::chrono::seconds _default_ttl;
    bool _caching_enabled = true;
};

} // namespace redisjson
