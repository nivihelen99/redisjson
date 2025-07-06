#pragma once

#include <string>
#include <vector>
#include <memory>
#include <vector>
#include <string>
#include <nlohmann/json.hpp>

#include "common_types.h"
#include "exceptions.h"
#include "redis_connection_manager.h"
#include "path_parser.h"
#include "json_modifier.h"
#include "lua_script_manager.h"
#include "transaction_manager.h" // Added
#include "json_query_engine.h"   // Added
#include "json_cache.h"          // Added
#include "json_schema_validator.h" // Added
#include "json_event_emitter.h"  // Added

namespace redisjson {

using json = nlohmann::json;

// Forward declarations
class RedisConnection;
class TransactionManager;
class JSONQueryEngine;
class JSONCache;
class JSONSchemaValidator;
class JSONEventEmitter;

class RedisJSONClient {
public:
    explicit RedisJSONClient(const ClientConfig& client_config);
    ~RedisJSONClient(); // Ensure proper cleanup of unique_ptrs

    // Document Operations
    void set_json(const std::string& key, const json& document,
                  const SetOptions& opts = {});
    json get_json(const std::string& key) const;
    bool exists_json(const std::string& key) const;
    void del_json(const std::string& key);

    // Path Operations
    json get_path(const std::string& key, const std::string& path) const;
    void set_path(const std::string& key, const std::string& path,
                  const json& value, const SetOptions& opts = {});
    void del_path(const std::string& key, const std::string& path);
    bool exists_path(const std::string& key, const std::string& path) const;

    // Array Operations
    void append_path(const std::string& key, const std::string& path,
                     const json& value); // Appends value to array at path
    void prepend_path(const std::string& key, const std::string& path,
                      const json& value); // Prepends value to array at path
    json pop_path(const std::string& key, const std::string& path,
                  int index = -1); // Removes and returns element from array at path
    size_t array_length(const std::string& key, const std::string& path) const;

    // Merge Operations
    void merge_json(const std::string& key, const json& patch,
                    const MergeStrategy& strategy = MergeStrategy::DEEP);
    void patch_json(const std::string& key, const json& patch); // RFC 6902 JSON Patch

    // Batch Operations (Conceptual - requires Operation struct and BatchResult)
    // struct Operation { /* ... */ };
    // struct BatchResult { /* ... */ };
    // BatchResult batch_operations(const std::vector<Operation>& ops);

    // Atomic Operations (using Lua)
    json atomic_get_set(const std::string& key, const std::string& path,
                        const json& new_value);
    bool atomic_compare_set(const std::string& key, const std::string& path,
                           const json& expected, const json& new_value);

    // Utility Operations
    std::vector<std::string> keys_by_pattern(const std::string& pattern) const;
    json search_by_value(const std::string& key, const json& search_value) const; // May use Lua or client-side logic
    std::vector<std::string> get_all_paths(const std::string& key) const; // Get all paths in a document

    // Access to sub-components (optional, depending on design preference)
    // Consider if these should be public or if client should mediate all interactions
    JSONQueryEngine& query_engine();
    JSONCache& cache();
    JSONSchemaValidator& schema_validator();
    JSONEventEmitter& event_emitter();
    TransactionManager& transaction_manager(); // Added for transaction access

    // Helper to get a connection (can be private or protected if only used internally)
    RedisConnectionManager::RedisConnectionPtr get_redis_connection() const;

private:
    ClientConfig _config;
    std::unique_ptr<RedisConnectionManager> _connection_manager;
    std::unique_ptr<PathParser> _path_parser;
    std::unique_ptr<JSONModifier> _json_modifier;
    std::unique_ptr<LuaScriptManager> _lua_script_manager;
    std::unique_ptr<TransactionManager> _transaction_manager; // Added
    std::unique_ptr<JSONQueryEngine> _query_engine;         // Added
    std::unique_ptr<JSONCache> _json_cache;                 // Added
    std::unique_ptr<JSONSchemaValidator> _schema_validator; // Added
    std::unique_ptr<JSONEventEmitter> _event_emitter;       // Added

    // Helper for operations that modify data and might need to invalidate cache / emit events
    void _perform_write_operation(const std::string& key, const std::optional<std::string>& path, std::function<void(RedisConnection&)> operation);

    // Helper to fetch a document, potentially using cache
    json _get_document_with_caching(const std::string& key) const;

    // Helper to parse json from string reply, throws on error
    json _parse_json_reply(const std::string& reply_str, const std::string& context_msg) const;
};

} // namespace redisjson
