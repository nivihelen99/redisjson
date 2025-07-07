#pragma once

#include <string>
#include <vector>
#include <memory>
#include <vector>
#include <string>
#include <nlohmann/json.hpp>
#include <optional> // For std::optional

#include <hiredis/hiredis.h> // For redisReply and redisContext

#include "common_types.h"
#include "exceptions.h"
#include "redis_connection_manager.h" // To be replaced
#include "path_parser.h"
#include "json_modifier.h"
#include "lua_script_manager.h" // To be removed or heavily adapted
// #include "transaction_manager.h" // May be removed if SWSS doesn't support easily
#include "json_query_engine.h"   // May be adapted or removed
#include "json_cache.h"          // May be adapted or removed
#include "json_schema_validator.h" // May be adapted or removed
#include "json_event_emitter.h"  // May be adapted or removed

// Placeholder for actual SWSS headers
// Actual path might be different, e.g. <swss/dbconnector.h>
// For now, use a path that might work in a typical SONiC build environment.
#if __has_include(<swss/dbconnector.h>)
#include <swss/dbconnector.h>
#elif __has_include("dbconnector.h") // Local build / test
#include "dbconnector.h"
#else
// Minimal fake DBConnector for compilation if header not found
namespace swss {
class DBConnector {
public:
    DBConnector(const std::string& dbName, unsigned int timeout, bool waitForDb = false, const std::string& unixPath = "") {}
    DBConnector(int dbId, unsigned int timeout, bool waitForDb = false, const std::string& unixPath = "") {}
    DBConnector(const std::string& dbName, const std::string& unixPath, unsigned int timeout) {}

    void set(const std::string& key, const std::string& value, const std::string& op = "SET", const std::string& prefix = "") { (void)key; (void)value; (void)op; (void)prefix; }
    std::string get(const std::string& key, const std::string& prefix = "") { (void)key; (void)prefix; return ""; }
    bool exists(const std::string& key, const std::string& prefix = "") { (void)key; (void)prefix; return false; }
    long long del(const std::string& key, const std::string& prefix = "") { (void)key; (void)prefix; return 0; }
    std::vector<std::string> keys(const std::string& pattern, const std::string& prefix = "") { (void)pattern; (void)prefix; return {}; }
    void flushdb() {} // For testing primarily

    // Mock transaction methods - actual DBConnector might not have these or have different ones
    void multi() {}
    redisReply* exec() { return nullptr; } // Hiredis type, may not be available or used
    void discard() {}
    // Raw command execution - highly dependent on actual DBConnector providing this
    redisReply* command(const char* format, ...) { (void)format; return nullptr; }
    redisReply* commandArgv(int argc, const char** argv, const size_t* argvlen) { (void)argc; (void)argv; (void)argvlen; return nullptr; }

    // If DBConnector wraps hiredis context directly (less likely for typical SWSS usage)
    redisContext* getContext() { return nullptr; }
};
} // namespace swss
#endif


namespace redisjson {

using json = nlohmann::json;

// Forward declarations for components that might be kept/adapted
// class TransactionManager; // Likely removed or re-thought
class JSONQueryEngine;    // May change significantly
class JSONCache;          // May change
class JSONSchemaValidator;// May change
class JSONEventEmitter;   // May change

class RedisJSONClient {
public:
    // Constructor for legacy direct Redis connections (existing config)
    explicit RedisJSONClient(const LegacyClientConfig& client_config);
    // Constructor for SONiC SWSS environment
    explicit RedisJSONClient(const SwssClientConfig& swss_config);

    ~RedisJSONClient();

    // Document Operations
    void set_json(const std::string& key, const json& document,
                  const SetOptions& opts = {});
    json get_json(const std::string& key) const;
    bool exists_json(const std::string& key) const;
    void del_json(const std::string& key);

    /**
     * @brief Merges a JSON object into an existing JSON document at the specified key (non-SWSS mode uses Lua script).
     * If the key does not exist, a new document is created with the content of sparse_json_object.
     * If the key exists and holds a JSON object, the fields from sparse_json_object are merged into it.
     * Existing fields in the document that are not present in sparse_json_object are preserved.
     * Fields in sparse_json_object will overwrite existing fields in the document.
     * This is a shallow merge operation on the top-level keys of sparse_json_object.
     *
     * In SWSS mode, this operation would be client-side (get, merge, set) and lose atomicity.
     * The primary implementation target for this feature is non-SWSS mode using Lua.
     *
     * @param key The Redis key where the JSON document is stored.
     * @param sparse_json_object A nlohmann::json object containing the fields to merge. Must be a JSON object.
     * @throws RedisJSONException if the operation fails (e.g., existing value is not an object, input is not an object, Redis error).
     * @return true if the Lua script reports success (typically 1), false otherwise. Specific error details are through exceptions.
     */
    bool set_json_sparse(const std::string& key, const json& sparse_json_object);

    // Path Operations (will be client-side get-modify-set, atomicity lost)
    json get_path(const std::string& key, const std::string& path) const;
    void set_path(const std::string& key, const std::string& path,
                  const json& value, const SetOptions& opts = {}); // create_path in SetOptions might be used client-side
    void del_path(const std::string& key, const std::string& path);
    bool exists_path(const std::string& key, const std::string& path) const;

    // Array Operations (will be client-side get-modify-set, atomicity lost)
    void append_path(const std::string& key, const std::string& path,
                     const json& value);
    void prepend_path(const std::string& key, const std::string& path,
                      const json& value);
    json pop_path(const std::string& key, const std::string& path,
                  int index = -1);
    size_t array_length(const std::string& key, const std::string& path) const;

    // Merge Operations (JSON.MERGE not standard in Redis, client-side or specific command needed)
    // For SWSS, this will likely be client-side get-merge-set.
    // MergeStrategy might be hard to implement fully client-side without deep knowledge of ReJSON behavior.
    // Let's simplify to a basic client-side merge or require a specific RedisJSON command if available via DBConnector.
    void merge_json(const std::string& key, const json& patch); // Simplified: deep merge by default client-side
    void patch_json(const std::string& key, const json& patch_operations); // RFC 6902 JSON Patch, client-side

    // Atomic Operations (atomicity will be lost with client-side logic)
    // These will become get-then-set or get-compare-then-set
    json non_atomic_get_set(const std::string& key, const std::string& path,
                            const json& new_value);
    bool non_atomic_compare_set(const std::string& key, const std::string& path,
                                const json& expected, const json& new_value);

    // Utility Operations
    std::vector<std::string> keys_by_pattern(const std::string& pattern) const;
    json search_by_value(const std::string& key, const json& search_value) const; // Stays client-side
    std::vector<std::string> get_all_paths(const std::string& key) const; // Stays client-side

    // Access to sub-components (review if these are still relevant/how they adapt)
    // JSONQueryEngine& query_engine();
    // JSONCache& cache();
    // JSONSchemaValidator& schema_validator();
    // JSONEventEmitter& event_emitter();
    // TransactionManager& transaction_manager(); // Likely removed

private:
    // Determine which config is active, or use a variant/union if both modes needed simultaneously (unlikely)
    bool _is_swss_mode = false;
    LegacyClientConfig _legacy_config;
    SwssClientConfig _swss_config;

    std::unique_ptr<swss::DBConnector> _db_connector; // For SWSS mode

    // Keep direct Redis connection management for legacy mode
    std::unique_ptr<RedisConnectionManager> _connection_manager; // For legacy mode
    std::unique_ptr<LuaScriptManager> _lua_script_manager; // For legacy mode, might be conditionally compiled/used

    // Common components (may need adaptation based on mode)
    std::unique_ptr<PathParser> _path_parser;
    std::unique_ptr<JSONModifier> _json_modifier; // Used for client-side modifications

    // Sub-components that might be removed or heavily adapted for SWSS mode
    // std::unique_ptr<TransactionManager> _transaction_manager;
    // std::unique_ptr<JSONQueryEngine> _query_engine;
    // std::unique_ptr<JSONCache> _json_cache;
    // std::unique_ptr<JSONSchemaValidator> _schema_validator;
    // std::unique_ptr<JSONEventEmitter> _event_emitter;

    // Helper to get a connection for legacy mode
    RedisConnectionManager::RedisConnectionPtr get_legacy_redis_connection() const;

    // Helper to parse json from string reply, throws on error
    json _parse_json_reply(const std::string& reply_str, const std::string& context_msg) const;

    // Client-side implementation for path-based modifications
    json _get_document_for_modification(const std::string& key) const;
    void _set_document_after_modification(const std::string& key, const json& document, const SetOptions& opts);

    // Placeholder for direct Redis command execution if DBConnector supports it (for legacy Lua scripts if adapted)
    // RedisReplyPtr _execute_redis_command(const char* format, ...);
    // RedisReplyPtr _execute_redis_command_argv(int argc, const char **argv, const size_t *argvlen);
};

} // namespace redisjson
