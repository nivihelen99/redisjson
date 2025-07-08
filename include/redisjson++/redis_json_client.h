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

    // Retrieves the keys of a JSON object at a specified path.
    // Returns a vector of strings representing the object keys.
    // If the path does not point to an object, or if the key/path does not exist,
    // it returns an empty vector. Throws LuaScriptException on script errors.
    // Path defaults to root '$' if not specified. Only applicable in non-SWSS mode.
    std::vector<std::string> object_keys(const std::string& key, const std::string& path = "$");

    // Retrieves the number of keys in a JSON object at a specified path.
    // Returns std::nullopt if the key or path does not exist, or if the target is not an object.
    // Throws LuaScriptException on script errors in non-SWSS mode.
    // Path defaults to root '$' if not specified.
    std::optional<size_t> object_length(const std::string& key, const std::string& path = "$");

    /**
     * @brief Clears containers (arrays become empty, objects' numbers become 0 and contained arrays are emptied)
     *        or "touches" scalars at a given path. Corresponds to JSON.CLEAR.
     *
     * @param key The key of the JSON document.
     * @param path The JSONPath to specify the element to clear. Defaults to root "$".
     * @return The number of values that were cleared or set to 0.
     *         Returns 1 if path points to a scalar (scalar value itself is not changed).
     *         Returns 0 if path does not exist.
     * @throw PathNotFoundException if the key does not exist and path is not root.
     * @throw InvalidPathException if the path syntax is invalid.
     * @throw LuaScriptException if the Lua script execution fails.
     * @throw ConnectionException if there's a problem with the Redis connection.
     * @throw JsonParsingException if the result from script cannot be parsed.
     */
    long long json_clear(const std::string& key, const std::string& path = "$");

    /**
     * @brief Searches for the first occurrence of a scalar JSON value within an array.
     * JSON.ARRINDEX key path json-value [start-index [stop-index]]
     * @param key The key of the JSON document.
     * @param path The JSONPath to the array.
     * @param value_to_find The JSON scalar value to search for.
     * @param start_index Optional 0-based index to start searching from (inclusive). Defaults to beginning of array. Negative values are calculated from the end.
     * @param end_index Optional 0-based index to stop searching at (inclusive). Defaults to end of array. Negative values are calculated from the end.
     * @return The 0-based index of the first occurrence of the value in the array, or -1 if not found.
     * @throws PathNotFoundException if the key or path does not exist.
     * @throws TypeMismatchException if the target at path is not an array or value_to_find is not a scalar.
     * @throws LuaScriptException for errors during script execution.
     * @throws ConnectionException on connection issues.
     */
    long long arrindex(const std::string& key,
                       const std::string& path,
                       const json& value_to_find,
                       std::optional<long long> start_index = std::nullopt,
                       std::optional<long long> end_index = std::nullopt);

    // Path Operations (will be client-side get-modify-set, atomicity lost for SWSS)
    json get_path(const std::string& key, const std::string& path) const;
    void set_path(const std::string& key, const std::string& path,
                  const json& value, const SetOptions& opts = {});
    void del_path(const std::string& key, const std::string& path);
    bool exists_path(const std::string& key, const std::string& path) const;

    // Numeric Operations
    /**
     * @brief Increments (or decrements) the numeric value stored at path by a specified amount.
     * This operation is atomic when using Lua scripts in non-SWSS mode.
     * In SWSS mode, this operation is non-atomic (get-modify-set).
     * @param key The Redis key.
     * @param path The JSON path to the numeric value.
     * @param value The amount to increment by (can be negative for decrement).
     * @return The new numeric value after the increment (as a nlohmann::json object).
     * @throws RedisJSONException if the key does not exist, path does not exist,
     *         value at path is not a number, or other script/command errors.
     */
    json json_numincrby(const std::string& key, const std::string& path, double value);

    // Array Operations (will be client-side get-modify-set, atomicity lost for SWSS)
    void append_path(const std::string& key, const std::string& path,
                     const json& value);
    void prepend_path(const std::string& key, const std::string& path,
                      const json& value);
    json pop_path(const std::string& key, const std::string& path,
                  int index = -1);
    size_t array_length(const std::string& key, const std::string& path) const;
    long long arrinsert(const std::string& key, const std::string& path, int index, const std::vector<json>& values);


    // Merge Operations (client-side or specific command needed)
    void merge_json(const std::string& key, const json& patch); // Simplified: deep merge by default client-side
    void patch_json(const std::string& key, const json& patch_operations); // RFC 6902 JSON Patch, client-side

    // Atomic Operations (atomicity will be lost with client-side logic for SWSS)
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

    // Helper to check if in legacy mode with Lua support
    void throwIfNotLegacyWithLua(const std::string& operation_name) const;

    // Placeholder for direct Redis command execution if DBConnector supports it (for legacy Lua scripts if adapted)
    // RedisReplyPtr _execute_redis_command(const char* format, ...);
    // RedisReplyPtr _execute_redis_command_argv(int argc, const char **argv, const size_t *argvlen);
};

} // namespace redisjson
