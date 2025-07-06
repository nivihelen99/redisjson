#include "redisjson++/redis_json_client.h"
#include "redisjson++/hiredis_RAII.h" // For RedisReplyPtr
#include "redisjson++/exceptions.h" // Added to include exception definitions
#include <stdexcept>
#include <string>    // Required for std::to_string with some compilers/setups
#include <iostream>  // For std::cout (logging)
#include <thread>    // For std::this_thread::get_id (logging)
#include <cstring>   // For strcmp in set_json

// For PathParser, JSONModifier, LuaScriptManager - include their headers once they exist
// For now, we might not be able to fully initialize them if their constructors are complex.

namespace redisjson {

RedisJSONClient::RedisJSONClient(const ClientConfig& client_config)
    : _config(client_config) {
    _connection_manager = std::make_unique<RedisConnectionManager>(_config);

    // Initialize other managers - for now, assume default constructors or simple ones
    // Once these classes are defined, their proper initialization will occur here.
    _path_parser = std::make_unique<PathParser>();
    _json_modifier = std::make_unique<JSONModifier>();
    // LuaScriptManager constructor expects RedisConnectionManager*
    // std::cout << "LOG: RedisJSONClient::RedisJSONClient() - Initializing LuaScriptManager. Thread ID: " << std::this_thread::get_id() << std::endl;
    _lua_script_manager = std::make_unique<LuaScriptManager>(_connection_manager.get());
    if (_lua_script_manager) {
        // std::cout << "LOG: RedisJSONClient::RedisJSONClient() - Calling LuaScriptManager::preload_builtin_scripts(). Thread ID: " << std::this_thread::get_id() << std::endl;
        // The try-catch here is less critical now since preload_builtin_scripts itself catches and logs.
        // However, keeping it doesn't hurt, in case preload_builtin_scripts itself throws for some unforeseen reason
        // (though it's designed not to).
        try {
            _lua_script_manager->preload_builtin_scripts();
            // std::cout << "LOG: RedisJSONClient::RedisJSONClient() - LuaScriptManager::preload_builtin_scripts() completed. Thread ID: " << std::this_thread::get_id() << std::endl;
        } catch (const RedisJSONException& e) {
            // This path should ideally not be hit if preload_builtin_scripts handles its own errors.
            // std::cout << "ERROR_LOG: RedisJSONClient::RedisJSONClient() - Error during preload_builtin_scripts (exception caught in client constructor): " << e.what() << ". Thread ID: " << std::this_thread::get_id() << std::endl;
            throw RedisJSONException("Failed to preload Lua scripts during RedisJSONClient construction: " + std::string(e.what()));
        }
    } else {
        // std::cout << "ERROR_LOG: RedisJSONClient::RedisJSONClient() - Failed to create LuaScriptManager. Thread ID: " << std::this_thread::get_id() << std::endl;
        // Depending on policy, might throw if LuaScriptManager is critical
    }

    // std::cout << "LOG: RedisJSONClient::RedisJSONClient() - Initializing other components (QueryEngine, Cache, etc.). Thread ID: " << std::this_thread::get_id() << std::endl;
    // Query engine and other components that might depend on Lua scripts or other services
    // should be initialized after their dependencies.
    _query_engine = std::make_unique<JSONQueryEngine>(*this); // If it depends on RedisJSONClient itself
    _json_cache = std::make_unique<JSONCache>(); // Example: Default constructor
    _schema_validator = std::make_unique<JSONSchemaValidator>(); // Example: Default constructor
    _event_emitter = std::make_unique<JSONEventEmitter>(); // Example: Default constructor
    _transaction_manager = std::make_unique<TransactionManager>(_connection_manager.get(), _path_parser.get(),_json_modifier.get());
}

RedisJSONClient::~RedisJSONClient() {
    // Resources managed by unique_ptr will be cleaned up automatically.
    // If LuaScriptManager or others have explicit shutdown needs, call them here.
}

RedisConnectionManager::RedisConnectionPtr RedisJSONClient::get_redis_connection() const {
    try {
        return _connection_manager->get_connection();
    } catch (const ConnectionException& e) {
        // Log error or rethrow as a more specific client error if desired
        throw; // Rethrow for now
    }
}

// --- Document Operations ---

void RedisJSONClient::set_json(const std::string& key, const json& document, const SetOptions& opts) {
    RedisConnectionManager::RedisConnectionPtr conn = get_redis_connection();
    std::string doc_str = document.dump(); // Serialize JSON to string

    RedisReplyPtr reply;

    if (opts.ttl.count() > 0) {
        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command("SET %s %s EX %lld", key.c_str(), doc_str.c_str(), (long long)opts.ttl.count())
        ));
    } else {
        reply = RedisReplyPtr(static_cast<redisReply*>(
            conn->command("SET %s %s", key.c_str(), doc_str.c_str())
        ));
    }

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn)); // Return connection before throwing
        throw RedisCommandException("SET", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Reply object exists but str is null") : "No reply or connection error"));
    }
    // Check reply status for SET, should be "OK"
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") != 0) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("SET", "Key: " + key + ", SET command did not return OK: " + std::string(reply->str));
    }

    _connection_manager->return_connection(std::move(conn));
}

json RedisJSONClient::get_json(const std::string& key) const {
    RedisConnectionManager::RedisConnectionPtr conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("GET %s", key.c_str())
    ));

    if (!reply) { // Covers null reply from command() itself (e.g. connection broken before command sent)
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("GET", "Key: " + key + ", Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("GET", "Key: " + key + ", Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    if (reply->type == REDIS_REPLY_NIL) {
        _connection_manager->return_connection(std::move(conn));
        // Using PathNotFoundException as KeyNotFoundException is not standard in exceptions.h
        throw PathNotFoundException(key, "$ (root)");
    }

    if (reply->type == REDIS_REPLY_STRING) {
        std::string doc_str = reply->str;
        _connection_manager->return_connection(std::move(conn));
        try {
            return json::parse(doc_str);
        } catch (const json::parse_error& e) {
            // Adapt to existing JsonParsingException constructor
            throw JsonParsingException("Failed to parse JSON string for key '" + key + "': " + e.what() + ". Received: " + doc_str);
        }
    }

    // Should not reach here with a valid Redis GET reply structure
    _connection_manager->return_connection(std::move(conn));
    // This path indicates a logic error or unexpected server behavior.
    // Return a default-constructed json or throw a more specific internal error.
    // For now, let's throw, consistent with other error handling.
    throw RedisCommandException("GET", "Key: " + key + ", Error: Unexpected reply type from Redis: " + std::to_string(reply ? reply->type : -1));
}

bool RedisJSONClient::exists_json(const std::string& key) const {
    RedisConnectionManager::RedisConnectionPtr conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("EXISTS %s", key.c_str())
    ));

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("EXISTS", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Reply object exists but str is null") : "No reply or connection error"));
    }

    _connection_manager->return_connection(std::move(conn));
    return (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
}

void RedisJSONClient::del_json(const std::string& key) {
    RedisConnectionManager::RedisConnectionPtr conn = get_redis_connection();

    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("DEL %s", key.c_str())
    ));

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("DEL", "Key: " + key + ", Error: " + (reply ? (reply->str ? reply->str : "Reply object exists but str is null") : "No reply or connection error"));
    }
    // DEL returns number of keys deleted. We don't strictly need to check it for success
    // unless we want to confirm it was > 0 for an "effective" delete.
    // For now, no error from Redis is success.
    _connection_manager->return_connection(std::move(conn));
}

// --- Path Operations ---

json RedisJSONClient::get_path(const std::string& key, const std::string& path) const {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for get_path.");
    }
    // TODO: Use _path_parser->parse(path) and pass structured path to Lua if script supports it.
    // For now, pass path string directly.
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path};

    try {
        json result = _lua_script_manager->execute_script("json_path_get", keys, args);
        // Lua script for get_path should return the JSON value at path, or null if not found.
        // If path not found, LuaScriptManager::redis_reply_to_json converts Redis NIL to json(nullptr)
        if (result.is_null()) {
            throw PathNotFoundException(key, path);
        }
        return result;
    } catch (const LuaScriptException& e) {
        // Check if the Lua error message indicates "path not found" or similar if script signals this way
        // For now, assume any LuaScriptException for get means a problem beyond simple not found.
        // Or, specific error codes from Lua could be translated.
        throw RedisCommandException("LUA_json_path_get", "Key: " + key + ", Path: " + path + ", Error: " + e.what());
    } catch (const PathNotFoundException& e) {
        throw; // Rethrow if script explicitly signals PathNotFound via a specific reply handled by execute_script or redis_reply_to_json
    } catch (const RedisJSONException& e) {
        throw; // Rethrow other RedisJSON specific errors
    }
}

void RedisJSONClient::set_path(const std::string& key, const std::string& path,
                               const json& value, const SetOptions& opts) {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for set_path.");
    }
    std::string value_str = value.dump();
    std::string condition_str;
    switch (opts.condition) {
        case SetCmdCondition::NX: condition_str = "NX"; break;
        case SetCmdCondition::XX: condition_str = "XX"; break;
        case SetCmdCondition::NONE: condition_str = "NONE"; break; // Or "" if Lua script expects that for no condition
    }

    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {
        path,
        value_str,
        condition_str,
        std::to_string(opts.ttl.count()),
        opts.create_path ? "true" : "false" // Added create_path option
    };

    try {
        json result = _lua_script_manager->execute_script("json_path_set", keys, args);
        // Lua script for set_path should return a status, e.g., 1 for OK, 0 for condition not met.
        // If result is json(nullptr) or a specific value indicating condition not met (e.g. 0)
        if (result.is_boolean() && !result.get<bool>()) { // Assuming script returns true on success, false on NX/XX fail
             // Condition (NX/XX) not met, or other failure indicated by 'false'
             // This part needs careful coordination with Lua script's return value.
             // For now, if it's explicitly false, assume condition not met and do not throw.
             return;
        }
        if (result.is_null()) { // Alternative: script returns null if condition not met
            return; // Condition not met
        }
        // If an error occurred within Lua that wasn't a typical Redis error (caught as LuaScriptException),
        // it might come as a non-true/non-null value. Or script throws error string.
        // The LuaScriptManager::execute_script and redis_reply_to_json should ideally parse known success/failure indicators.

    } catch (const LuaScriptException& e) {
        // Specific Lua errors might indicate "key does not exist" for XX, or "path exists" for NX if script handles this.
        // For now, generic error.
        throw RedisCommandException("LUA_json_path_set", "Key: " + key + ", Path: " + path + ", Error: " + e.what());
    } catch (const RedisJSONException& e) {
        throw;
    }
}

void RedisJSONClient::del_path(const std::string& key, const std::string& path) {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for del_path.");
    }
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path};

    try {
        json result = _lua_script_manager->execute_script("json_path_del", keys, args);
        // Lua script should return number of paths deleted (e.g., 0 or 1).
        // If result is 0, it means path was not found or key did not exist, which is not an error for DEL.
        // If LuaScriptManager returns integer as json number:
        if (result.is_number_integer()) {
            // int deleted_count = result.get<int>();
            // No specific action based on count for void return type.
            return;
        }
        // If script indicates error in other ways, it should be caught by LuaScriptException.
    } catch (const LuaScriptException& e) {
        throw RedisCommandException("LUA_json_path_del", "Key: " + key + ", Path: " + path + ", Error: " + e.what());
    } catch (const RedisJSONException& e) {
        throw;
    }
}

bool RedisJSONClient::exists_path(const std::string& key, const std::string& path) const {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for exists_path.");
    }
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path};

    try {
        json result = _lua_script_manager->execute_script("json_path_type", keys, args);
        // Lua script for type/exists should return the type string if exists, or null/nil if not.
        // If result is json(nullptr), path does not exist.
        // Otherwise, if it's a non-null value (typically a string representing the type), it exists.
        return !result.is_null();
    } catch (const LuaScriptException& e) {
        // If script throws error for "key not found" before even checking path, that's a LuaScriptException.
        // For exists_path, this should probably return false, not throw.
        // This requires the Lua script to handle "key not found" gracefully and return nil/null.
        // If any other Lua error, then it's a proper exception.
        // For now, rethrow, but this might need refinement based on Lua script behavior.
        // A robust Lua script for "type" would return nil if key doesn't exist, or path doesn't exist.
        // In such cases, LuaScriptManager would give json(nullptr), and `!result.is_null()` would be false.
        // So, a LuaScriptException here implies a more severe script error.
        throw RedisCommandException("LUA_json_path_type", "Key: " + key + ", Path: " + path + ", Error: " + e.what());
    } catch (const RedisJSONException& e) {
        throw;
    }
}

// --- Array Operations ---

void RedisJSONClient::append_path(const std::string& key, const std::string& path,
                                  const json& value) {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for append_path.");
    }
    std::string value_str = value.dump();
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path, value_str};

    try {
        json result = _lua_script_manager->execute_script("json_array_append", keys, args);
        // Lua script should return the new array length or confirm success.
        // If it returns an integer (new length), we don't use it in void function.
        // If it throws an error (e.g. path not an array), it's caught as LuaScriptException.
        if (result.is_number_integer() || result.is_boolean() && result.get<bool>()) { // Assuming script returns new length or true
            return;
        }
         // Handle cases where script might return specific error codes as non-exception results
        if (result.is_string() && result.get<std::string>() == "ERR_NOT_ARRAY") {
            throw TypeMismatchException(key, path, "Path is not an array for append operation.");
        }
        if (result.is_null()){ // Could mean key or path not found
            throw PathNotFoundException(key,path);
        }
        // Fallback for unexpected success indication
        // throw RedisCommandException("LUA_json_array_append", "Key: " + key + ", Path: " + path + ", Unexpected success result: " + result.dump());


    } catch (const LuaScriptException& e) {
        throw RedisCommandException("LUA_json_array_append", "Key: " + key + ", Path: " + path + ", Error: " + e.what());
    } catch (const RedisJSONException& e) {
        throw;
    }
}

void RedisJSONClient::prepend_path(const std::string& key, const std::string& path,
                                   const json& value) {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for prepend_path.");
    }
    std::string value_str = value.dump();
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path, value_str};

    try {
        json result = _lua_script_manager->execute_script("json_array_prepend", keys, args);
        // Similar to append, expect success confirmation or new length.
        if (result.is_number_integer() || result.is_boolean() && result.get<bool>()) {
            return;
        }
        if (result.is_string() && result.get<std::string>() == "ERR_NOT_ARRAY") {
            throw TypeMismatchException(key, path, "Path is not an array for prepend operation.");
        }
         if (result.is_null()){
            throw PathNotFoundException(key,path);
        }
        // throw RedisCommandException("LUA_json_array_prepend", "Key: " + key + ", Path: " + path + ", Unexpected success result: " + result.dump());

    } catch (const LuaScriptException& e) {
        throw RedisCommandException("LUA_json_array_prepend", "Key: " + key + ", Path: " + path + ", Error: " + e.what());
    } catch (const RedisJSONException& e) {
        throw;
    }
}

json RedisJSONClient::pop_path(const std::string& key, const std::string& path,
                               int index) {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for pop_path.");
    }
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path, std::to_string(index)};

    try {
        json result = _lua_script_manager->execute_script("json_array_pop", keys, args);
        // Lua script should return the popped value (as JSON string, parsed by LuaScriptManager)
        // or null/nil if path not found, not an array, or index out of bounds.
        if (result.is_null()) {
             // Distinguish between "path not found/not array" and "index out of bounds but array exists"
             // For now, PathNotFoundException covers these broadly. A more specific error could be used.
            throw PathNotFoundException(key, path); // Or specific "PopFailedException"
        }
        return result;
    } catch (const LuaScriptException& e) {
         // Lua script might throw specific errors for "out of bounds" vs "not an array"
        throw RedisCommandException("LUA_json_array_pop", "Key: " + key + ", Path: " + path + ", Index: " + std::to_string(index) + ", Error: " + e.what());
    } catch (const RedisJSONException& e) {
        throw;
    }
}

size_t RedisJSONClient::array_length(const std::string& key, const std::string& path) const {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized for array_length.");
    }
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path};

    try {
        json result = _lua_script_manager->execute_script("json_array_length", keys, args);
        // Lua script should return the array length as an integer, or null/nil if path not found/not array.
        if (result.is_number_integer()) {
            long long length = result.get<long long>();
            if (length < 0) { // Should not happen for a valid length
                throw RedisCommandException("LUA_json_array_length", "Key: " + key + ", Path: " + path + ", Received negative array length.");
            }
            return static_cast<size_t>(length);
        }
        if (result.is_null()) { // Path not found or not an array
            throw PathNotFoundException(key, path); // Or TypeMismatchException if path exists but not array
        }
        // Fallback for unexpected result type
        throw RedisCommandException("LUA_json_array_length", "Key: " + key + ", Path: " + path + ", Unexpected result type: " + result.dump());
    } catch (const LuaScriptException& e) {
        throw RedisCommandException("LUA_json_array_length", "Key: " + key + ", Path: " + path + ", Error: " + e.what());
    } catch (const RedisJSONException& e) {
        throw;
    }
}

// --- Merge Operations ---

void RedisJSONClient::merge_json(const std::string& key, const json& patch,
                                 const MergeStrategy& strategy) {
    // NOTE: The `MergeStrategy` enum (DEEP, SHALLOW, etc.) is not directly supported by
    // the standard Redis `JSON.MERGE` command. `JSON.MERGE` always performs a deep merge
    // of objects. If different strategies are strictly required, this would necessitate
    // client-side logic (fetch, merge with custom logic, then set) or a complex Lua script.
    // For this implementation, we will use `JSON.MERGE` with its default behavior,
    // effectively ignoring the `strategy` parameter beyond potentially logging a warning
    // if it's not the default DEEP (which aligns with JSON.MERGE).

    if (strategy != MergeStrategy::DEEP) {
        // Optionally log a warning here:
        // std::cerr << "Warning: RedisJSONClient::merge_json called with strategy other than DEEP. "
        //           << "Redis JSON.MERGE performs a deep merge by default. Strategy parameter is currently ignored." << std::endl;
        // Depending on strictness, one might even throw an std::invalid_argument if only DEEP is supported.
    }

    RedisConnectionManager::RedisConnectionPtr conn = get_redis_connection();
    std::string patch_str = patch.dump();
    
    // JSON.MERGE <key> <path> <value>
    // We assume merging into the root of the document, so path is '$'.
    // The `patch` here is the JSON value to be merged in.
    RedisReplyPtr reply(static_cast<redisReply*>(
        conn->command("JSON.MERGE %s $ %s", key.c_str(), patch_str.c_str())
    ));

    if (!reply) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: No reply or connection error");
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        _connection_manager->return_connection(std::move(conn));
        throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: " + (reply->str ? reply->str : "Redis error reply with no message"));
    }

    // JSON.MERGE returns "OK" on success.
    // It returns NIL if the key does not exist and the path is not '$' (older versions).
    // For path '$', if the key does not exist, JSON.MERGE will create it and set it to the patch value if patch is an object/array.
    // If the existing value or patch is not a JSON object, it might error or have specific behavior.
    
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
        _connection_manager->return_connection(std::move(conn));
        return; // Success
    }
    
    // JSON.MERGE (on ReJSON < 2.0) could return an integer reply (0 or 1) indicating if a change was made.
    // ReJSON 2.0+ returns "OK". We will primarily check for "OK".
    // If older versions are supported, this might need adjustment.
    // For now, strict check for "OK".

    _connection_manager->return_connection(std::move(conn));
    throw RedisCommandException("JSON.MERGE", "Key: " + key + ", Path: $, Error: Unexpected reply: " + (reply->str ? reply->str : "Status not OK or unknown reply type ") + std::to_string(reply->type));
}

void RedisJSONClient::patch_json(const std::string& key, const json& patch_operations) {
    // This is a non-atomic client-side implementation of JSON Patch (RFC 6902).
    // For atomicity, a Lua script would be required, which would need a Lua JSON library
    // and a JSON Patch library in Lua, or a very complex script.
    // Alternatively, if RedisJSON supported JSON.PATCH command directly, that would be used.

    // 1. Get the current JSON document
    json current_doc;
    try {
        current_doc = get_json(key); // Uses the class's existing get_json method
    } catch (const PathNotFoundException& e) {
        // If the key does not exist, a patch operation might still be valid if it's an "add" to root.
        // However, nlohmann::json::patch expects the document to exist.
        // RFC6902 says: "If the target document is an empty JSON document (e.g., ""),
        // the "add" operation can be used to add a data structure to the document."
        // nlohmann json `patch` method expects a non-null document to patch.
        // If key doesn't exist, we can treat it as an empty object `json::object()` or based on patch.
        // For simplicity now, if key does not exist, we'll let it fail if patch expects content,
        // or let it proceed if patch creates everything (e.g. add to root of a conceptual empty doc).
        // A common approach for "add" to a non-existent root would be to initialize current_doc to json::object() or json::value_t::null.
        // Let's assume for now `get_json` throws if key not found, and we re-throw or handle.
        // If the patch is meant to create the document (e.g. add / an empty object),
        // then `get_json` failing is problematic.
        // Let's try to patch a null JSON object if the key is not found, as some patch operations
        // (like adding to the root) might still be valid.
        current_doc = json::object(); // Start with an empty object if key not found.
                                      // This might not be correct for all patch operations.
                                      // A specific "add" to "/" might work.
                                      // Consider if PathNotFoundException should be re-thrown.
                                      // For now, let's assume "patching a non-existent doc" means starting from `{}`.
                                      // This behavior might need refinement based on desired semantics.
                                      // A safer default if key not found might be to throw, unless patch is a single "add" to "/"
    } catch (const RedisJSONException& e) { // Catch other Redis specific exceptions from get_json
        throw; // Rethrow other potentially critical errors from get_json
    }


    // 2. Apply the patch operations (RFC 6902)
    json patched_doc;
    try {
        patched_doc = current_doc.patch(patch_operations);
    } catch (const json::exception& e) {
        // This could be json::parse_error if patch_operations is malformed,
        // or json::out_of_range / json::type_error if patch operations are invalid for the current_doc.
        throw PatchFailedException("Failed to apply JSON Patch for key '" + key + "': " + e.what());
    }

    // 3. Set the patched document back to Redis
    try {
        // Use the class's existing set_json method. This doesn't provide options like NX/XX from here.
        // If TTL was associated with the key, it will be cleared by set_json unless set_json is modified
        // or we fetch TTL and re-apply. For simplicity, this basic patch doesn't manage TTL explicitly.
        set_json(key, patched_doc);
     } catch (const RedisJSONException& e) {
        // Catch Redis specific exceptions from set_json
        throw; // Rethrow
    }
}

// --- Atomic Operations (using Lua) ---

json RedisJSONClient::atomic_get_set(const std::string& key, const std::string& path,
                                     const json& new_value) {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized.");
    }

    // Script name expected to be loaded by LuaScriptManager
    const std::string script_name = "atomic_json_get_set_path";

    // Ensure script is loaded (optional check, execute_script might throw if not found)
    // if (!_lua_script_manager->is_script_loaded(script_name)) {
    //    throw RedisConfigurationException("Lua script '" + script_name + "' is not loaded.");
    // }
    // Alternatively, LuaScriptManager::preload_builtin_scripts() should be called during client construction.

    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path, new_value.dump()};

    try {
        // _execute_lua_script is a private helper that should call the public LuaScriptManager method.
        // Let's call the manager directly as _execute_lua_script itself is also a TODO/stub.
        // json result = _execute_lua_script(script_name, keys, args);

        json result = _lua_script_manager->execute_script(script_name, keys, args);

        // The Lua script for RedisJSON's JSON.GET returns a string which is a JSON value,
        // or nil if path not found. LuaScriptManager::execute_script and its helper
        // redis_reply_to_json should handle parsing this string back to nlohmann::json.
        // If current_value_str was nil from redis.call, redis_reply_to_json should turn it into json(nullptr).
        return result;

    } catch (const LuaScriptException& e) {
        // More specific error context
        throw LuaScriptException(script_name, "Error executing atomic_get_set script for key '" + key + "', path '" + path + "': " + e.what());
    } catch (const JsonParsingException& e) {
        throw LuaScriptException(script_name, "Failed to parse result from atomic_get_set script for key '" + key + "', path '" + path + "': " + e.what());
    } catch (const RedisJSONException& e) { // Catch other Redis specific exceptions
        throw; // Rethrow
    }
    // Should be unreachable if all exceptions from execute_script are RedisJSONExceptions
    // or derived from it, or LuaScriptException. Adding throw to satisfy compiler.
    throw RedisJSONException("Unknown error in atomic_get_set after Lua script execution attempt.");
}

bool RedisJSONClient::atomic_compare_set(const std::string& key, const std::string& path,
                                        const json& expected, const json& new_value) {
    if (!_lua_script_manager) {
        throw RedisJSONException("LuaScriptManager is not initialized.");
    }

    const std::string script_name = "atomic_json_compare_set_path";
    // LuaScriptManager::preload_builtin_scripts() should load this.

    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {path, expected.dump(), new_value.dump()};

    try {
        json result_json = _lua_script_manager->execute_script(script_name, keys, args);

        // The Lua script returns 1 for success (set was performed) or 0 for failure (no match).
        // LuaScriptManager::redis_reply_to_json should convert Redis integer reply to nlohmann::json number.
        if (result_json.is_number_integer()) {
            return result_json.get<int>() == 1;
        } else {
            // Should not happen if script behaves correctly.
            throw LuaScriptException(script_name, "Atomic compare-and-set script '" + script_name + "' for key '" + key + "', path '" + path + "' returned non-integer result: " + result_json.dump());
        }
    } catch (const LuaScriptException& e) {
        throw LuaScriptException(script_name, "Error executing atomic_compare_set script for key '" + key + "', path '" + path + "': " + e.what());
    } catch (const RedisJSONException& e) { // Catch other Redis specific exceptions
        throw; // Rethrow
    }
    // Should be unreachable if all exceptions from execute_script are RedisJSONExceptions
    // or derived from it, or LuaScriptException. Adding throw to satisfy compiler.
    throw RedisJSONException("Unknown error in atomic_compare_set after Lua script execution attempt.");
}

// --- Utility Operations ---

std::vector<std::string> RedisJSONClient::keys_by_pattern(const std::string& pattern) const {
    std::vector<std::string> found_keys;
    std::string cursor = "0";
    RedisConnectionManager::RedisConnectionPtr conn; // Declare here for potential reuse across SCAN loops

    do {
        conn = get_redis_connection(); // Get a fresh connection for each SCAN command or reuse carefully.
                                       // For simplicity of connection management, getting a new one is safer.

        RedisReplyPtr reply(static_cast<redisReply*>(
            // Using a COUNT of 100, adjust as needed for performance/memory.
            conn->command("SCAN %s MATCH %s COUNT 100", cursor.c_str(), pattern.c_str())
        ));

        if (!reply) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Cursor: " + cursor + ", Error: No reply or connection error");
        }

        if (reply->type == REDIS_REPLY_ERROR) {
            std::string err_msg = (reply->str ? reply->str : "Redis error reply with no message");
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Cursor: " + cursor + ", Error: " + err_msg);
        }

        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Cursor: " + cursor + ", Error: Unexpected reply structure from SCAN.");
        }

        // First element is the new cursor
        redisReply* cursor_reply = reply->element[0];
        if (cursor_reply->type != REDIS_REPLY_STRING) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Error: New cursor is not a string.");
        }
        cursor = std::string(cursor_reply->str, cursor_reply->len);

        // Second element is an array of keys
        redisReply* keys_reply = reply->element[1];
        if (keys_reply->type != REDIS_REPLY_ARRAY) {
            if (conn) _connection_manager->return_connection(std::move(conn));
            throw RedisCommandException("SCAN", "Pattern: " + pattern + ", Error: Keys element is not an array.");
        }

        for (size_t i = 0; i < keys_reply->elements; ++i) {
            redisReply* key_element_reply = keys_reply->element[i];
            if (key_element_reply->type == REDIS_REPLY_STRING) {
                found_keys.emplace_back(key_element_reply->str, key_element_reply->len);
            }
            // Non-string elements in keys array would be strange, ignore or log.
        }
        
        _connection_manager->return_connection(std::move(conn)); // Return connection after processing reply

    } while (cursor != "0");

    return found_keys;
}

// Helper recursive function for search_by_value
void find_values_recursive(const json& current_json_doc, const json& search_value, json::array_t& found_values) {
    if (current_json_doc == search_value) {
        found_values.push_back(current_json_doc);
    }

    if (current_json_doc.is_object()) {
        for (auto& el : current_json_doc.items()) {
            find_values_recursive(el.value(), search_value, found_values);
        }
    } else if (current_json_doc.is_array()) {
        for (const auto& item : current_json_doc) {
            find_values_recursive(item, search_value, found_values);
        }
    }
    // Primitive types are handled by direct comparison at the start of the function.
}

json RedisJSONClient::search_by_value(const std::string& key, const json& search_value) const {
    // Client-side search. Fetches the whole document.
    // This can be inefficient for large documents.
    // RedisSearch or specific Lua scripts would be better for server-side searching.

    json document_to_search;
    try {
        document_to_search = get_json(key); // Fetches the document from Redis
    } catch (const PathNotFoundException& e) {
        // If the key doesn't exist, there's nothing to search.
        return json::array(); // Return an empty JSON array
    } catch (const RedisJSONException& e) {
        throw; // Rethrow other Redis-related exceptions
    }

    json::array_t results_array;
    find_values_recursive(document_to_search, search_value, results_array);

    return json(results_array); // Convert the C++ vector of json objects to a single json object (array type)
}

// Helper recursive function for get_all_paths
void find_json_paths_recursive(const json& current_node, const std::string& current_path_str, std::vector<std::string>& paths_list) {
    // Add the path to the current node itself.
    // Depending on desired output, might not add path to leaf primitive if only paths to containers are needed.
    // For "all paths", path to everything including primitives makes sense.
    // paths_list.push_back(current_path_str); // Path to current node. This might be too verbose.
                                            // Usually paths to elements *within* objects/arrays are listed.
                                            // Let's adjust to add paths to where values are stored.

    if (current_node.is_object()) {
        if (current_node.empty() && current_path_str != "$") { // Represent path to empty object itself
             // paths_list.push_back(current_path_str); // Only if objects themselves need a path entry.
        }
        for (auto& el : current_node.items()) {
            // Escape key name if it contains special characters for robust path generation.
            // For simplicity, direct concatenation is used here. A robust solution would check el.key().
            // Example: `$.` is for root, `.` is separator. If key is `my.key`, path becomes `$.my.key` not `$.my.key`.
            // nlohmann::json path pointers are like "/a/b/0". We are generating JSONPath $.a.b[0]
            std::string child_path = current_path_str + "." + el.key();
            paths_list.push_back(child_path); // Path to the key-value pair
            find_json_paths_recursive(el.value(), child_path, paths_list);
        }
    } else if (current_node.is_array()) {
        if (current_node.empty() && current_path_str != "$") { // Represent path to empty array itself
            // paths_list.push_back(current_path_str);
        }
        for (size_t i = 0; i < current_node.size(); ++i) {
            std::string child_path = current_path_str + "[" + std::to_string(i) + "]";
            paths_list.push_back(child_path); // Path to the array element
            find_json_paths_recursive(current_node[i], child_path, paths_list);
        }
    }
    // For primitive types, the path *to* them (from their parent object/array) is already added.
    // If we wanted to list the path *of* the primitive value itself, it would be `current_path_str`.
}


std::vector<std::string> RedisJSONClient::get_all_paths(const std::string& key) const {
    // Client-side implementation. Fetches the whole document.
    // RedisJSON's `JSON.DEBUG PATHS <key> [path]` could be an alternative if available and suitable,
    // but it's a debug command and might not be universally available or intended for production use.

    json document;
    try {
        document = get_json(key);
    } catch (const PathNotFoundException& e) {
        // Key does not exist, so no paths.
        return {};
    } catch (const RedisJSONException& e) {
        throw; // Rethrow other Redis-related errors
    }

    std::vector<std::string> all_paths;
    if (!document.is_null() && !document.empty()) { // Only proceed if there's something to get paths from
        all_paths.push_back("$"); // Path to the root element itself
        find_json_paths_recursive(document, "$", all_paths);
    } else if (document.is_object() || document.is_array()) { // Handles empty object {} or array [] at root
        all_paths.push_back("$");
    }


    // Remove duplicate paths that might arise if we add path for current node and then for its children.
    // The current recursive logic aims to avoid direct duplicates by adding path to child.
    // std::sort(all_paths.begin(), all_paths.end());
    // all_paths.erase(std::unique(all_paths.begin(), all_paths.end()), all_paths.end());
    // The current implementation should generate unique paths by construction.

    return all_paths;
}

// --- Access to sub-components ---
// These might not need implementation if they are just returning references/pointers
// to already initialized members. However, ensure members are initialized.

JSONQueryEngine& RedisJSONClient::query_engine() {
    if (!_query_engine) {
        // Lazy initialization or ensure it's done in constructor
        _query_engine = std::make_unique<JSONQueryEngine>(*this);
    }
    return *_query_engine;
}

JSONCache& RedisJSONClient::cache() {
    if (!_json_cache) {
        _json_cache = std::make_unique<JSONCache>(/* constructor args if any */);
    }
    return *_json_cache;
}

JSONSchemaValidator& RedisJSONClient::schema_validator() {
    if (!_schema_validator) {
        _schema_validator = std::make_unique<JSONSchemaValidator>(/* constructor args if any */);
    }
    return *_schema_validator;
}

JSONEventEmitter& RedisJSONClient::event_emitter() {
    if (!_event_emitter) {
        _event_emitter = std::make_unique<JSONEventEmitter>(/* constructor args if any */);
    }
    return *_event_emitter;
}

TransactionManager& RedisJSONClient::transaction_manager() {
    if (!_transaction_manager) {
        // TransactionManager might need RedisConnectionManager or similar
        _transaction_manager = std::make_unique<TransactionManager>(_connection_manager.get(), _path_parser.get(),_json_modifier.get());
    }
    return *_transaction_manager;
}


// --- Internal Helpers ---

void RedisJSONClient::_perform_write_operation(const std::string& key, const std::optional<std::string>& path, std::function<void(RedisConnection&)> operation) {
    // Internal helper. If called by stubbed methods, it might not be fully exercised.
    // Actual implementation would involve getting a connection, running operation,
    // then cache invalidation and event emission logic.
    RedisConnectionManager::RedisConnectionPtr conn = get_redis_connection();
    try {
        operation(*conn);
        // if (_json_cache) _json_cache->invalidate(key, path);
        // if (_event_emitter) _event_emitter->emit(key, path, "modified"); // Example event
    } catch (...) {
        _connection_manager->return_connection(std::move(conn));
        throw;
    }
    _connection_manager->return_connection(std::move(conn));
    // For now, just a basic pass-through or throw if called by a stub.
    // This method itself doesn't need to throw "Not Implemented" if its callers do.
}

json RedisJSONClient::_get_document_with_caching(const std::string& key) const {
    // Internal helper.
    // if (_json_cache && _json_cache->is_enabled()) {
    //     auto cached_val = _json_cache->get(key);
    //     if (cached_val) return *cached_val;
    // }
    // json doc = get_json(key); // Careful: this calls the public get_json
                                // This helper should probably implement direct Redis GET
    // if (_json_cache && _json_cache->is_enabled()) {
    //     _json_cache->put(key, doc);
    // }
    // return doc;
    throw std::runtime_error("Not implemented: RedisJSONClient::_get_document_with_caching (via stub)");
    return json{};
}

json RedisJSONClient::_parse_json_reply(const std::string& reply_str, const std::string& context_msg) const {
    try {
        return json::parse(reply_str);
    } catch (const json::parse_error& e) {
        throw JsonParsingException(context_msg + ": " + e.what() + ". Received: " + reply_str);
    }
}


} // namespace redisjson
