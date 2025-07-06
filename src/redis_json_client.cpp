#include "redisjson++/redis_json_client.h"
#include "redisjson++/hiredis_RAII.h" // For RedisReplyPtr
#include <stdexcept>
#include <string> // Required for std::to_string with some compilers/setups

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
    _lua_script_manager = std::make_unique<LuaScriptManager>(_connection_manager.get());
    // Consider calling lua_script_manager_->preload_builtin_scripts(); here if appropriate
    // For now, user might need to call it explicitly or it's done lazily.
}

RedisJSONClient::~RedisJSONClient() {
    // Resources managed by unique_ptr will be cleaned up automatically.
    // If LuaScriptManager or others have explicit shutdown needs, call them here.
}

std::unique_ptr<RedisConnection> RedisJSONClient::get_redis_connection() const {
    try {
        return _connection_manager->get_connection();
    } catch (const ConnectionException& e) {
        // Log error or rethrow as a more specific client error if desired
        throw; // Rethrow for now
    }
}

// --- Document Operations ---

void RedisJSONClient::set_json(const std::string& key, const json& document, const SetOptions& opts) {
    std::unique_ptr<RedisConnection> conn = get_redis_connection();
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
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

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
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

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
    std::unique_ptr<RedisConnection> conn = get_redis_connection();

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
    // TODO: Implement actual logic using JSON.GET command
    throw std::runtime_error("Not implemented: RedisJSONClient::get_path");
    return json{}; // Placeholder
}

void RedisJSONClient::set_path(const std::string& key, const std::string& path,
                               const json& value, const SetOptions& opts) {
    // TODO: Implement actual logic using JSON.SET command with path
    throw std::runtime_error("Not implemented: RedisJSONClient::set_path");
}

void RedisJSONClient::del_path(const std::string& key, const std::string& path) {
    // TODO: Implement actual logic using JSON.DEL command with path
    throw std::runtime_error("Not implemented: RedisJSONClient::del_path");
}

bool RedisJSONClient::exists_path(const std::string& key, const std::string& path) const {
    // TODO: Implement actual logic. This might require JSON.GET and checking if result is empty/error
    // or potentially a more specific command if available, or a Lua script.
    throw std::runtime_error("Not implemented: RedisJSONClient::exists_path");
    return false; // Placeholder
}

// --- Array Operations ---

void RedisJSONClient::append_path(const std::string& key, const std::string& path,
                                  const json& value) {
    // TODO: Implement actual logic using JSON.ARRAPPEND command
    throw std::runtime_error("Not implemented: RedisJSONClient::append_path");
}

void RedisJSONClient::prepend_path(const std::string& key, const std::string& path,
                                   const json& value) {
    // TODO: Implement actual logic (likely JSON.ARRINSERT at index 0, or custom Lua)
    // JSON.ARRINSERT <key> <path> 0 <json> [json ...]
    throw std::runtime_error("Not implemented: RedisJSONClient::prepend_path");
}

json RedisJSONClient::pop_path(const std::string& key, const std::string& path,
                               int index) {
    // TODO: Implement actual logic using JSON.ARRPOP command
    throw std::runtime_error("Not implemented: RedisJSONClient::pop_path");
    return json{}; // Placeholder
}

size_t RedisJSONClient::array_length(const std::string& key, const std::string& path) const {
    // TODO: Implement actual logic using JSON.ARRLEN command
    throw std::runtime_error("Not implemented: RedisJSONClient::array_length");
    return 0; // Placeholder
}

// --- Merge Operations ---

void RedisJSONClient::merge_json(const std::string& key, const json& patch,
                                 const MergeStrategy& strategy) {
    // TODO: Implement actual logic using JSON.MERGE command (if available directly)
    // or by fetching, merging locally, and setting. Strategy might affect implementation.
    throw std::runtime_error("Not implemented: RedisJSONClient::merge_json");
}

void RedisJSONClient::patch_json(const std::string& key, const json& patch) {
    // TODO: Implement actual logic. This implies applying a JSON Patch (RFC 6902).
    // Might require fetching the document, applying patch locally, then SETting it back,
    // ideally within a transaction or Lua script for atomicity.
    throw std::runtime_error("Not implemented: RedisJSONClient::patch_json");
}

// --- Atomic Operations (using Lua) ---

json RedisJSONClient::atomic_get_set(const std::string& key, const std::string& path,
                                     const json& new_value) {
    // TODO: Implement using a Lua script for atomicity.
    // The script would GET the value at path, then SET the new value, returning the old one.
    throw std::runtime_error("Not implemented: RedisJSONClient::atomic_get_set");
    return json{}; // Placeholder
}

bool RedisJSONClient::atomic_compare_set(const std::string& key, const std::string& path,
                                        const json& expected, const json& new_value) {
    // TODO: Implement using a Lua script for atomicity.
    // The script would GET value at path, compare with expected, and if matches, SET new_value.
    // Return true if set, false otherwise.
    throw std::runtime_error("Not implemented: RedisJSONClient::atomic_compare_set");
    return false; // Placeholder
}

// --- Utility Operations ---

std::vector<std::string> RedisJSONClient::keys_by_pattern(const std::string& pattern) const {
    // TODO: Implement using Redis SCAN or KEYS (KEYS is not recommended for production)
    throw std::runtime_error("Not implemented: RedisJSONClient::keys_by_pattern");
    return {}; // Placeholder
}

json RedisJSONClient::search_by_value(const std::string& key, const json& search_value) const {
    // TODO: This is complex. May use client-side logic after fetching the whole JSON,
    // or a Lua script for server-side searching if feasible for the structure.
    // RedisSearch module would be ideal for this, but that's a different scope.
    throw std::runtime_error("Not implemented: RedisJSONClient::search_by_value");
    return json{}; // Placeholder
}

std::vector<std::string> RedisJSONClient::get_all_paths(const std::string& key) const {
    // TODO: Implement. Could fetch the JSON and recursively find all paths,
    // or use JSON.DEBUG PATHS (if available and suitable). JSON.TYPE for each key in an object?
    throw std::runtime_error("Not implemented: RedisJSONClient::get_all_paths");
    return {}; // Placeholder
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

json RedisJSONClient::_execute_lua_script(const std::string& script_name, const std::vector<std::string>& keys, const std::vector<std::string>& args) const {
    // This is an internal helper, its full implementation depends on LuaScriptManager
    // For now, if it's called by a public stubbed method, this might also just throw.
    if (!_lua_script_manager) {
        throw std::runtime_error("LuaScriptManager not initialized");
    }
    // Example of how it might be called, actual implementation is in LuaScriptManager
    // return _lua_script_manager->execute_script(script_name, keys, args);
    throw std::runtime_error("Not implemented: RedisJSONClient::_execute_lua_script (via stub)");
    return json{};
}

void RedisJSONClient::_perform_write_operation(const std::string& key, const std::optional<std::string>& path, std::function<void(RedisConnection&)> operation) {
    // Internal helper. If called by stubbed methods, it might not be fully exercised.
    // Actual implementation would involve getting a connection, running operation,
    // then cache invalidation and event emission logic.
    std::unique_ptr<RedisConnection> conn = get_redis_connection();
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
